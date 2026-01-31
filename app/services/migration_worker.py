import json
from confluent_kafka import Consumer
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import os

config = {
    'bootstrap.servers': 'localhost:59092',
    'group.id': 'tfg-migration-group',      
    'auto.offset.reset': 'earliest'         
}

consumer = Consumer(config)
topics = ["tfg_relacional.public.users", "tfg_relacional.public.orders"]
consumer.subscribe(topics)

link = os.getenv("MONGO_URL")

client = MongoClient(link)
db = client['tfg_nosql']


def transformar(dada):
    nova_dada = dada.copy()
    
    if "created_at" in nova_dada:
        nova_dada["created_at"] = datetime.fromtimestamp(nova_dada["created_at"] / 1000000)
    
    if "updated_at" in nova_dada:
        nova_dada["updated_at"] = datetime.fromtimestamp(nova_dada["updated_at"] / 1000000)
    
    nova_dada["migrat_el"] = datetime.now()
    nova_dada["font"] = "postgresql_tfg"

    return nova_dada

def start_worker():
    print(f"Escoltant el tòpic: {topics}...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue
            if msg.value() is None:
                continue
            
            raw_data = json.loads(msg.value().decode('utf-8'))
            
            print(f"CRASH DEBUG - JSON rebut: {raw_data}")

            operacio = raw_data['op']
            nom_taula = raw_data['source']['table']
            #print(f"DEBUG: Guardant a la base de dades '{db.name}' i a la col·lecció 'users'")
            colleccion = db["users"]

            if(nom_taula == 'users'):
                if operacio == 'c' or operacio == 'u':
                    dada_neta = transformar(raw_data['after'])
                    filtret = {"id": dada_neta["id"]}
                    accions = {"$set": dada_neta}
                    colleccion.update_one(filtret, accions, upsert=True)
                    print(f"Sincronitzat (C/U): {dada_neta.get('username', dada_neta['id'])}")
                
                elif operacio == 'd':
                    id_a_esborrar = raw_data['before']['id']
                    colleccion.delete_one({"id": id_a_esborrar})
                    print(f"Esborrat ID: {id_a_esborrar}")
                
                elif operacio == 'r':
                    dada_neta = transformar(raw_data['after'])
                    colleccion.update_one({"id": dada_neta["id"]}, {"$set": dada_neta}, upsert=True)

            elif(nom_taula == 'orders'):
                if operacio == 'c' or operacio == 'r':
                    comanda_neta = transformar(raw_data['after'])
                    user_id = int(comanda_neta['user_id'])
                    colleccion.update_one({"id": user_id, "orders.id": {"$ne": comanda_neta['id']} },{"$push": {"orders": comanda_neta}}, upsert=False)
                    print(f"Comanda afegida a l'usuari {user_id}")

                elif operacio == 'd':
                    comanda_neta = transformar(raw_data['before'])
                    print("Comanda neta: ", comanda_neta)
                    print("User id: ", comanda_neta['user_id'])
                    user_id = int(comanda_neta['user_id'])
                    colleccion.update_one({"id": user_id}, {"$pull": {"orders": {"id": comanda_neta['id']}}}, upsert=True)
                    print(f"Comanda esborrada de l'usuari {user_id}")
                
                elif operacio == 'u':
                    comanda_neta = transformar(raw_data['after'])
                    user_id = int(comanda_neta['user_id'])
                    filter_id = {"id": user_id, "orders.id": comanda_neta['id']}
                    accion = {"$set": {"orders.$": comanda_neta}}
                    colleccion.update_one(filter_id, accion)
                    print(f"Comanda actualitzada a l'usuari {user_id}")
           
    except KeyboardInterrupt:
        print("\nATURAT")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_worker()