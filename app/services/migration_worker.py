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
topic = "tfg_relacional.public.users"
consumer.subscribe([topic])

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
    print(f"Escoltant el tòpic: {topic}...")

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
            payload = raw_data['payload']
            operacio = payload['op']
            nom_taula = payload['source']['table']
            print(f"DEBUG: Guardant a la base de dades '{db.name}' i a la col·lecció '{nom_taula}'")
            colleccion = db[nom_taula]

            match operacio:
                case 'c' | 'u':
                    dada_neta = transformar(payload['after'])
                    filtret = {"id": dada_neta["id"]}
                    accions = {"$set": dada_neta}
                    colleccion.update_one(filtret, accions, upsert=True)
                    print(f"Sincronitzat (C/U): {dada_neta.get('username', dada_neta['id'])}")

                case 'd':
                    id_a_esborrar = payload['before']['id']
                    colleccion.delete_one({"id": id_a_esborrar})
                    print(f"Esborrat ID: {id_a_esborrar}")

                case 'r':
                    dada_neta = transformar(payload['after'])
                    colleccion.update_one({"id": dada_neta["id"]}, {"$set": dada_neta}, upsert=True)

    except KeyboardInterrupt:
        print("\nATURAT")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_worker()