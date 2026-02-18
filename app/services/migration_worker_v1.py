import json
from confluent_kafka import Consumer
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import os
from app.log_config import get_logger

logger = get_logger("WORKER")

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
                logger.error(f"Error Kafka: {msg.error()}")
                continue
            if msg.value() is None:
                continue
            
            raw_data = json.loads(msg.value().decode('utf-8'))
            
            logger.info(f"JSON rebut: {raw_data}")

            operacio = raw_data['op']
            nom_taula = raw_data['source']['table']
            colleccion = db["users"]

            if(nom_taula == 'users'):
                if operacio == 'c' or operacio == 'u':
                    dada_neta = transformar(raw_data['after'])
                    filtret = {"id": dada_neta["id"]}
                    accions = {"$set": dada_neta}
                    colleccion.update_one(filtret, accions, upsert=True)
                    logger.info(f"[USERS](C/U): {dada_neta.get('username', dada_neta['id'])}")
                
                elif operacio == 'd':
                    id_a_esborrar = raw_data['before']['id']
                    colleccion.delete_one({"id": id_a_esborrar})
                    logger.info(f"[USERS](D): {id_a_esborrar}")
                
                elif operacio == 'r':
                    dada_neta = transformar(raw_data['after'])
                    colleccion.update_one({"id": dada_neta["id"]}, {"$set": dada_neta}, upsert=True)

            elif(nom_taula == 'orders'):
                if operacio == 'c' or operacio == 'r':
                    comanda_neta = transformar(raw_data['after'])
                    user_id = int(comanda_neta['user_id'])
                    colleccion.update_one({"id": user_id, "orders.id": {"$ne": comanda_neta['id']} },{"$push": {"orders": comanda_neta}}, upsert=False)
                    logger.info(f"[ORDERS] (C): {user_id}")

                elif operacio == 'd':
                    comanda_neta = transformar(raw_data['before'])
                    user_id = int(comanda_neta['user_id'])
                    colleccion.update_one({"id": user_id}, {"$pull": {"orders": {"id": comanda_neta['id']}}}, upsert=False)
                    logger.info(f"[ORDERS] (D): {user_id}")
                
                elif operacio == 'u':
                    comanda_neta = transformar(raw_data['after'])
                    user_id = int(comanda_neta['user_id'])
                    filter_id = {"id": user_id, "orders.id": comanda_neta['id']}
                    accion = {"$set": {"orders.$": comanda_neta}}
                    colleccion.update_one(filter_id, accion)
                    logger.info(f"[ORDERS] (U): {user_id}")
           
    except KeyboardInterrupt:
        print("\nATURAT")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_worker()