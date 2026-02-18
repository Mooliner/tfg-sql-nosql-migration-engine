import json
import os
from confluent_kafka import Consumer
from pymongo import MongoClient
from dotenv import load_dotenv
from app.services.data_transformer import DataTransformer
from app.log_config import get_logger

logger = get_logger("WORKER")
load_dotenv()

class MigrationEngine:
    def __init__(self):
        self.transformer = DataTransformer()
        
        link = os.getenv("MONGO_URL")
        self.client = MongoClient(link)
        self.db = self.client['tfg_nosql']
        self.collection = self.db["users"]
        
        config = {
            'bootstrap.servers': 'localhost:59092',
            'group.id': 'tfg-migration-group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(config)

    def start_worker(self, topics):
        self.consumer.subscribe(topics)
        logger.info(f"Escoltant els tòpics: {topics}...")

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None: continue
                if msg.error():
                    logger.error(f"Error Kafka: {msg.error()}")
                    continue
                if msg.value() is None: continue
                
                raw_data = json.loads(msg.value().decode('utf-8'))
                
                self.processar_missatge(raw_data)

        except KeyboardInterrupt:
            print("\nATURAT")
        finally:
            self.consumer.close()

    def processar_missatge(self, raw_data):
        operacio = raw_data['op']
        nom_taula = raw_data['source']['table']

        if nom_taula == 'users':
                if operacio == 'c' or operacio == 'u':
                    dada_neta = self.transformer.transformar(raw_data['after'])
                    filtret = {"id": dada_neta["id"]}
                    accions = {"$set": dada_neta}
                    self.collection.update_one(filtret, accions, upsert=True)
                    logger.info(f"[USERS](C/U): {dada_neta.get('username', dada_neta['id'])}")
                
                elif operacio == 'd':
                    id_a_esborrar = raw_data['before']['id']
                    self.collection.delete_one({"id": id_a_esborrar})
                    logger.info(f"[USERS](D): {id_a_esborrar}")
                
                elif operacio == 'r':
                    dada_neta = self.transformer.transformar(raw_data['after'])
                    self.collection.update_one({"id": dada_neta["id"]}, {"$set": dada_neta}, upsert=True)

        elif nom_taula == 'orders':
                if operacio == 'c' or operacio == 'r':
                    comanda_neta = self.transformer.transformar(raw_data['after'])
                    user_id = int(comanda_neta['user_id'])
                    self.collection.update_one({"id": user_id, "orders.id": {"$ne": comanda_neta['id']} },{"$push": {"orders": comanda_neta}}, upsert=False)
                    logger.info(f"[ORDERS] (C): {user_id}")

                elif operacio == 'd':
                    comanda_neta = self.transformer.transformar(raw_data['before'])
                    user_id = int(comanda_neta['user_id'])
                    self.collection.update_one({"id": user_id}, {"$pull": {"orders": {"id": comanda_neta['id']}}}, upsert=False)
                    logger.info(f"[ORDERS] (D): {user_id}")
                
                elif operacio == 'u':
                    comanda_neta = self.transformer.transformar(raw_data['after'])
                    user_id = int(comanda_neta['user_id'])
                    filter_id = {"id": user_id, "orders.id": comanda_neta['id']}
                    accion = {"$set": {"orders.$": comanda_neta}}
                    self.collection.update_one(filter_id, accion)
                    logger.info(f"[ORDERS] (U): {user_id}")

if __name__ == "__main__":
    motor = MigrationEngine()
    motor.start_worker(["tfg_relacional.public.users", "tfg_relacional.public.orders"])