import json
from confluent_kafka import Consumer
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

config = {
    'bootstrap.servers': 'localhost:59092',
    'group.id': 'tfg-migration-group',      
    'auto.offset.reset': 'earliest'         
}

consumer = Consumer(config)
topic = "tfg_relacional.public.users"
consumer.subscribe([topic])

user = os.getenv("MONGO_USER")
pwd = os.getenv("MONGO_PWD")

client = MongoClient(f'mongodb://{user}:{pwd}@localhost:27017/')
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

            raw_data = json.loads(msg.value().decode('utf-8'))

            payload_after = raw_data['payload']['after']
            nom_taula = raw_data['payload']['source']['table']
            colleccion = db[nom_taula]

            dada_neta = transformar(payload_after)           

            filter = {"id" : dada_neta["id"]}
            accions = {"$set" : dada_neta}

            colleccion.update_one(filter, accions, upsert=True)
            print(f"Funciona: {dada_neta['username']}")

    except KeyboardInterrupt:
        print("\nATURAT")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_worker()