import streamlit as st
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import time

load_dotenv()
link = os.getenv("MONGO_URL")

@st.cache_resource
def get_mongo_client():
    return MongoClient(link)

client = get_mongo_client()
db_nosql = client['tfg_nosql']
collection_users = db_nosql["users"]

st.set_page_config(page_title="TFG Dashboard", page_icon="🚀", layout="wide")

st.title("🔄 Panell de Sincronització: SQL ➔ NoSQL")
st.markdown("Monitorització en temps real de l'arquitectura *Polyglot Persistence* del TFG.")
st.divider()

col1, col2, col3 = st.columns(3)

total_usuaris_nosql = collection_users.count_documents({})

with col1:
    st.metric(label="🟢 Estat MongoDB", value="Connectat")
with col2:
    st.metric(label="📄 Usuaris (Documents)", value=total_usuaris_nosql)
with col3:
    st.metric(label="⚡ Motor CDC (Kafka)", value="Actiu")

st.divider()

st.subheader("Últims documents sincronitzats a MongoDB")

ultims_usuaris = list(collection_users.find().sort("migrat_el", -1).limit(5))

if ultims_usuaris:
    for usuari in ultims_usuaris:
        with st.expander(f"👤 {usuari.get('username', 'Usuari sense nom')} (ID: {usuari.get('id')})"):
            st.json(usuari)
else:
    st.info("Encara no hi ha cap dada a la base de dades NoSQL.")

time.sleep(2)
st.rerun()