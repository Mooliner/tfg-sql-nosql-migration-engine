import streamlit as st
import os
import time
import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv

# 1. Carregar configuració
load_dotenv()
mongo_link = os.getenv("MONGO_URL")
# Si no troba POSTGRES_URL, agafarà aquest valor per defecte. Canvia'l si els teus credencials són uns altres!
postgres_link = os.getenv("POSTGRES_URL", "postgresql://usuari_tfg:password_tfg_1234@localhost:5432/tfg_relacional")

# Connectar a Mongo
@st.cache_resource
def get_mongo_client():
    return MongoClient(mongo_link)

# Connectar a Postgres
@st.cache_resource
def get_postgres_connection():
    return psycopg2.connect(postgres_link)

client = get_mongo_client()
db_nosql = client['tfg_nosql']
collection_users = db_nosql["users"]

# 2. Configuració visual de la pàgina
st.set_page_config(page_title="TFG Dashboard", page_icon="🚀", layout="wide")

st.title("🔄 Panell de Sincronització: SQL ➔ NoSQL")
st.markdown("Monitorització en temps real de l'arquitectura *Polyglot Persistence* del TFG.")
st.divider()

# --- LÒGICA PER A POSTGRES ---
try:
    pg_conn = get_postgres_connection()
    cur = pg_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users;") # Ajusta-ho a "public.users" si et dona error
    total_usuaris_sql = cur.fetchone()[0]
except Exception as e:
    total_usuaris_sql = 0
    st.error(f"Error connectant a Postgres. Revisa el teu POSTGRES_URL. Detall: {e}")

# --- LÒGICA PER A MONGO ---
total_usuaris_nosql = collection_users.count_documents({})

# 3. Fila de mètriques (Kpis) - Ara amb 4 columnes!
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="🐘 Usuaris a PostgreSQL (Origen)", value=total_usuaris_sql)
with col2:
    st.metric(label="🍃 Usuaris a MongoDB (Destí)", value=total_usuaris_nosql)
with col3:
    # Calculem si estan iguals o si Kafka està treballant
    diferencia = total_usuaris_sql - total_usuaris_nosql
    if diferencia == 0:
        estat_sincro = "✅ Sincronitzat"
        color_delta = "normal"
    else:
        estat_sincro = "⏳ Pendent"
        color_delta = "inverse" # Ho posem en vermell
        
    st.metric(label="⚖️ Estat Consistència (BASE)", value=estat_sincro, delta=f"{diferencia} docs", delta_color=color_delta)
with col4:
    st.metric(label="⚡ Motor CDC (Kafka)", value="Actiu", delta="En línia")

st.divider()

# 4. Visualització de les dades reals
st.subheader("Últims documents sincronitzats a MongoDB")

ultims_usuaris = list(collection_users.find().sort("migrat_el", -1).limit(5))

if ultims_usuaris:
    for usuari in ultims_usuaris:
        with st.expander(f"👤 {usuari.get('username', 'Usuari sense nom')} (ID: {usuari.get('id')})"):
            st.json(usuari)
else:
    st.info("Encara no hi ha cap dada a la base de dades NoSQL.")

# ==========================================
# 🔄 LA MÀGIA DE L'AUTO-REFRESC
# ==========================================
time.sleep(2)
st.rerun()