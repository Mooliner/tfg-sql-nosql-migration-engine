# Motor de Migració Bidireccional SQL <> NoSQL

Aquest projecte és el meu Treball de Final de Grau (TFG). L'objectiu és desenvolupar un motor de sincronització de dades en temps real entre una base de dades relacional (**PostgreSQL**) i una no relacional (**MongoDB**) utilitzant una arquitectura dirigida per esdeveniments.

## Arquitectura i Tecnologies
* **Lògica:** Python 3.12 + FastAPI
* **Bases de Dades:** PostgreSQL 15 & MongoDB 6.0
* **Missatgeria:** Apache Kafka (Confluent)
* **Infraestructura:** Docker & Docker Compose
* **ORM/ODM:** SQLAlchemy & PyMongo

## Estructura del Projecte
* `/app`: Codi font de l'aplicació Python.
* `/docker`: Fitxers de configuració de la infraestructura.
* `docker-compose.yml`: Orquestració de serveis.

## Instal·lació i Ús

### 1. Prerequisits
* Docker Desktop
* Python 3.12

### 2. Configurar la infraestructura
```bash
docker-compose up -d
```
### 3. Configurar l'entorn Python
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1  # Windows
pip install -r requirements.txt
```

## Dades del Projecte
* **Autor:** [Gerard Moliner i Condomines](https://github.com/mooliner)
* **Titulació:** Grau en Enginyeria Informàtica
* **Institució:** [Universitat de Lleida (UdL) - Campus d'Igualada](https://www.campusigualada.udl.cat/ca/estudis/grau-en-enginyeria-informatica/)
* **Curs:** 2025-2026
