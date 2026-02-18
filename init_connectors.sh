#!/bin/bash

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "tfg-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "db_sql", 
    "database.port": "5432",
    "database.user": "usuari_tfg",
    "database.password": "password_tfg_1234",
    "database.dbname": "tfg_relacional",
    "topic.prefix": "tfg_relacional",
    "plugin.name": "pgoutput",
    "table.include.list": "public.users,public.orders"
  }
}'

echo -e "\n\n✅ Fet"