#!/bin/bash

# Activem l'entorn virtual
source venv/bin/activate

# Arrenquem el Worker de Kafka
python3 -m app.services.migration_engine &
WORKER_PID=$!

#Arrenquem el Dashboard 
streamlit run app_gui.py &
GUI_PID=$!

# Trap per tancar-ho tot
trap "echo -e '\n🛑 Aturant tots els serveis...'; kill $WORKER_PID $GUI_PID; exit" SIGINT SIGTERM

echo "✅ Tots els serveis estan corrent! Prem Ctrl+C per aturar-los."

wait