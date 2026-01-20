import logging
from passlib.context import CryptContext

# Això silencia l'error de lectura de versió de bcrypt que veus al log
logging.getLogger("passlib").setLevel(logging.ERROR)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")