from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

# 1. Crea el motor (engine)
engine = create_engine(settings.SQLALCHEMY_DATABASE_URL)

# 2. Crea la fàbrica de sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Funció per obtenir la base de dades (Dependency Injection)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

        