from app.core.database import engine
from app.models import sql_models
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app import schema, models
from app.core.security import pwd_context

# Això és el que diu: "SQLAlchemy, crea totes les taules que trobis a sql_models"
sql_models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="TFG Engine - Gerard Moliner")

@app.get("/")
def read_root():
    return {"message": "Motor de migració actiu"}

@app.post("/users", response_model=schema.User)
def create_user(user_data: schema.UserCreate, db: Session = Depends(get_db)):

    hashed_password = pwd_context.hash(user_data.password)

    new_user = sql_models.UserSQL(
        username=user_data.username,
        email=user_data.email,
        password_hash=hashed_password
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user

