from .base import Base, BaseEntity
from sqlalchemy.orm import Mapped, mapped_column

class UserSQL(Base, BaseEntity):
    __tablename__ = "users"

    username: Mapped[str] = mapped_column(unique=True, nullable=False)
    email: Mapped[str] = mapped_column(unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self):
        return f"<UserSQL(username={self.username})>"