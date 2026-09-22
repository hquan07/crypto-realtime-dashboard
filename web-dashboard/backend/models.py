import sqlalchemy as sa
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = sa.Column(sa.Integer, primary_key=True, index=True)
    username = sa.Column(sa.String, unique=True, index=True)
    password_hash = sa.Column(sa.String)

class Watchlist(Base):
    __tablename__ = "watchlists"
    id = sa.Column(sa.Integer, primary_key=True, index=True)
    user_id = sa.Column(sa.Integer, sa.ForeignKey("users.id"))
    symbol = sa.Column(sa.String)

class Alert(Base):
    __tablename__ = "alerts"
    id = sa.Column(sa.Integer, primary_key=True, index=True)
    user_id = sa.Column(sa.Integer, sa.ForeignKey("users.id"))
    symbol = sa.Column(sa.String)
    condition = sa.Column(sa.String)  # e.g., "ABOVE", "BELOW"
    threshold = sa.Column(sa.Float)
    is_active = sa.Column(sa.Boolean, default=True)
