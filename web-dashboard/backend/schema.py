import strawberry
from typing import List
from sqlalchemy.orm import Session
from fastapi import Depends
from models import User, Watchlist, Alert

# Database dependency placeholder, we'll setup DB in app.py
def get_db():
    pass

@strawberry.type
class WatchlistItem:
    id: int
    symbol: str

@strawberry.type
class AlertItem:
    id: int
    symbol: str
    condition: str
    threshold: float
    is_active: bool

@strawberry.type
class UserProfile:
    id: int
    username: str
    watchlists: List[WatchlistItem]
    alerts: List[AlertItem]

@strawberry.type
class Query:
    @strawberry.field
    def me(self, info) -> UserProfile:
        # In a real app, we extract the user from info.context["user"]
        # For this MVP, we return a mock or fetch from DB
        request = info.context["request"]
        user = request.state.user if hasattr(request.state, "user") else None
        if not user:
            raise Exception("Not authenticated")
            
        db: Session = info.context["db"]
        db_user = db.query(User).filter(User.username == user).first()
        if not db_user:
            raise Exception("User not found")
            
        watchlists = db.query(Watchlist).filter(Watchlist.user_id == db_user.id).all()
        alerts = db.query(Alert).filter(Alert.user_id == db_user.id).all()
        
        return UserProfile(
            id=db_user.id,
            username=db_user.username,
            watchlists=[WatchlistItem(id=w.id, symbol=w.symbol) for w in watchlists],
            alerts=[AlertItem(id=a.id, symbol=a.symbol, condition=a.condition, threshold=a.threshold, is_active=a.is_active) for a in alerts]
        )

@strawberry.type
class Mutation:
    @strawberry.mutation
    def add_watchlist(self, info, symbol: str) -> WatchlistItem:
        request = info.context["request"]
        user = request.state.user
        db: Session = info.context["db"]
        
        db_user = db.query(User).filter(User.username == user).first()
        new_w = Watchlist(user_id=db_user.id, symbol=symbol)
        db.add(new_w)
        db.commit()
        db.refresh(new_w)
        return WatchlistItem(id=new_w.id, symbol=new_w.symbol)

schema = strawberry.Schema(query=Query, mutation=Mutation)
