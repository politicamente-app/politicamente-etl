# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 20:38:26

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .config import DATABASE_URL

def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    return sessionmaker(bind=engine)()