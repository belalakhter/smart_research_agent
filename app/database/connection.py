import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as _connection
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.models import Base  

_connection_pool: pool.SimpleConnectionPool | None = None
_engine = None 
_SessionLocal = None  

def init_connection_pool(minconn=1, maxconn=5):
    """
    Initialize the PostgreSQL connection pool and create tables if they don't exist.
    Call this once in main.py
    """
    global _connection_pool, _engine, _SessionLocal

    if _connection_pool is None:
        _connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            user="postgres",
            password="postgres",
            host="smart_agent_db",
            database="postgres"
        )
        print("PostgreSQL connection pool initialized.")

        def get_conn():
            return _connection_pool.getconn()

        _engine = create_engine(
            "postgresql+psycopg2://",
            creator=get_conn,
            poolclass=None  
        )    

        Base.metadata.create_all(_engine)
        print("Database tables ensured (migration complete).")

        _SessionLocal = sessionmaker(bind=_engine)

    return _connection_pool

def get_connection() -> _connection:
    """
    Get a connection from the pool.
    Raises an error if pool is not initialized.
    """
    if _connection_pool is None:
        raise Exception("Connection pool not initialized. Call init_connection_pool() first.")
    return _connection_pool.getconn()

def release_connection(conn: _connection):
    """
    Return a connection back to the pool.
    """
    if _connection_pool is None:
        raise Exception("Connection pool not initialized.")
    _connection_pool.putconn(conn)

def close_all_connections():
    """
    Close all connections in the pool.
    """
    if _connection_pool is not None:
        _connection_pool.closeall()
        print("All connections in the pool have been closed.")

def get_session():
    """
    Get a SQLAlchemy session for ORM operations.
    """
    if _SessionLocal is None:
        raise Exception("Connection pool not initialized. Call init_connection_pool() first.")
    return _SessionLocal()