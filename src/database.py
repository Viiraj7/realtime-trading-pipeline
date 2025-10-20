import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from src.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

# This is the standard connection string format for SQLAlchemy:
# "dialect+driver://user:password@host:port/dbname"
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# A global variable to hold our engine
_engine = None

def get_db_engine() -> Engine:
    """
    Creates a reusable SQLAlchemy engine.
    
    If the engine doesn't exist, it creates one.
    If it already exists, it returns the existing one.
    This prevents creating multiple connection pools.
    """
    global _engine
    if _engine is None:
        try:
            # The 'pool_size' and 'max_overflow' are good defaults
            _engine = create_engine(
                DATABASE_URL, 
                pool_size=5, 
                max_overflow=10
            )
            logging.info(f"Database engine created successfully for {DB_NAME} at {DB_HOST}.")
        except Exception as e:
            logging.error(f"Error creating database engine: {e}")
            raise
    
    return _engine

def test_db_connection():
    """
    A simple function to test if the database connection is valid.
    """
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            if result.scalar() == 1:
                logging.info("Database connection successful!")
                return True
            else:
                logging.error("Database connection test failed.")
                return False
    except Exception as e:
        logging