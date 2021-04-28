
from sqlalchemy import create_engine


def get_db_connection():
    """
    Returns a database connection
    """

    engine = create_engine(
        'postgresql://postgres:Omsairam@localhost:5432/crosslend')

    return engine
