import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date
import os
import socket
from dotenv import load_dotenv

load_dotenv()

def create_db_engine(user=os.environ['DB_USER'], password=os.environ['DB_PASS'], port=5432):
    """
    Creates engine needed to create connections to the database
    with the credentials and parameters provided.

    Args:
        user (str): Database username credential
        password (str): Database password credential
        port (int): Source port for database connection
    Returns:
        SQLAlchemy engine object
    Notes:
        Check https://github.com/Bain/ekpi-priorities/ README for setup
        instructions
    """

    assert isinstance(port, int), "Port must be numeric"
    assert user is not None, 'Username is empty'
    assert password is not None, 'Password is empty'

    if is_running_locally():
        db_uri = f'postgresql+psycopg2://{user}:{password}@dpg-ck7ghkvq54js73fbrei0-a.oregon-postgres.render.com/house_listings'
    else:
        db_uri = f'postgresql+psycopg2://{user}:{password}@dpg-ck7ghkvq54js73fbrei0-a/house_listings'
    engine = create_engine(db_uri, future=True, pool_size=10,
                           max_overflow=2,
                           pool_recycle=300,
                           pool_pre_ping=True,
                           pool_use_lifo=True)

    return engine

def read_listings_sql_table():
    """
    Read house listings from db table
    Returns:

    """
    engine = create_db_engine()
    with engine.connect() as conn:
        search_results = pd.read_sql(
            """
            SELECT *
            from listings
            where price_per_area_in_first_quartile = TRUE """,
            con=conn, index_col='listing_id')
    engine.dispose()
    return search_results

def is_running_locally():
    """
    Check if code is running locally or in the cloud

    Returns:
    """
    hostname = socket.gethostname()
    return hostname == "localhost" or hostname == "127.0.0.1" or hostname == 'SAOX1Y6-58781'

def get_listings_urls(listing_ids, engine):

    query = \
        f"""
        SELECT url
        from listings
        where listing_id in {tuple(listing_ids)} 
        """
    with engine.connect() as conn:
        urls = pd.read_sql(query, con=conn).squeeze()

    return urls
def delete_listings_from_db(unavailable_ids, engine):

    if len(unavailable_ids) > 1:
        unavailable_ids = tuple(unavailable_ids)
    elif len(unavailable_ids) == 1:
        unavailable_ids = f"('{unavailable_ids[0]}')"
    else:
        return f'No unavailable ids found'
    query = \
        text(
            f"""
            DELETE FROM listings
            WHERE listing_id IN {unavailable_ids}
            """)
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()
    return

