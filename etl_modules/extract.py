import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date
import os
import socket
from dotenv import load_dotenv
import requests as r

load_dotenv()

def get_neighborhoods_from_city_and_state(state, city):
    """
    Downloads all neighborhood names for a given city
    """
    city_id = get_city_id_from_city_and_state_names(state, city)
    response = r.get(
        f"https://api.brasilaberto.com/v1/districts/{city_id}",
        headers={"Bearer": os.environ["BRASIL_ABERTO_API_KEY_PAID"]},
    )
    city_neighborhood_data = response.json()
    neighborhoods = pd.DataFrame.from_dict(city_neighborhood_data.get("result"))[
        "name"
    ].tolist()
    return neighborhoods

def get_city_id_from_city_and_state_names(state, city):
    engine = create_db_engine()
    with engine.connect() as conn:
        # Checking for existing listing_ids on the database according to the specified filters
        filter_conditions = {'city':city, 'state':state}
        
        city_id_dataframe = pd.read_sql(
            rf"""
            SELECT
                city_id
            from
                dim_cities
            JOIN dim_states on dim_states.state_short = dim_cities.state_short
            WHERE
                city_name = %(city)s
                and state_name = %(state)s
            LIMIT 1
                """,
            con=conn,
            params=filter_conditions
        )

    city_id = city_id_dataframe.iloc[0,0]
    return city_id


def create_db_engine(
    user=os.environ["DB_USER"], password=os.environ["DB_PASS"], port=5432
):
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
    """

    assert isinstance(port, int), "Port must be numeric"
    assert user is not None, "Username is empty"
    assert password is not None, "Password is empty"

    if is_running_locally():
        db_uri = f"postgresql+psycopg2://{user}:{password}@aws-0-sa-east-1.pooler.supabase.com:{port}/postgres"
    else:
        db_uri = f"postgresql+psycopg2://{user}:{password}@aws-0-sa-east-1.pooler.supabase.com:{port}/postgres"
    engine = create_engine(db_uri, future=True)

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
            """,
            con=conn,
            index_col="listing_id",
        )
    engine.dispose()
    return search_results


def is_running_locally():
    """
    Check if code is running locally or in the cloud

    Returns:
    """
    hostname = socket.gethostname()
    return (
        hostname == "localhost"
        or hostname == "127.0.0.1"
        or hostname == "SAOX1Y6-58781"
    )


def get_listings_urls(listing_ids, engine):

    query = f"""
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
        return f"No unavailable ids found"
    query = text(
        f"""
            DELETE FROM listings
            WHERE listing_id IN {unavailable_ids}
            """
    )
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()
    return
