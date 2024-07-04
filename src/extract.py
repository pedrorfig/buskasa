import os
import socket
from datetime import date
import sys

import pandas as pd
import requests as r
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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
    neighborhoods = sorted(pd.DataFrame.from_dict(city_neighborhood_data.get("result"))[
        "name"
    ].tolist())
    return neighborhoods


def get_city_id_from_city_and_state_names(state, city):
    engine = create_db_engine()
    with engine.connect() as conn:
        # Checking for existing listing_ids on the database according
        # to the specified filters
        filter_conditions = {'city': city, 'state': state}
        city_id_dataframe = pd.read_sql(
            """
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
    user=os.environ["DB_USER"], password=os.environ["DB_PASS"], port=6543
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

    db_uri = f"postgresql+psycopg2://{user}:{password}@aws-0-sa-east-1.pooler.supabase.com:{port}/postgres"
    engine = create_engine(db_uri, future=True)

    return engine


@st.cache_data
def get_best_deals_from_city(city):
    """
    Read house listings from db table
    Returns:

    """
    engine = create_db_engine()
    with engine.connect() as conn:
        search_results = pd.read_sql(
            """
            SELECT *
            FROM fact_listings
            WHERE price_per_area_in_first_quartile = True
            AND city = %(city)s
            ORDER BY price_per_area DESC
            """,
            con=conn,
            params={'city': city},
            index_col="listing_id"
        )
    engine.dispose()
    return search_results


def get_unique_cities_from_db():
    """
    Read house listings from db table
    Returns:

    """
    engine = create_db_engine()
    with engine.connect() as conn:
        unique_cities = pd.read_sql(
            """
            SELECT DISTINCT city
            FROM fact_listings
            """,
            con=conn
        )
    engine.dispose()
    return unique_cities


def get_listings_urls(listing_ids, engine):

    query = f"""
        SELECT url
        from fact_listings
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
            DELETE FROM fact_listings
            WHERE listing_id IN {unavailable_ids}
            """
    )
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()
    return


def get_search_parameters():
    """
    Get the search parameters from the command prompt
    Returns:
        neighborhoods (list): Neighborhoods to scrape
        state (str): State to scrape
        city (str): City to scrape
    """

    neighborhoods = [""]

    if len(sys.argv) == 3:
        print(f"Running for all neighborhoods in {sys.argv[1]} - {sys.argv[2]}")
        neighborhoods = \
            get_neighborhoods_from_city_and_state(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:
        print(f"Running for {sys.argv[3]} in {sys.argv[1]} - {sys.argv[2]}")
        neighborhoods = sys.argv[3].split(",")
    elif len(sys.argv) < 3:
        print("Please provide at least the following arguments: state, city")
        sys.exit(1)
    else:
        print(
            """Please provide at most the
            following arguments:state, city, neighborhoods"""
        )
        sys.exit(1)
    return sys.argv[1], sys.argv[2], neighborhoods