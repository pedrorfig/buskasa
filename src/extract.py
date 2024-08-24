import io
import os
import sys

import pandas as pd
import requests as r
import streamlit as st
from dotenv import load_dotenv
from PIL import Image
from sqlalchemy import create_engine, text
import overpy
import logging

# Configure logging
logging.basicConfig(format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)
# Create logger object
logger = logging.getLogger(__name__)


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
    neighborhoods = sorted(
        pd.DataFrame.from_dict(city_neighborhood_data.get("result"))["name"].tolist()
    )
    return neighborhoods


def get_city_id_from_city_and_state_names(state, city):
    engine = create_db_engine()
    with engine.begin() as conn:
        # Checking for existing listing_ids on the database according
        # to the specified filters
        filter_conditions = {"city": city, "state": state}
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
            params=filter_conditions,
        )

    city_id = city_id_dataframe.iloc[0, 0]
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
    
    logger.info("Creating database engine")
    
    db_uri = f"postgresql+psycopg2://{user}:{password}@aws-0-sa-east-1.pooler.supabase.com:{port}/postgres"
    engine = create_engine(db_uri, future=True, echo=True)

    return engine

@st.cache_data(show_spinner=False)
def get_listings(_conn, user_has_visits=False, user=None):
    """
    Get listings for registered user
    """
    if user:
        if user_has_visits:
            listings = pd.read_sql(
                """
                    with listings_visited_by_user  as (
                        select * from fact_listings_visited as flv
                        where flv."user" = %(user)s
                        )
                    SELECT *
                    FROM fact_listings fl
                    LEFT JOIN listings_visited_by_user lvu on lvu.visited_listing_id = fl.listing_id
                    WHERE
                        price_per_area_in_first_quartile = True
                    """,
                con=_conn,
                index_col="listing_id",
                params={"user": user},
            )
        else:
            listings = pd.read_sql(
                        """
                        SELECT *, null as user
                        FROM fact_listings
                        WHERE price_per_area_in_first_quartile = True
                        """,
                        con=_conn,
                        index_col="listing_id"
                    )
    
    else:
        listings = pd.read_sql(
                    """
                    SELECT *, null as user
                    FROM fact_listings
                    WHERE price_per_area_in_first_quartile = True
                    """,
                    con=_conn,
                    index_col="listing_id"
                )

    return listings


def get_sat_image(min_lat, max_lat, min_lon, max_lon):
    # Define the URL template
    url_template = "https://api.mapbox.com/styles/v1/{username}/{style_id}/static/[{min_lon},{min_lat},{max_lon},{max_lat}]/{width}x{height}@2x?access_token={access_token}"

    # Define the variable values
    username = "mapbox"
    style_id = "satellite-v9"
    width = 500
    height = 500
    access_token = os.getenv('MAPBOX_TOKEN')
    # Replace the variables in the URL template
    url = url_template.format(
        username=username,
        style_id=style_id,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lon=min_lon,
        max_lon=max_lon,
        width=width,
        height=height,
        access_token=access_token
    )

    # Send the GET request
    response = r.get(url)

    # Check the response status code
    if response.status_code == 200:
        # Process the response data
        image = Image.open(io.BytesIO(response.content))
    else:
        print("API call failed with status code:", response)
        image = None
    return image

def get_n_bus_lines(min_lat, max_lat, min_lon, max_lon):
    # Import the overpy module
    
    api = overpy.Overpass()

    bbox = [*map(lambda x: str(x), [min_lat, min_lon, max_lat, max_lon])]
    query = f'relation["route"="bus"]({",".join(bbox)});out;'

    # Execute the query
    result = api.query(query)

    # Output the number of bus stops
    return len(result.relations)

def is_next_to_park(lat, lon):
    api = overpy.Overpass()

    query = f'way["leisure"="park"](around:1000, {lat}, {lon});out;'

    # Execute the query
    result = api.query(query)

    next_to_park = False
    for way in result.ways:
        if way.tags.get('park:type') == 'city_park':
            next_to_park = True
    return next_to_park


def add_green_density_to_db(db_engine, min_lat, max_lat, min_lon, max_lon, green_density):
    with db_engine.begin() as conn:
        query = text(
        """
        INSERT INTO fact_image_analysis (min_lat, max_lat, min_lon, max_lon, green_density)
        VALUES (:min_lat, :max_lat, :min_lon, :max_lon, :green_density)
        """)
        conn.execute(
            query,
            parameters={
                "min_lat": min_lat,
                "max_lat": max_lat,
                "min_lon": min_lon,
                "max_lon": max_lon,
                "green_density": green_density,
            }
        )
    return

def get_unique_cities_from_db():
    """
    Read house listings from db table
    Returns:

    """
    engine = create_db_engine()
    with engine.begin() as conn:
        unique_cities = pd.read_sql(
            """
            SELECT DISTINCT city
            FROM fact_listings
            """,
            con=conn,
        )
    engine.dispose()
    return unique_cities


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
    with engine.begin() as conn:
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
        neighborhoods = get_neighborhoods_from_city_and_state(sys.argv[1], sys.argv[2])
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
