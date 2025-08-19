import io
import os
import sys

import pandas as pd
import requests as r
import streamlit as st
from dotenv import load_dotenv
from PIL import Image
from sqlalchemy import  text
import overpy
import logging

from src.database import db_manager

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
    """Get city ID from city and state names with optimized database access"""
    with db_manager.get_transaction() as conn:
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
    DEPRECATED: Use db_manager.get_engine() instead.
    Creates engine needed to create connections to the database
    with the credentials and parameters provided.

    Args:
        user (str): Database username credential
        password (str): Database password credential
        port (int): Source port for database connection
    Returns:
        SQLAlchemy engine object
    Notes:
        This function is deprecated in favor of the DatabaseManager singleton
    """
    logger.warning("create_db_engine is deprecated. Use db_manager.get_engine() instead.")
    return db_manager.get_engine()

@st.cache_data(show_spinner=False)
def get_listings(business_type, city):
    """
    Get listings with optimized database access
    """
    with db_manager.get_connection() as conn:
        listings = pd.read_sql(
            """
            SELECT *
            FROM fact_listings
            WHERE price_per_area_in_first_quartile = True
            AND business_type = %(business_type)s
            AND city = %(city)s
            AND unit_type not in ('BUILDING', 'BUSINESS', 'COMMERCIAL_BUILDING', 'COMMERCIAL_PROPERTY', 'FARM', 'RESIDENTIAL_ALLOTMENT_LAND', 'OFFICE', 'SHED_DEPOSIT_WAREHOUSE')
            """,
            con=conn,
            index_col="listing_id",
            params={"business_type": business_type, "city": city},
        )

    logger.info(f"Found {len(listings)} listings for {city} and {business_type} business type")

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
        logger.error("API call failed with status code:", response)
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

    query = f'nwr["leisure"="park"](around:1000, {lat}, {lon});out;'

    # Execute the query
    result = api.query(query)

    next_to_park = False
    for way in result.ways:
        if (way.tags.get('park:type') == 'city_park') or \
            ((way.tags.get('leisure') == 'park') and ('Parque' in way.tags.get('name', ''))):
            next_to_park = True
    for relation in result.relations:
        if relation.tags.get('leisure') == 'park':
            next_to_park = True
    return next_to_park


def add_green_density_to_db(min_lat, max_lat, min_lon, max_lon, green_density):
    """Add green density analysis to database using optimized connection"""

    with db_manager.get_transaction() as conn:
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
    Read house listings from db table with optimized database access
    Returns:
        DataFrame with unique cities
    """
    with db_manager.get_connection() as conn:
        unique_cities = pd.read_sql(
            """
            SELECT DISTINCT city
            FROM fact_listings
            """,
            con=conn,
        )
    return unique_cities


def delete_listings_from_db(unavailable_ids):
    """Delete listings from database with optimized batch processing"""
    if not unavailable_ids:
        return "No unavailable ids found"
    
    # Use parameterized query for better security and performance
    with db_manager.get_transaction() as conn:
        query = text(
            """
            DELETE FROM fact_listings
            WHERE listing_id = ANY(:ids)
            """
        )
        conn.execute(query, {"ids": unavailable_ids})
    return f"Deleted {len(unavailable_ids)} listings"

def get_unit_type(unit_type):
    """
    Get the unity type
    """
    if unit_type == "APARTMENT":
        return ",".join(["APARTMENT"] * 5)
    elif unit_type == "HOME":
        return ",".join(["HOME"] * 4)
    else:
        raise ValueError(f"Invalid unit type: {unit_type}")

def get_unit_type_v3(unit_type):
    """
    Get the unit type v3
    """
    if unit_type == 'APARTMENT':
        return 'APARTMENT,UnitType_NONE,PENTHOUSE,FLAT,LOFT'
    elif unit_type == 'HOME':
        return 'HOME,TWO_STORY_HOUSE,CONDOMINIUM,VILLAGE_HOUSE'
    else:
        raise ValueError(f"Invalid unit type: {unit_type}")

def get_unit_subtype(unit_type):
    """
    Get the unity subtype
    """
    if unit_type == "APARTMENT":
        return "APARTMENT,UnitSubType_NONE,DUPLEX,TRIPLEX|STUDIO|PENTHOUSE|FLAT"
    elif unit_type == "HOME":
        return "UnitSubType_NONE,TWO_STORY_HOUSE,SINGLE_STOREY_HOUSE,KITNET|TWO_STORY_HOUSE|CONDOMINIUM|VILLAGE_HOUSE"
    else:
        raise ValueError(f"Invalid unit type: {unit_type}")

def get_search_parameters():
    """
    Get the search parameters from the command prompt
    Returns:
        state (str): State to scrape
        city (str): City to scrape
        unit_type (str): Type of construction, APARTMENT or HOME
        neighborhoods (list): Neighborhoods to scrape
    """

    neighborhoods = [""]
    assert len(sys.argv) >= 4, "Please provide at least the following \
        arguments: state, city, unit and business type"
    assert sys.argv[3] in ["APARTMENT", "HOME"], \
        "Unit type must be APARTMENT or HOME"
    assert sys.argv[4] in ["SALE", "RENTAL"], \
        "Business type must be either SALE or RENTAL"

    if len(sys.argv) == 5:
        logger.info(f"Running for {sys.argv[3]} {sys.argv[4]} on all neighborhoods in {sys.argv[1]} - {sys.argv[2]}")
        neighborhoods = get_neighborhoods_from_city_and_state(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 6:
        logger.info(f"Running for {sys.argv[3]} {sys.argv[4]} at {sys.argv[5]} neighborhoods in {sys.argv[1]} - {sys.argv[2]}")
        neighborhoods = sys.argv[5].split(",")
    else:
        logger.error(
            f"""Please provide at most 5 search parameters, being the
            following:state, city, unit type, listing business type and neighborhoods.
            Received {len(sys.argv)} parameters, which are: {sys.argv}"""
        )
        sys.exit(1)
    
    unit_type = get_unit_type(sys.argv[3])
    unit_type_v3 = get_unit_type_v3(sys.argv[3])
    unit_subtype = get_unit_subtype(sys.argv[3])

    return sys.argv[1], sys.argv[2], unit_type, unit_type_v3, unit_subtype, sys.argv[4], neighborhoods
