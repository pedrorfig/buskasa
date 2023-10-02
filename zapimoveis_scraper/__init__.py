import time
import pandas as pd
from sqlalchemy import create_engine
from zapimoveis_scraper.classes import ZapItem, ZapPage
from datetime import date
import os
from dotenv import load_dotenv
import socket

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
        db_uri = f'postgresql://{user}:{password}@dpg-ck7ghkvq54js73fbrei0-a.oregon-postgres.render.com/house_listings'
    else:
        db_uri = f'postgresql://{user}:{password}@dpg-ck7ghkvq54js73fbrei0-a/house_listings'
    engine = create_engine(db_uri, future=True, pool_pre_ping=True)

    return engine


def convert_to_dataframe(data):
    """
    Simple function to convert the data from objects to a pandas DataFrame
    Args:
        data (list of ZapItem): Empty default dictionary
    """
    # Iterate through your objects
    obj_data = []
    for obj in data:
        # Create a dictionary to hold the attributes of the current object

        # Get all attributes of the current object using vars()
        object_dict = vars(obj)

        # Append the dictionary to the data list
        obj_data.append(object_dict)

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(obj_data)
    cols_to_drop = []
    for col in df.columns:
        if col.startswith('_'):
            cols_to_drop.append(col)
    df = df.drop(columns=cols_to_drop)
    return df


def search(business_type: str, state: str, city: str, neighborhoods: list, usage_type: str, unit_type: str,
           min_area: int, max_price: int, time_to_wait=0):
    """

    Args:
        business_type:
        state:
        city:
        neighborhoods:
        usage_type:
        unit_type:
        min_area:
        max_price:
        time_to_wait:

    Returns:

    """
    for neighborhood in neighborhoods:
        page = 0
        print(f"Getting listings from neighborhood {neighborhood}")
        while True:
            print(f"Page #{page} on {neighborhood}")
            existing_ids = get_available_ids()
            zap_page = ZapPage(business_type, state, city, neighborhood, usage_type, unit_type, min_area, max_price,page)
            zap_page.get_page()
            listings = zap_page.get_listings()
            if not listings:
                break
            for listing in listings:
                listing_id = listing.get('listing').get('sourceId')
                if listing_id not in existing_ids:
                    item = ZapItem(listing, zap_page)
                    zap_page.add_zap_item(item)
            # Convert output to standard format before saving
            zap_page.convert_zip_code_to_df()
            zap_page.convert_listing_to_df()
            # Treating listings
            zap_page.remove_fraudsters()
            zap_page.remove_outliers()
            # Save results to db
            zap_page.save_listings_to_db()
            zap_page.save_zip_codes_to_db()
            # Close engine
            zap_page.close_engine()
            page += 1
        time.sleep(time_to_wait)
    return


def get_available_ids():
    engine = create_db_engine().connect()
    with engine as conn:
        ids = pd.read_sql('SELECT DISTINCT listing_id from listings', con=conn)
    ids_list = [*ids['listing_id']]
    return ids_list


def check_if_update_needed(test: bool):
    """
    Check if the data was already updated in the current day
    Args:
        test:

    Returns:

    """
    if test:
        return True
    today_date = date.today().strftime('%Y-%m-%d')
    db_connection = create_db_engine().connect()
    with db_connection as conn:
        update_table = pd.read_sql('SELECT * from update_date', con=conn)
        last_date = update_table['update_date'][0]
        if today_date == last_date:
            return False
        else:
            return True

def read_listings_sql_table():
    """
    Read house listings from db table
    Returns:

    """
    engine = create_db_engine().connect()
    with engine as conn:
        search_results = pd.read_sql('SELECT * from listings', con=conn, index_col='listing_id')
    return search_results

def is_running_locally():
    """
    Check if code is running locally or in the cloud

    Returns:
    """
    hostname = socket.gethostname()
    return hostname == "localhost" or hostname == "127.0.0.1" or hostname == 'SAOX1Y6-58781'
