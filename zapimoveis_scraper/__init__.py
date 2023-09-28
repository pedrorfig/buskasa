import copy

import requests
import time
from scipy import stats
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from zapimoveis_scraper.classes import ZapItem
from datetime import date
import os
from dotenv import load_dotenv
import socket

load_dotenv()


def get_page(tipo_negocio, state, city, neighborhood, usage_type, unit_type, min_area, max_price, page):
    """
    Get results from a house search at Zap Imoveis
    Args:
        tipo_negocio (str):
        state (str):
        city (str):
        neighborhood (str):
        usage_type (str):
        unit_type:
        min_area:
        max_price:
        page:

    Returns:

    """
    number_of_listings = 100
    initial_listing = number_of_listings * page

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
        'Authorization': 'Bearer undefined',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Origin': 'https://www.zapimoveis.com.br',
        'Referer': 'https://www.zapimoveis.com.br/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'X-DeviceId': '0d645541-36ea-45b4-9c59-deb2d736595c',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'x-domain': '.zapimoveis.com.br',
    }
    params = {
        'user': '0d645541-36ea-45b4-9c59-deb2d736595c',
        'portal': 'ZAP',
        'categoryPage': 'RESULT',
        'developmentsSize': '0',
        'superPremiumSize': '0',
        'business': tipo_negocio,
        'parentId': 'null',
        'listingType': 'USED',
        'unitTypesV3': unit_type,
        'unitTypes': unit_type,
        'usableAreasMin': min_area,
        'priceMax': max_price,
        'priceMin': 100000,
        'addressCity': city,
        'addressState': state,
        'addressNeighborhood': neighborhood,
        'page': '1',
        'from': initial_listing,
        'size': number_of_listings,
        'usageTypes': usage_type
    }
    response = requests.get('https://glue-api.zapimoveis.com.br/v2/listings', params=params, headers=headers)
    data = response.json()

    return data

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
    engine = create_engine(db_uri, future=True)

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


def get_listings(data):
    """
    Get listings from a house search at Zap Imoveis
    Args:
        data (JSON string): Response content from a Zap Imoveis search result
    """
    listings = data.get('search', {}).get('result', {}).get('listings', 'Not a listing')
    return listings


def search(tipo_negocio: str, state: str, city: str, neighborhoods: list, usage_type: str, unit_type: str,
           min_area: int, max_price: int, dataframe_out=False, time_to_wait=0):
    """

    Args:
        tipo_negocio:
        state:
        city:
        neighborhoods:
        usage_type:
        unit_type:
        min_area:
        max_price:
        dataframe_out:
        time_to_wait:

    Returns:

    """
    items = []
    existing_ids = get_available_ids()
    for neighborhood in neighborhoods:
        page = 0
        listings = None
        while listings != []:
            page_data = get_page(tipo_negocio, state, city, neighborhood, usage_type, unit_type, min_area, max_price,page)
            listings = get_listings(page_data)
            if listings != 'Not a listing':
                for listing in listings:
                    listing_id = listing.get('listing').get('sourceId')
                    if listing_id not in existing_ids:
                        item = ZapItem(listing)
                        items.append(item)
            page += 1
            time.sleep(time_to_wait)
    if dataframe_out:
        return convert_to_dataframe(items)
    return items

def get_available_ids():
    engine = create_db_engine()
    with engine.connect() as conn:
        ids = pd.read_sql('SELECT id from listings', con=conn)
    ids_list = [*ids['id']]
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
    db_connection = create_db_engine()
    with db_connection as conn:
        update_table = pd.read_sql('SELECT * from update_date', con=conn)
        last_date = update_table['update_date'][0]
        if today_date == last_date:
            return False
        else:
            return True


def export_results_to_db(data):
    """
    Export listing results to the cloud
    Args:
        data (pandas DataFrame): House search results
    """

    today_date = date.today().strftime('%Y-%m-%d')
    update_date = pd.DataFrame({'update_date': [today_date]})
    engine = create_db_engine()
    with engine.connect() as conn:
        data.to_sql(name='listings', con=conn, index=True, if_exists='append')
        update_date.to_sql(name='update_date', con=conn, if_exists='replace', index=False)
    return

def read_listings_sql_table():
    """
    Read house listings from db table
    Returns:

    """
    engine = create_db_engine()
    with engine.connect() as conn:
        search_results = pd.read_sql('SELECT * from listings', con=conn, index_col='id')
    return search_results

def remove_fraudsters(search_results):
    """
    Remove possible fraudsters from house listings
    Args:
        search_results:

    Returns:

    """
    # Removing known fraudster
    search_results = search_results[search_results['advertizer'] != "Camila Damaceno Bispo"]
    # Removing fraudsters by primary phone location inconsistency
    search_results = search_results[search_results['primary_phone'].str.startswith('11')]
    return search_results

def remove_outliers(data_with_outliers, feature='price_per_area'):
    """
    Removing outlier on assigned feature

    Args:
        feature:
        data_with_outliers:
    """
    z = np.abs(stats.zscore(data_with_outliers[feature]))
    data_without_outliers = data_with_outliers[z < 3]
    return data_without_outliers

def is_running_locally():
    """
    Check if code is running locally or in the cloud

    Returns:
    """
    hostname = socket.gethostname()
    return hostname == "localhost" or hostname == "127.0.0.1" or hostname == 'SAOX1Y6-58781'
