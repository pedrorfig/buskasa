import sqlite3
import requests
import time
import pandas as pd
from sqlalchemy import create_engine
from zapimoveis_scraper.classes import ZapItem
from collections import defaultdict
from datetime import date
import os
from dotenv import load_dotenv
import socket

load_dotenv()


def get_page(tipo_negocio, state, city, neighborhood, usage_type, unit_type, min_area, max_price, page):
    """
    Get results from a house search at Zap Imoveis
    Args:
        min_area:
        max_price:
        tipo_negocio (str):
        state (str):
        city (str):
        neighborhood (str):
        page (int):
        usage_type (str):

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

def create_db_engine(default_schema=None, user=os.getenv('DB_USER'), password=os.getenv('DB_PASS'), port=5432):
    """
    Creates engine needed to create connections to the database
    with the credentials and parameters provided.

    Args:
        default_schema (str): Schema that will be used as default to query data on DB
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

    db_uri = f'postgres://{user}:{password}@dpg-ck7ghkvq54js73fbrei0-a/house_listings'
    if is_running_locally():
        db_uri = f'postgresql://{user}:{password}@dpg-ck7ghkvq54js73fbrei0-a.oregon-postgres.render.com/house_listings'
    engine = create_engine(db_uri, future=True)

    return engine


def convert_to_dataframe(data, attributes):
    """
    Simple function to convert the data from objects to a pandas DataFrame
    Args:
        data (list of ZapItem): Empty default dictionary
    """
    # start dictonary
    dicts = defaultdict(list)
    # create a list with the keys
    keys = attributes

    # simple for loops to create the dictionary
    for i in keys:
        for j in range(len(data)):
            to_dict = data[j].__dict__
            dicts[i].append(to_dict['%s' % i])
    results = pd.DataFrame(dicts)
    return results


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
    items = []
    for neighborhood in neighborhoods:
        page = 0
        listings = None
        while listings != []:
            page_data = get_page(tipo_negocio, state, city, neighborhood, usage_type, unit_type, min_area, max_price,page)
            listings = get_listings(page_data)
            if listings != 'Not a listing':
                for listing in listings:
                    if 'type' not in listing or listing['type'] != 'nearby':
                        item = ZapItem(listing)
                        items.append(item)
            page += 1
            time.sleep(time_to_wait)

    if dataframe_out:
        return convert_to_dataframe(items, item.get_instance_attributes())

    return items


def check_if_update_needed(test: bool):
    if test:
        return True
    today_date = date.today().strftime('%Y-%m-%d')
    connection = sqlite3.connect('..\data\listings.db')
    with connection as conn:
        update_table = pd.read_sql('SELECT * from update_date', con=conn)
        last_date = update_table['update_date'][0]
        if today_date == last_date:
            return False
        else:
            return True


def export_results_to_db(data):
    """
    Export listing results to the cloud or local file
    Args:
        data (pandas DataFrame): House search results
    """

    today_date = date.today().strftime('%Y-%m-%d')
    update_date = pd.DataFrame({'update_date': [today_date]})
    engine = create_db_engine()
    with engine.connect() as conn:
        data.to_sql(name='listings', con=conn, index=True, if_exists='replace')
        update_date.to_sql(name='update_date', con=conn, if_exists='replace', index=False)
    return

def filter_results(search_results, min_price_per_area, max_price_per_area):
    # read data
    search_results = search_results[search_results['price_per_area'].between(min_price_per_area, max_price_per_area)]
    return search_results


def read_listings_sql_table():
    engine = create_db_engine()
    with engine.connect() as conn:
        search_results = pd.read_sql('SELECT * from listings', con=conn, index_col='id')
    return search_results
def read_listings_csv():
    search_results = pd.read_csv(r'./data/listings.csv', index_col='index')
    return search_results


def remove_fraudsters(search_results):
    # Removing known fraudster
    search_results = search_results[search_results['advertizer'] != "Camila Damaceno Bispo"]
    return search_results

def convert_sqlite_to_csv():

    sqllite_data = read_listings_sql_table()
    sqllite_data.to_csv(r'../data/listings.csv', index=False)

    return


def is_running_locally():
    hostname = socket.gethostname()
    return hostname == "localhost" or hostname == "127.0.0.1"
