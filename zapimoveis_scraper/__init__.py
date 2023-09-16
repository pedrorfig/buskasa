import requests
import time
import pandas as pd
import plotly.express as px
from zapimoveis_scraper.item import ZapItem
from collections import defaultdict

__all__ = [
    # Main search function.
    'search',
]


def get_page(tipo_negocio, state, city, neighborhood, usage_type, min_area, max_price, page):
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
    initial_listing = number_of_listings*page

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
        'unitTypesV3': 'APARTMENT,HOME',
        'unitTypes': 'APARTMENT,HOME',
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
    listings = data.get('search')['result']['listings']
    return listings

def search(tipo_negocio, state, city, neighborhood, usage_type,min_area, max_price, num_pages=1, dataframe_out=False, time_to_wait=0):

    items = []

    for page in range(1, num_pages+1):
        page_data = get_page(tipo_negocio, state, city, neighborhood, usage_type, min_area, max_price, page)
        listings = get_listings(page_data)
        for listing in listings:
            if 'type' not in listing or listing['type'] != 'nearby':
                item = ZapItem(listing)
                items.append(item)
        page += 1
        time.sleep(time_to_wait)

    if dataframe_out:
        return convert_to_dataframe(items, item.get_instance_attributes())

    return items

def export_results(data, path=r".\house_search.csv"):
    """
    Export listing results to the cloud or local file
    Args:
        data (pandas DataFrame): House search results
    """
    data.to_csv(path, sep=';')

    return

def create_map(search_results, mapbox_token):

    px.set_mapbox_access_token(mapbox_token)
    fig = px.scatter_mapbox(search_results, lat="latitude", lon="longitude", size="price",
                            color='price_per_area', hover_name='description', zoom=15,  hover_data='link')
    fig.show()

def filter_results(search_results, max_price_per_area, min_price_per_area):

    search_results = search_results[search_results['price_per_area'].between(min_price_per_area, max_price_per_area)]
    return search_results