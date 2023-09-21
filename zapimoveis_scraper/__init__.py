import sqlite3
import numpy as np
import requests
import time
import pandas as pd
import plotly.express as px
from zapimoveis_scraper.classes import ZapItem
from collections import defaultdict
from datetime import date
import plotly.graph_objects as go


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
            page_data = get_page(tipo_negocio, state, city, neighborhood, usage_type, unit_type, min_area, max_price,
                                 page)
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


def export_results(data):
    """
    Export listing results to the cloud or local file
    Args:
        data (pandas DataFrame): House search results
    """

    today_date = date.today().strftime('%Y-%m-%d')
    update_date = pd.DataFrame({'update_date': [today_date]})

    connection = sqlite3.connect('..\data\listings.db')
    with connection as conn:
        data.to_sql(name='houses', con=conn, if_exists='replace')
        update_date.to_sql(name='update_date', con=conn, if_exists='replace', index=False)
    return


def create_map(search_results, mapbox_token):
    px.set_mapbox_access_token(mapbox_token)

    size = 1 / search_results['price_per_area']

    hover_template = ('<b>%{customdata[0]}</b> <br>' +
                      'Price: R$ %{customdata[1]:,.2f} <br>' +
                      'Price per Area: R$/m<sup>2</sup> %{customdata[2]:,.2f} <br>' +
                      'Condo Fee: R$ %{customdata[3]:,.2f} <br>' +
                      'Usable Area: %{customdata[4]} m<sup>2</sup> <br>' +
                      'Floor: %{customdata[5]}')

    custom_data = np.stack((search_results['link'], search_results['price'], search_results['price_per_area'],
                            search_results['condo_fee'], search_results['total_area_m2'], search_results['floor']),
                           axis=1)
    fig = go.Figure()

    fig.add_trace(
        go.Scattermapbox(
            lat=search_results['latitude'],
            lon=search_results['longitude'],
            mode='markers',
            name='',
            customdata=custom_data,
            hovertemplate=hover_template,
            marker=go.scattermapbox.Marker(
                size=size,
                sizemin=5,
                sizeref=0.00001,
                colorscale='plotly3_r',
                color=search_results['price_per_area'],
                colorbar=dict(title='Price per Area (R$/m<sup>2</sup>)')
            ),
        )
    )

    fig.update_layout(
        title='Best Deals in São Paulo',
        hovermode='closest',
        hoverdistance=50,
        hoverlabel=dict(
            bgcolor="white",
            font_size=16,
            font_family="Rockwell"
        ),
        mapbox=dict(
            style='outdoors',
            accesstoken=mapbox_token,
            bearing=0,
            center=dict(
                lat=search_results['latitude'].mean(),
                lon=search_results['longitude'].mean()
            ),
            pitch=0,
            zoom=15
        ),
    )

    fig.show()


def filter_results(min_price_per_area, max_price_per_area):
    # read data
    connection = sqlite3.connect('.\data\listings.db')
    with connection as conn:
        search_results = pd.read_sql('SELECT * from houses', con=conn)
    search_results = search_results[search_results['price_per_area'].between(min_price_per_area, max_price_per_area)]
    return search_results


def remove_fraudsters(search_results):
    # Removing known fraudster
    search_results = search_results[search_results['advertizer'] != "Camila Damaceno Bispo"]
    return search_results
