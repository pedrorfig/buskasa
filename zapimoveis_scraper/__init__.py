#!/usr/bin/env python

# Python bindings to the Google search engine
# Copyright (c) 2009-2016, Geovany Rodrigues
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice,this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the copyright holder nor the names of its
#       contributors may be used to endorse or promote products derived from
#       this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
from urllib.request import Request, urlopen

import requests
import time
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
        'usableAreasMin': min_area,
        'priceMax': max_price,
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


def convert_dict(data):
    """
    Simple function to convert the data from objects to a dictionary
    Args:
        data (list of ZapItem): Empty default dictionary
    """
    # start dictonary
    dicts = defaultdict(list)
    # create a list with the keys
    keys = [attribute for attribute in dir(data[0]) if not attribute.startswith('__')]

    # simple for loops to create the dictionary
    for i in keys:
        for j in range(len(data)):
            to_dict = data[j].__dict__
            dicts[i].append(to_dict['%s' % i])

    return dicts


def get_listings(data):
    """
    Get listings from a house search at Zap Imoveis
    Args:
        data (JSON string): Response content from a Zap Imoveis search result
    """
    listings = data['search']['result']['listings']
    return listings

def search(tipo_negocio, state, city, neighborhood, usage_type,min_area, max_price, num_pages=1, dictionary_out=False, time_to_wait=0):

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

    if dictionary_out:
        return convert_dict(items)

    return items
