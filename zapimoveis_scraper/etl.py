import time
import pandas as pd
from sqlalchemy import create_engine
from zapimoveis_scraper.classes import ZapItem, ZapPage
from datetime import date
import os
import socket
from dotenv import load_dotenv

load_dotenv()

city = 'São Paulo'
state = 'São Paulo'
business_type = 'SALE'
usage_type = 'RESIDENTIAL'
unit_type = 'APARTMENT'
min_area = 100
min_price = 1000000
max_price = 1500000
# neighborhoods = ['Pinheiros', 'Vila Madalena,]
neighborhoods = ['Bela Vista', 'Vila Mariana', 'Jardim Paulista', 'Jardins', 'Jardim Europa', 'Consolação',
                 'Cerqueira César', 'Higienópolis', 'Itaim Bibi', 'Ibirapuera', 'Vila Nova Conceição', 'Vila Olímpia',
                 'Sumaré', 'Perdizes', 'Pacaembu']

def save(zap_page):
    # Save results to db
    zap_page.save_listings_to_db()
    zap_page.save_zip_codes_to_db()
    # Close engine
    zap_page.close_engine()


def transform(zap_page):
    # Convert output to standard format before saving
    zap_page.convert_zip_code_to_df()
    zap_page.convert_listing_to_df()
    # Treating listings
    zap_page.remove_fraudsters()
    zap_page.remove_outliers()
    return zap_page


def extract(business_type, city, max_price, min_area, min_price, neighborhood, state, unit_type, usage_type):
    page = 0
    print(f"Getting listings from neighborhood {neighborhood}")
    while True:
        print(f"Page #{page} on {neighborhood}")
        zap_page = ZapPage(business_type, state, city, neighborhood, usage_type, unit_type, min_area, min_price,
                           max_price,
                           page)
        zap_page.get_page()
        zap_page.get_available_ids()
        listings = zap_page.get_listings()
        if not listings:
            break
        zap_page.create_zap_items()
        page += 1
    return zap_page


def search(business_type: str, state: str, city: str, neighborhoods: list, usage_type: str, unit_type: str,
           min_area: int, min_price: int, max_price: int):
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
    Returns:

    """
    for neighborhood in neighborhoods:
        zap_page = extract(business_type, city, max_price, min_area, min_price, neighborhood, state, unit_type,
                           usage_type)
        zap_page = transform(zap_page)
        save(zap_page)

if __name__ == '__main__':
    #  run EKD's algorithm to the list of given episodes
    search(business_type, state, city, neighborhoods, usage_type, unit_type, min_area, min_price, max_price)
