import random
import re

import numpy as np
import pandas as pd
from geopy import Nominatim
from datetime import datetime, timedelta
import requests as r


class ZapItem:
    """
    Zap Imoveis listing object
    """
    def __init__(self, listing):
        self.id = listing['listing']['sourceId']
        self.listing_date = datetime.fromisoformat(listing['listing']['createdAt'].replace('Z', '+00:00')).date()
        self.new_listing = self.is_new_listing()
        self.price = int(listing['listing']['pricingInfos'][0].get('price', None)) if len(
            listing['listing']['pricingInfos']) > 0 else 0
        self.condo_fee = int(listing['listing']['pricingInfos'][0].get('monthlyCondoFee', 0)) if len(
            listing['listing']['pricingInfos']) > 0 else 0
        self.bedrooms = int(listing['listing']['bedrooms'][0] if len(listing['listing']['bedrooms']) > 0 else 0)
        self.bathrooms = int(listing['listing']['bathrooms'][0] if len(listing['listing']['bathrooms']) > 0 else 0)
        self.vacancies = listing['listing']['parkingSpaces'][0] if len(listing['listing']['parkingSpaces']) > 0 else 0
        self.floor = listing.get('listing', {}).get('unitFloor', -1)
        self.construction_year = self.get_construction_year(listing)
        self.total_area_m2 = int(
            listing['listing']['usableAreas'][0] if len(listing['listing']['usableAreas']) > 0 else 0)
        self.price_per_area = self.price / self.total_area_m2
        self.country = listing['listing']['address']['country']
        self.state = listing['listing']['address']['stateAcronym']
        self.city = listing['listing']['address']['city']
        self.neighborhood = listing['link']['data']['neighborhood']
        self.zip_code = listing['listing']['address']['zipCode']
        self.street_address = listing['link']['data']['street']
        self.street_number = self.get_street_number(listing)
        self.address = ", ".join([self.street_address + " " + self.street_number, self.neighborhood,
                                  self.zip_code, self.city, self.state, self.country])
        self.description = listing['listing']['title']
        self.url = 'https://www.zapimoveis.com.br' + listing['link']['href']
        self.link = f'<a href="{self.url}">{self.description}</a>'
        self.precision = None
        self.latitude = self.get_latitude(listing)
        self.longitude = self.get_longitude(listing)
        self.advertizer = listing['account']['name']

    def get_street_number(self, listing):
        assigned_number = listing['link']['data']['streetNumber']
        if assigned_number != "":
            return assigned_number
        else:
            return self.get_random_streetnumber_from_zipcode()

    def get_construction_year(self, listing):

        try:
            deliver_date = listing['listing']['deliveredAt']
            deliver_year = pd.to_datetime(deliver_date, errors='coerce').year
        except KeyError:
            deliver_year = -1
        return deliver_year
    def get_instance_attributes(self):
        attributes = []
        for attribute, value in self.__dict__.items():
            attributes.append(attribute)
        return attributes

    def get_random_streetnumber_from_zipcode(self):
        zip_code = self.zip_code
        response = r.get(f'https://viacep.com.br/ws/{zip_code}/json/')
        zip_data = response.json()
        street_complement = zip_data['complemento']
        street_complement_numbers = re.findall(r'\d+', street_complement)
        if street_complement_numbers:
            num_max = int(max(street_complement_numbers))
            num_min = int(min(street_complement_numbers))
            random_number = str(round(random.uniform(num_min, num_max)))
        else:
            random_number = str(round(random.uniform(0, 1000)))
        return random_number
    def get_latitude(self, listing):

        try:
            lat = listing['listing']['address']['point']['lat']
            self.precision = 'exact'
        except KeyError:
            if self.street_number == "" or self.street_address == "":
                return np.nan
            else:
                try:
                    locator = Nominatim(user_agent='zap_scraper')
                    location = locator.geocode(self.address)
                    lat = location.latitude
                    self.precision = 'approximate'
                except:
                    lat = np.nan
        return lat
    def get_longitude(self, listing):

        try:
            lon = listing['listing']['address']['point']['lon']
        except KeyError:
            if self.street_number == "" or self.street_address == "":
                return np.nan
            else:
                try:
                    locator = Nominatim(user_agent='zap_scraper')
                    location = locator.geocode(self.address)
                    lon = location.longitude
                except:
                    lon = np.nan
        return lon


    def is_new_listing(self):

        current_date = datetime.now().date()
        one_month_ago = current_date - timedelta(days=30)

        if self.listing_date >= one_month_ago:
            return True
        else:
            return False
