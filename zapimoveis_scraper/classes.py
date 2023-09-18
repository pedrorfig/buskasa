import numpy as np
import pandas as pd
from geopy import Nominatim

class ZapItem:
    """
    Zap Imoveis listing object
    """
    def __init__(self, listing):
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
        self.street_address = listing['link']['data']['street']
        self.street_number = listing['link']['data']['streetNumber']
        self.neighborhood = listing['link']['data']['neighborhood']
        self.zip_code = listing['listing']['address']['zipCode']
        self.city = listing['listing']['address']['city']
        self.state = listing['listing']['address']['stateAcronym']
        self.country = listing['listing']['address']['country']
        self.address = ", ".join([self.street_address + " " + self.street_number, self.neighborhood,
                                  self.zip_code, self.city, self.state, self.country])
        self.description = listing['listing']['title']
        self.url = 'https://www.zapimoveis.com.br' + listing['link']['href']
        self.link = f'<a href="{self.url}">{self.description}</a>'
        self.latitude = self.get_latitude(listing)
        self.longitude = self.get_longitude(listing)
        self.advertizer = listing['account']['name']

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

    def get_latitude(self, listing):

        try:
            lat = listing['listing']['address']['point']['lat']
        except KeyError:
            if self.street_number == "":
                return np.nan
            else:
                locator = Nominatim(user_agent='myGeocoder')
                location = locator.geocode(self.address)
                if location is None:
                    lat = np.nan
                else:
                    lat = location.latitude
        return lat

    def get_longitude(self, listing):

        try:
            lon = listing['listing']['address']['point']['lon']
        except KeyError:
            if self.street_number == "":
                return np.nan
            else:
                locator = Nominatim(user_agent='myGeocoder')
                location = locator.geocode(self.address)
                if location is None:
                    lon = np.nan
                else:
                    lon = location.longitude
        return lon
