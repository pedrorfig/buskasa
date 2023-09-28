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
        # Getting listing data
        self._listing_data = listing
        self.id = self.get_listing_id()
        self.listing_date = self.get_listing_date()
        self.new_listing = self.is_new_listing()
        self.description = self.get_listing_title()
        # Getting cost data
        self.price = self.get_listing_price(listing)
        self.condo_fee = self.get_condo_fee(listing)
        # Getting house attributes
        self.bedrooms = self.get_number_of_bedrooms()
        self.bathrooms = self.get_number_of_bathrooms()
        self.vacancies = self.get_number_of_parking_spaces()
        self.floor = self.get_floor_number()
        self.construction_year = self.get_construction_year()
        self.total_area_m2 = self.get_usable_area()
        self.price_per_area = self.get_price_per_area()
        # Getting location data
        self.country = self.get_listing_country()
        self.state = self.get_state_acronym()
        self.city = self.get_city()
        self.neighborhood = self.get_neighborhood()
        self.zip_code = self.get_zip_code()
        self.street_address = self.get_street_address()
        self.street_number = self.get_street_number()
        self.location_type = self.get_location_type()
        self.address = self.get_complete_address()
        self.precision = None
        self.latitude = self.get_latitude()
        self.longitude = self.get_longitude()
        # Getting reference data
        self.url = self.get_listing_url()
        self.link = self.get_html_link()
        # Advertizer info
        self.advertizer = self.get_advitizer_name()
        self.primary_phone = self.get_primary_phone()

    def get_primary_phone(self):
        return self._listing_data['account']['phones']['primary']

    def get_advitizer_name(self):
        return self._listing_data['account']['name']

    def get_html_link(self):
        return f'<a href="{self.url}">{self.description}</a>'
    def get_listing_url(self):
        return 'https://www.zapimoveis.com.br' + self._listing_data['link']['href']

    def get_complete_address(self):
        return ", ".join([self.street_address + " " + self.street_number, self.neighborhood,
                          self.zip_code, self.city, self.state, self.country])

    def get_street_address(self):
        return self._listing_data['link']['data']['street']

    def get_zip_code(self):
        return self._listing_data['listing']['address']['zipCode']

    def get_neighborhood(self):
        return self._listing_data['link']['data']['neighborhood']

    def get_city(self):
        return self._listing_data['listing']['address']['city']

    def get_state_acronym(self):
        return self._listing_data['listing']['address']['stateAcronym']

    def get_listing_country(self):
        return self._listing_data['listing']['address']['country']

    def get_price_per_area(self):
        return self.price / self.total_area_m2

    def get_usable_area(self):
        return int(self._listing_data['listing']['usableAreas'][0] if len(
            self._listing_data['listing']['usableAreas']) > 0 else 0)

    def get_floor_number(self):
        return self._listing_data.get('listing', {}).get('unitFloor', -1)

    def get_number_of_parking_spaces(self):
        return self._listing_data['listing']['parkingSpaces'][0] if len(
            self._listing_data['listing']['parkingSpaces']) > 0 else 0

    def get_number_of_bathrooms(self):
        return int(
            self._listing_data['listing']['bathrooms'][0] if len(self._listing_data['listing']['bathrooms']) > 0 else 0)

    def get_number_of_bedrooms(self):
        return int(
            self._listing_data['listing']['bedrooms'][0] if len(self._listing_data['listing']['bedrooms']) > 0 else 0)

    def get_condo_fee(self, listing):
        return int(self._listing_data['listing']['pricingInfos'][0].get('monthlyCondoFee', 0)) if len(
            listing['listing']['pricingInfos']) > 0 else 0

    def get_listing_price(self, listing):
        return int(self._listing_data['listing']['pricingInfos'][0].get('price', None)) if len(
            listing['listing']['pricingInfos']) > 0 else 0

    def get_listing_title(self):
        return self._listing_data['listing']['title']

    def get_listing_date(self):
        return datetime.fromisoformat(self._listing_data['listing']['createdAt'].replace('Z', '+00:00')).date()

    def get_listing_id(self):
        return self._listing_data.get('listing').get('sourceId')

    def get_location_type(self):
        street_address_split = self.street_address.split()
        if street_address_split:
            return self.street_address.split()[0]
        return 'N/A'

    def get_street_number(self):
        assigned_number = self._listing_data['link']['data']['streetNumber']
        if assigned_number != "":
            return assigned_number
        else:
            return self.get_random_streetnumber_from_zipcode()

    def get_construction_year(self):

        try:
            deliver_date = self._listing_data['listing']['deliveredAt']
            deliver_year = pd.to_datetime(deliver_date, errors='coerce').year
        except KeyError:
            deliver_year = -1
        return deliver_year

    def get_instance_attributes(self):
        attributes = []
        for attribute, value in self.__dict__.items():
            if not attribute.startswith('_'):
                attributes.append(attribute)
        return attributes

    def get_random_streetnumber_from_zipcode(self):
        """
        Assign
        Returns:

        """
        zip_code = self.zip_code
        random_limited_numbers = str(round(random.uniform(0, 1000)))
        if zip_code != "":
            try:
                response = r.get(f'https://viacep.com.br/ws/{zip_code}/json/')
                zip_data = response.json()
                street_complement = zip_data['complemento']
                street_complement_numbers = re.findall(r'\d+', street_complement)
                if street_complement_numbers:
                    num_max = int(max(street_complement_numbers))
                    num_min = int(min(street_complement_numbers))
                    random_number = str(round(random.uniform(num_min, num_max)))
                else:
                    random_number = random_limited_numbers
            except ConnectionError:
                random_number = random_limited_numbers
        else:
            random_number = ""
        return random_number

    def get_latitude(self):

        try:
            lat = self._listing_data['listing']['address']['point']['lat']
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

    def get_longitude(self):

        try:
            lon = self._listing_data['listing']['address']['point']['lon']
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
