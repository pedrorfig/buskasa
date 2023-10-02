import random
import re
import numpy as np
import pandas as pd
import requests
from geopy import Nominatim
from datetime import datetime, timedelta
import requests as r
from scipy.stats import stats

import zapimoveis_scraper as zap


class ZapPage:
    """
    Zap Imoveis page object
    """

    def __init__(self, business_type, state, city, neighborhood, usage_type, unit_type, min_area, max_price, batch_id):
        self._engine = zap.create_db_engine()
        self.batch_id = batch_id
        self.business_type = business_type
        self.state = state
        self.city = city
        self.neighborhood = neighborhood
        self.usage_type = usage_type
        self.unit_type = unit_type
        self.min_area = min_area
        self.max_price = max_price
        self.zip_code_to_add = {}
        self.zap_items_to_add = []
        self.zap_page_listings = None
        self.existing_zip_codes = self.read_zip_code_table()

    def get_page(self):
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
        initial_listing = number_of_listings * self.batch_id

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
            'business': self.business_type,
            'parentId': 'null',
            'listingType': 'USED',
            'unitTypesV3': self.unit_type,
            'unitTypes': self.unit_type,
            'usableAreasMin': self.min_area,
            'priceMax': self.max_price,
            'priceMin': 100000,
            'addressCity': self.city,
            'addressState': self.state,
            'addressNeighborhood': self.neighborhood,
            'page': '1',
            'from': initial_listing,
            'size': number_of_listings,
            'usageTypes': self.usage_type
        }
        response = requests.get('https://glue-api.zapimoveis.com.br/v2/listings', params=params, headers=headers)
        page_data = response.json()
        self.page_data = page_data

    def get_listings(self):
        """
        Get listings from a house search at Zap Imoveis
        Args:
            data (JSON string): Response content from a Zap Imoveis search result
        """

        listings = self.page_data.get('search', {}).get('result', {}).get('listings', 'Not a listing')
        if listings != 'Not a listing':
            return listings
        else:
            return None

    def add_zip_code(self, zip_code, complement):
        """

        Args:
            zip_code:
            complement:
        """
        self.zip_code_to_add[zip_code] = complement

    def convert_zip_code_to_df(self):
        """

        """
        zip_code_df = pd.DataFrame.from_dict(self.zip_code_to_add, columns=['complement'], orient='index')
        self.zip_code_df = zip_code_df

    def read_zip_code_table(self):
        """
        Read db table
        Returns:

        """
        engine = self._engine
        data = pd.read_sql(f'SELECT * from dim_zip_code', con=engine, index_col='zip_code')
        return data

    def save_zip_codes_to_db(self):
        """

        """
        print("Saving ZIP codes")
        zip_df = self.zip_code_df
        if not zip_df.empty:
            zip_df.to_sql(name='dim_zip_code', con=self._engine, if_exists='append', index=True, index_label='zip_code')

    def save_listings_to_db(self):
        """

        """
        print("Saving house listings")
        page_listings = self.zap_page_listings
        if not page_listings.empty:
            page_listings.to_sql(name='listings', con=self._engine, if_exists='append', index=False, index_label='listing_id')

    def add_zap_item(self, zap_item):

        self.zap_items_to_add.append(zap_item)

    def close_engine(self):
        self._engine.dispose()

    def convert_listing_to_df(self):
        items = self.zap_items_to_add
        page_listings = zap.convert_to_dataframe(items)
        page_listings = page_listings.drop_duplicates(subset='listing_id')
        self.zap_page_listings = page_listings

    def remove_fraudsters(self):
        """
        Remove possible fraudsters from house listings
        Args:
            search_results:

        Returns:

        """
        print("Removing fraudsters")
        page_listings = self.zap_page_listings
        if not page_listings.empty:
            # Removing known fraudster
            page_listings = page_listings[page_listings['advertizer'] != "Camila Damaceno Bispo"]
            # Removing fraudsters by primary phone location inconsistency
            page_listings = page_listings[page_listings['primary_phone'].str.startswith('11')]

            self.zap_page_listings = page_listings

    def remove_outliers(self, feature='price_per_area'):
        """
        Removing outlier on assigned feature

        Args:
            feature:
            data_with_outliers:
        """
        print("Removing outliers on listing prices")
        page_listings = self.zap_page_listings
        if not page_listings.empty:
            z = np.abs(stats.zscore(page_listings[feature]))
            page_listings_without_outlier = page_listings[z < 3]
            self.zap_page_listings = page_listings_without_outlier


class ZapItem:
    """
    Zap Imoveis listing object
    """

    def __init__(self, listing, zap_page):
        # Getting ZapPage object
        self._zap_page = zap_page
        # Getting listing data
        self._listing_data = listing
        self.listing_id = self.get_listing_id()
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
            return self.get_random_street_number_from_zipcode()

    def get_construction_year(self):

        try:
            deliver_date = self._listing_data['listing']['deliveredAt']
            deliver_year = deliver_date[:4]
        except KeyError:
            deliver_year = -1
        return deliver_year

    def get_random_street_number_from_zipcode(self):
        """
        Assign
        Returns:

        """
        zip_code = self.zip_code
        existing_zip_codes = self._zap_page.existing_zip_codes
        random_limited_numbers = str(round(random.uniform(0, 1000)))
        if zip_code not in ["", "00000000"]:
            try:
                if zip_code not in existing_zip_codes.index:
                    street_complement = self.download_street_complement(zip_code)
                    self._zap_page.add_zip_code(zip_code, street_complement)
                else:
                    street_complement = existing_zip_codes.loc[self.zip_code, 'complement']
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

    def download_street_complement(self, zip_code):
        response = r.get(f'https://brasilaberto.com/api/v1/zipcode/{zip_code}.json')
        zip_data = response.json()
        street_complement = zip_data.get('result').get('complement')
        return street_complement

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