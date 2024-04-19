import os
import random
import re
import numpy as np
import pandas as pd
import requests
from datetime import datetime, timedelta
import requests as r
from etl_modules import extract, transform, save

class ZapSearch:
    """
    Object containing attributes for a search query on a complete neighborhood
    """

    def __init__(self, state: str, city: str, neighborhood: str, unit_type: str, usage_type: str, business_type: str, max_price: int, min_price: int, min_area: int ):
        # Create SQLalchemy engine for connections with DB
        self._engine = extract.create_db_engine()
        # Define filters used on the search
        self.state = state
        self.city = city
        self.neighborhood = neighborhood
        self.business_type = business_type
        self.unit_type = unit_type
        self.usage_type = usage_type
        self.max_price = max_price
        self.min_price = min_price
        self.min_area = min_area

        # Placeholder to save results from pages
        self.zap_pages = []
        # Save all listings from search to check if any was deleted on the web, so it is also deleted on the DB
        self.all_listing_from_search = []
        # Placeholder for saving ZIP codes that aren't currently on the DB
        self.existing_zip_codes = None
        self.zip_codes_to_add = pd.DataFrame()
        # Placeholder for saving ZIP codes that aren't currently on the DB
        self.listing_ids_to_remove = []
        self.listings_to_add = pd.DataFrame()
        self.existing_listing_ids_in_db = None


    def get_existing_ids(self):
        """
        Get existing listing ids for the specified conditions
        """
        engine = self._engine
        with engine.connect() as conn:
            # Checking for existing listing_ids on the database according to the specified filters
            ids = pd.read_sql(
                rf"""
                SELECT listing_id
                from listings
                where
                    city='{self.city}' and
                    neighborhood = '{self.neighborhood}' and
                    business_type = '{self.business_type}' and
                    total_area_m2 >= {self.min_area} and
                    price between {self.min_price} and {self.max_price}
                        """, con=conn)
            ids_list = [*ids['listing_id']]
        self.existing_listing_ids_in_db = ids_list


    def concat_zip_codes(self):
        """
        Concatenate zip codes from all pages searched
        """
        zip_codes = self.zip_codes_to_add
        if self.zap_pages:
            for zap_page in self.zap_pages:
                zap_page.convert_zap_page_zip_code_to_df()
                zip_codes = pd.concat([zip_codes, zap_page.zip_code_df])
            zip_codes = zip_codes.drop_duplicates()
            self.zip_codes_to_add = zip_codes

    def concat_listings(self):
        """
        Concatenate listings from all pages searched
        """
        listings = pd.DataFrame()
        for zap_page in self.zap_pages:
            zap_page.convert_zap_page_listing_to_df()
            listings = pd.concat([listings, zap_page.zap_page_listings])
        listings = listings.drop_duplicates(subset='listing_id')
        self.listings_to_add = listings

    def append_zap_pages(self, zap_page):
        """
        Append ZapPage object to other ZapPage objects in a ZapSearch
        Args:
            zap_page:
        """
        self.zap_pages.append(zap_page)

    def save_listings_to_check(self, listings):
        """
        Save all avaialble listings on a ZapPage, event those already on the database
        Args:
            listings (list): listings to save
        """
        self.all_listing_from_search.extend(listings)

    def remove_fraudsters(self):
        """
        Remove possible fraudsters from house listings
        Args:
            search_results:

        Returns:

        """
        print("Removing fraudsters")
        listings = self.listings_to_add
        # Listing users known to be fraudsters
        known_fraudsters = ["Camila Damaceno Bispo", "Lucas Antônio", "Imóveis São Caetano", "São Caetano Imóveis",
                            "Alex Matheus  Moura", "Claudia Cristina Ribeiro de Almeida"]
        listings_from_known_fraudsters = pd.Series()
        listings_from_phone_fraudsters = pd.Series()
        listings_with_total_area_typos = pd.Series()
        if not listings.empty:
            # Known fraudster
            listings_from_known_fraudsters = listings[listings['advertizer'].isin(known_fraudsters)]['listing_id']
            # Fraudsters by primary phone location inconsistency
            # listings_from_phone_fraudsters = listings[~listings['primary_phone'].str.startswith('11')]['listing_id']
            # Total area typos
            listings_with_total_area_typos = listings[listings['total_area_m2']>=500]['listing_id']
            # Populating series of listings to be removed
            listing_ids_to_remove = pd.concat([listings_from_known_fraudsters, listings_from_phone_fraudsters, listings_with_total_area_typos])
            self.listing_ids_to_remove = listing_ids_to_remove.to_list()
            # Remove listings
            cleaned_listings = listings.loc[~listings['listing_id'].isin(listing_ids_to_remove), :]

            self.listings_to_add = cleaned_listings

    def calculate_price_per_area_first_quartile(self):
        engine = self._engine
        with engine.connect() as conn:
            listings_on_db = pd.read_sql(
                f"""SELECT *
                        from listings
                        where
                        city = '{self.city}' and
                        neighborhood = '{self.neighborhood}' and
                        business_type = '{self.business_type}'
                    """,
                con=conn)
        search_listings = self.listings_to_add
        if not search_listings.empty:
            all_listings = pd.concat([search_listings, listings_on_db])
            # Calculate inter-quartile range
            q_low = all_listings["price_per_area"].quantile(0.25)
            self.neighborhood_price_per_area_first_quartile = q_low
            self.listings_to_add['price_per_area_in_first_quartile'] = self.listings_to_add['price_per_area'] <= q_low
    def remove_outliers(self):
        """
        Removing outlier on assigned feature

        Args:
            feature:
            data_with_outliers:
        """
        # TODO: Change outlier calculation for boxplot
        print("Removing outliers on listing prices")
        engine = self._engine
        with engine.connect() as conn:
            listings_on_db = pd.read_sql(
                f"""SELECT *
                        from listings
                        where
                        city = '{self.city}' and
                        neighborhood = '{self.neighborhood}' and
                        business_type = '{self.business_type}'
                    """,
                con=conn)
        search_listings = self.listings_to_add
        if not search_listings.empty and not listings_on_db.empty:
            all_listings = pd.concat([listings_on_db, search_listings])
            # Calculate interquartile range
            q_low = all_listings["price_per_area"].quantile(0.25)
            q_hi = all_listings["price_per_area"].quantile(0.75)
            inter_q_range = q_hi - q_low
            # Calculate outlier thresholds to remove anything above or below the limits assigned
            threshold_high = q_hi + inter_q_range*1.5
            threshold_low = q_low - inter_q_range*1.5
            outlier_listings = search_listings[
                (search_listings["price_per_area"] > threshold_high) & (search_listings["price_per_area"] < threshold_low)]['listing_id']
            # Removing price_per_area outlier listings
            cleaned_listings = search_listings.loc[~search_listings['listing_id'].isin(outlier_listings), :]
            self.listings_to_add = cleaned_listings
            # Saving listings removed
            listing_ids_to_remove = self.listing_ids_to_remove
            listing_ids_to_remove.extend(outlier_listings.to_list())
        else:
            self.listings_to_add = search_listings
        return 'Outliers removed'

    def remove_listings_deleted(self):
        """
        Remove listings that haven't been updated for more than a week
        """
        print('Removing deleted listings')
        engine = self._engine
        with engine.connect() as conn:
            old_listings = pd.read_sql(
                f"""SELECT listing_id
                        from listings
                        where
                        updated_at < current_date - 7
                    """,
                con=conn)
        if not old_listings.empty:
            old_listings = old_listings.squeeze()
            if type(old_listings) == str:
                old_listings = [old_listings]
            else:
                old_listings = old_listings.to_list()
            # Delete unavailable ids from db
            extract.delete_listings_from_db(old_listings, self._engine)
        return

    def get_request_headers(self):
        """

        Returns:

        """
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

        return headers

    def save_zip_codes_to_db(self):
        """

        """
        print("Saving ZIP codes to database")
        zip_df = self.zip_codes_to_add
        if not zip_df.empty:
            with self._engine.connect() as conn:
                zip_df.to_sql(name='dim_zip_code', con=conn, if_exists='append', index=True, index_label='zip_code')

    def save_listings_to_db(self):
        """

        """
        print("Saving listings to database")
        # Retrieve listing to be saved in the database
        listings = self.listings_to_add
        if not listings.empty:
            engine = self._engine
            with engine.begin() as conn:
                save.upsert_df(df=listings, table_name='listings', connection=conn)

    def get_existing_zip_codes(self):
        """
        Read db table
        Returns:

        """
        engine = self._engine
        with engine.connect() as conn:
            data = pd.read_sql(f'SELECT * from dim_zip_code', con=conn, index_col='zip_code')
        self.existing_zip_codes = data

    def close_engine(self):
        self._engine.dispose()


class ZapPage:
    """
    Zap Imoveis page object
    """

    def __init__(self, page_number, zap_search):

        self.page_number = page_number
        self.zap_search = zap_search
        self.business_type = zap_search.business_type
        self.state = zap_search.state
        self.city = zap_search.city
        self.neighborhood = zap_search.neighborhood
        self.usage_type = zap_search.usage_type
        self.unit_type = zap_search.unit_type
        self.min_area = zap_search.min_area
        self.min_price = zap_search.min_price
        self.max_price = zap_search.max_price
        self.zip_code_to_add = {}
        self.zap_items_to_add = []
        self.listings_to_check = []
        self.zap_page_listings = None
        self.existing_listing_ids_in_db = None
        self.existing_zip_codes = None

    def get_page(self):
        """
        Get results from a house search at Zap Imoveis

        Returns:

        """
        number_of_listings_per_page = 100
        initial_listing = number_of_listings_per_page * self.page_number
        headers = self.zap_search.get_request_headers()
        params = {
            'user': '0d645541-36ea-45b4-9c59-deb2d736595c',
            'portal': 'ZAP',
            'categoryPage': 'RESULT',
            'developmentsSize': '0',
            'superPremiumSize': '0',
            'business': self.business_type,
            'sort': f"pricingInfos.price ASC sortFilter:pricingInfos.businessType='{self.business_type}'",
            'parentId': 'null',
            'listingType': 'USED',
            'unitTypesV3': self.unit_type,
            'unitTypes': self.unit_type,
            'usableAreasMin': self.min_area,
            'priceMax': self.max_price,
            'priceMin': self.min_price,
            'addressCity': self.city,
            'addressState': self.state,
            'addressNeighborhood': self.neighborhood,
            'page': '1',
            'from': initial_listing,
            'size': number_of_listings_per_page,
            'usageTypes': self.usage_type,
            'levels': 'NEIGHBORHOOD'
        }
        response = requests.get('https://glue-api.zapimoveis.com.br/v2/listings', params=params, headers=headers)
        page_data = response.json()
        self.page_data = page_data

    def get_listings(self):
        """
        Get only listings from a ZapImoveis page API response
        
        """

        listings = self.page_data.get('search', {}).get('result', {}).get('listings', None)
        if listings is not None:
            self.listings = listings
        

    def add_zip_code(self, zip_code, complement):
        """
        Create new item on dictionary for zip codes
        Args:
            zip_code:
            complement:
        """
        self.zip_code_to_add[zip_code] = complement

    def check_if_search_ended(self):
        return self.page_data.get('page', {}).get('uriPagination', {}).get('from', 1) > self.page_data.get(
            'page', {}).get('uriPagination', {}).get('totalListingCounter', 0)

    def convert_zap_page_zip_code_to_df(self):
        """
        Convert zip code dict to df
        """
        zip_code_df = pd.DataFrame.from_dict(self.zip_code_to_add, columns=['complement'], orient='index')
        self.zip_code_df = zip_code_df

    def create_zap_items(self):
        """
        Create new zap items only for lthos listings ids that aren't already in the DB
        """
        for listing in self.listings:
            listing_id = listing.get('listing').get('id')
            self.append_listings_to_check(listing_id)
            if listing_id not in self.zap_search.existing_listing_ids_in_db:
                item = ZapItem(listing, self)
                self.add_zap_item(item)

    def append_listings_to_check(self, listing_id):
        """
        Append listing_id to other listing ids on the ZapSearch obejct
        Args:
            listing_id (str): ID of listing
        """
        self.listings_to_check.append(listing_id)

    def add_zap_item(self, zap_item):
        """
        Append ZapItem to other listings that will be added to the DB
        Args:
            zap_item:
        """
        self.zap_items_to_add.append(zap_item)

    def convert_zap_page_listing_to_df(self):
        """
        Convert all items ZapPage item into pandas Dataframe
        """
        items = self.zap_items_to_add
        page_listings = transform.convert_to_dataframe(items)
        self.zap_page_listings = page_listings


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
        self.description = self.get_listing_title()
        # Getting time related info
        self.listing_date = self.get_listing_date()
        self.updated_at = self.get_update_date()
        self.new_listing = self.is_new_listing()
        # Getting house attributes
        self.bedrooms = self.get_number_of_bedrooms()
        self.bathrooms = self.get_number_of_bathrooms()
        self.vacancies = self.get_number_of_parking_spaces()
        self.floor = self.get_floor_number()
        self.construction_year = self.get_construction_year()
        self.total_area_m2 = self.get_usable_area()
        self.business_type = self._zap_page.business_type
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
        # Getting cost data
        self.price = self.get_listing_price()
        self.condo_fee = self.get_condo_fee()
        self.price_per_area = self.calculate_price_per_area()
        self.latitude, self.longitude = self.get_longitude_latitude()
        # Getting reference data
        self.url = self.create_listing_url()
        self.link = self.create_html_link()
        # Advertizer info
        self.advertizer = self.get_advitizer_name()
        self.primary_phone = self.get_primary_phone()

    def is_good_deal(self):

        item_price_per_area = self.price_per_area
        neighborhood_good_price_threshold = self._zap_page.zap_search.neighborhood_good_deal_price
        if neighborhood_good_price_threshold:
            is_listing_good_deal = item_price_per_area < neighborhood_good_price_threshold
        else:
            is_listing_good_deal = True
        return is_listing_good_deal

    def get_primary_phone(self):
        """
        Get the primary phone number
        """
        return self._listing_data['account']['phones']['primary']

    def get_advitizer_name(self):
        """
        Get Advertizer's name'
        """
        return self._listing_data['account']['name']

    def create_html_link(self):
        """
        Create HTML link to listing
        """
        return f'<a href="{self.url}">{self.description}</a>'

    def create_listing_url(self):
        """
        Create listing URL
        """
        return 'https://www.zapimoveis.com.br' + self._listing_data['link']['href']

    def get_complete_address(self):
        """
        Create complete address for a listing
        """
        return ", ".join([self.street_address + " " + str(self.street_number), self.neighborhood,
                          self.zip_code, self.city, self.state, self.country])

    def get_street_address(self):
        """
        Get street name from listing
        """
        return self._listing_data['link']['data']['street']

    def get_zip_code(self):
        """
        Get zip code from listing
        """
        return self._listing_data['listing']['address']['zipCode']

    def get_neighborhood(self):
        """
        Get neighborhood from listing
        """
        return self._listing_data.get('listing').get('address').get('neighborhood')

    def get_city(self):
        """
        Get city from listing
        """
        return self._listing_data['listing']['address']['city']

    def get_state_acronym(self):
        """
        Get state acronym from listing
        """
        return self._listing_data['listing']['address']['stateAcronym']

    def get_listing_country(self):
        """
        Get country from listing
        """
        return self._listing_data['listing']['address']['country']

    def calculate_price_per_area(self):
        """
        Calculate price per area for a listing
        """
        return self.price / self.total_area_m2

    def get_usable_area(self):
        """
        Get first usable area for a listing
        """
        return int(self._listing_data['listing']['usableAreas'][0] if len(
            self._listing_data['listing']['usableAreas']) > 0 else 0)

    def get_floor_number(self):
        """
        Get floor number from listing
        """
        return self._listing_data.get('listing', {}).get('unitFloor', None)

    def get_number_of_parking_spaces(self):
        """
        Get number of parking slots
        """
        return self._listing_data['listing']['parkingSpaces'][0] if len(
            self._listing_data['listing']['parkingSpaces']) > 0 else 0

    def get_number_of_bathrooms(self):
        """
        Get number of bathrooms available for a listing
        """
        return int(
            self._listing_data['listing']['bathrooms'][0] if len(self._listing_data['listing']['bathrooms']) > 0 else 0)

    def get_number_of_bedrooms(self):
        """
        Get the number of bedrooms available for a listing
        """
        return int(
            self._listing_data['listing']['bedrooms'][0] if len(self._listing_data['listing']['bedrooms']) > 0 else 0)

    def get_condo_fee(self):
        """
        Get the condo fee for a listing
        """
        listing_data = self._listing_data
        return int(listing_data['listing']['pricingInfos'][0].get('monthlyCondoFee', 0)) if len(
            listing_data['listing']['pricingInfos']) > 0 else 0

    def get_listing_price(self):
        """
        Get the listing price for properties or sale or rent
        """
        business_type = self.business_type
        pricing_list = self._listing_data['listing']['pricingInfos']
        price = 0
        for pricing in pricing_list:
            if business_type in pricing.values() and business_type == 'SALE':
                price = int(pricing.get('price', None))
            elif business_type in pricing.values() and business_type == 'RENTAL':
                price = int(pricing.get('rentalInfo', {}).get(
                    'monthlyRentalTotalPrice', 0))
        return price

    def get_listing_title(self):
        """
        Get the listing title
        """
        return self._listing_data['listing']['title']

    def get_listing_date(self):
        """
        Get the date when the listing was created
        """
        return datetime.fromisoformat(self._listing_data['listing']['createdAt'].replace('Z', '+00:00')).date()

    def get_update_date(self):
        return datetime.date(pd.to_datetime(self._listing_data['listing']['updatedAt']))

    def get_listing_id(self):
        """
        Get the id of the listing
        """
        return self._listing_data.get('listing').get('id')

    def get_location_type(self):
        """
        Get the location type of the listing, e.g. "Rua" or "Avenida"

        """
        street_address_split = self.street_address.split()
        if street_address_split:
            return self.street_address.split()[0]
        return 'N/A'

    def get_street_number(self):
        """
        Get the street number of the listing, if it is not available guess one from the zip code
        """
        assigned_number = self._listing_data['link']['data']['streetNumber']
        if assigned_number != "":
            return assigned_number
        else:
            return self.get_random_street_number_from_zipcode()

    def get_construction_year(self):
        """
        Get the construction year of the listing, if it is not available fill with -1
        """
        deliver_date = self._listing_data.get('listing', {}).get('deliveredAt', 0)
        if deliver_date:
            delivery_year = int(deliver_date[:4])
        else:
            delivery_year = 0
        return delivery_year

    def get_random_street_number_from_zipcode(self):
        """
        Assign random street number to the property,if this data wasn't available yet
        """
        # Get ZIP Code for listing
        zip_code = self.zip_code
        # Get existing ZIP codes on database
        existing_zip_codes = self._zap_page.zap_search.existing_zip_codes
        # Initiate random number
        random_number = 1
        if zip_code not in ["", "00000000"]:
            try:
                # If ZIP code not available, download street complement from Brasil Aberto API
                if zip_code not in existing_zip_codes.index:
                    street_complement = self.download_street_complement(zip_code)
                    # And add it to the the database
                    self._zap_page.add_zip_code(zip_code, street_complement)
                # If available, then retrieve from database
                else:
                    street_complement = existing_zip_codes.loc[self.zip_code, 'complement']
                # Get street numbers contained on stree complement
                street_complement_numbers = [*map(int, re.findall(r"\d+", street_complement))]
                if street_complement_numbers:
                    # If there are two values, randomly assing one between max and min values
                    if max(street_complement_numbers) - min(street_complement_numbers) > 10:
                        num_max = int(max(street_complement_numbers))
                        num_min = int(min(street_complement_numbers))
                        random_number = str(round(random.uniform(num_min, num_max)))
                    # If has one or more values and the word "fim" on it, them we have the minimum possible street number                    
                    elif len(street_complement_numbers)>=1 and "fim" in street_complement:
                        num_min = int(min(street_complement_numbers))
                        random_number = str(round(random.uniform(num_min, num_min+100)))
                    # If has one or more values and the word "até" on it, them we have the maximum possible street number                    
                    elif len(street_complement_numbers)>=1 and "até" in street_complement:
                        num_max = int(max(street_complement_numbers))
                        random_number = str(round(random.uniform(1, num_max)))
                else:
                    # Creating a random street number
                    random_number = 1
            # If there is an error with connection or data type street number will be 1 
            except (ConnectionError, TypeError) as error:
                print(f"Found error: {error}")
                random_number = 1
        return random_number

    def download_street_complement(self, zip_code):
        """
        Get street complement from BrasilAberto.com API
        Args:
            zip_code (int): Zip code for location
        """
        response = r.get(f'https://api.brasilaberto.com/v1/zipcode/{zip_code}.json', headers={'Bearer':os.environ['BRASIL_ABERTO_API_KEY']})
        zip_data = response.json()
        street_complement = zip_data.get('result').get('complement', '')
        return street_complement

    def get_longitude_latitude(self):
        """
        Get latitude, if not available calculate it based on the street address
        """

        latitude = self._listing_data.get('listing', {}).get('displayAddressGeolocation', {}).get('lat', np.nan)
        longitude = self._listing_data.get('listing', {}).get('displayAddressGeolocation', {}).get('lon', np.nan)
        if not (np.isnan(latitude) or np.isnan(longitude)):
            self.precision = 'exact'
        # elif self.good_deal:
        #     locator = Nominatim(user_agent='zap_scraper')
        #     try:
        #         location = locator.geocode(self.address)
        #         latitude = location.latitude
        #         longitude = location.longitude
        #         self.precision = 'approximate'
        #     except:
        #         pass
        else:
            self.precision = 'approximate'
        return latitude, longitude

    def is_new_listing(self):
        """
        Check if listing is new or not
        """
        current_date = datetime.now().date()
        one_month_ago = current_date - timedelta(days=30)

        if self.listing_date >= one_month_ago:
            return True
        else:
            return False
