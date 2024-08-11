import os
import random
import re
from datetime import datetime, timedelta

import backoff
import cloudscraper
import numpy as np
import pandas as pd
import requests as r
from sqlalchemy import text

import src.extract as extract
import src.transform as transform


class ZapNeighborhood:
    """
    Object containing attributes for a search query on a complete neighborhood
    """

    def __init__(
        self,
        state: str,
        city: str,
        neighborhood: str,
        unit_type: str,
        usage_type: str,
        business_type: str,
        max_price: int,
        min_price: int,
        min_area: int,
    ):
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
        self.number_of_listings_per_page = 100

        # Placeholder to save results from pages
        self.zap_pages = []
        # Save all listings from search to check if any was deleted on the web,
        # so it is also deleted on the DB
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
        with engine.begin() as conn:
            # Checking for existing listing_ids on the database
            # according to specified filters
            filter_conditions = {
                "neighborhood": self.neighborhood,
                "city": self.city,
                "business_type": self.business_type,
                "min_area": self.min_area,
                "min_price": self.min_price,
                "max_price": self.max_price,
            }
            listing_id_sql_statement = """
                SELECT listing_id
                from fact_listings
                where
                    city= %(city)s
                    and neighborhood = %(neighborhood)s
                    and business_type = %(business_type)s
                    and total_area_m2 >= %(min_area)s
                    and price between %(min_price)s and %(max_price)s
                """
            ids = pd.read_sql(
                listing_id_sql_statement, con=conn, params=filter_conditions
            )
            ids_list = [*ids["listing_id"]]
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
        listings = listings.drop_duplicates(subset="listing_id")
        self.listings_to_add = listings

    def append_zap_page(self, zap_page):
        """
        Append ZapPage object to other ZapPage objects in a ZapSearch
        Args:
            zap_page:
        """
        self.zap_pages.append(zap_page)

    def remove_fraudsters(self):
        """
        Remove possible fraudsters from house listings
        Args:
            search_results:

        Returns:

        """
        print("\tRemoving fraudsters")
        listings = self.listings_to_add
        # Listing users known to be fraudsters
        known_fraudsters = [
            "Camila Damaceno Bispo",
            "Lucas Antônio",
            "Imóveis São Caetano",
            "São Caetano Imóveis",
            "Alex Matheus  Moura",
            "Claudia Cristina Ribeiro de Almeida",
        ]
        listings_from_known_fraudsters = pd.Series()
        listings_from_recent_accounts = pd.Series()
        listings_with_total_area_typos = pd.Series()
        if not listings.empty:
            # Known fraudster
            listings_from_known_fraudsters = listings[
                listings["advertizer"].isin(known_fraudsters)
            ]["listing_id"]
            # Fraudsters by accounts that are too recent
            listings_from_recent_accounts = listings[listings["recent_account"]][
                "listing_id"
            ]
            # Total area typos
            listings_with_total_area_typos = listings[listings["total_area_m2"] >= 500][
                "listing_id"
            ]
            # Populating series of listings to be removed
            listing_ids_to_remove = pd.concat(
                [
                    listings_from_known_fraudsters,
                    listings_from_recent_accounts,
                    listings_with_total_area_typos,
                ]
            )
            self.listing_ids_to_remove = listing_ids_to_remove.to_list()
            # Remove listings
            cleaned_listings = listings.loc[
                ~listings["listing_id"].isin(listing_ids_to_remove), :
            ]

            self.listings_to_add = cleaned_listings

    def calculate_price_per_area_first_quartile(self):
        engine = self._engine
        with engine.begin() as conn:
            filter_conditions = {
                "neighborhood": self.neighborhood,
                "city": self.city,
                "business_type": self.business_type,
                "min_area": self.min_area,
                "min_price": self.min_price,
                "max_price": self.max_price,
            }
            listingd_on_db_sql_statement = """SELECT *
                        from fact_listings
                    WHERE
                        city = %(city)s and
                        neighborhood = %(neighborhood)s and
                        business_type = %(business_type)s
                    """
            listings_on_db = pd.read_sql(
                listingd_on_db_sql_statement, con=conn, params=filter_conditions
            )
        search_listings = self.listings_to_add
        if (not search_listings.empty) and (not listings_on_db.empty):
            all_listings = pd.concat([search_listings, listings_on_db])
        elif not search_listings.empty:
            all_listings = search_listings
        elif not listings_on_db.empty:
            all_listings = listings_on_db
        else:
            return "No listings to add"
        # Calculate first quartile on price per area
        q_low = all_listings["price_per_area"].quantile(0.25)
        self.neighborhood_price_per_area_first_quartile = q_low
        if not search_listings.empty:
            self.listings_to_add["price_per_area_in_first_quartile"] = (
                search_listings["price_per_area"] <= q_low
            )

    def remove_outliers(self):
        """
        Removing outlier on assigned feature

        Args:
            feature:
            data_with_outliers:
        """
        # TODO: Change outlier calculation for boxplot
        print("\tRemoving outliers on listing prices")
        engine = self._engine
        with engine.begin() as conn:
            filter_conditions = {
                "neighborhood": self.neighborhood,
                "city": self.city,
                "business_type": self.business_type,
                "min_area": self.min_area,
                "min_price": self.min_price,
                "max_price": self.max_price,
            }
            listings_on_db_sql_statement = """
                SELECT *
                    from fact_listings
                WHERE
                    city = %(city)s and
                    neighborhood = %(neighborhood)s and
                    business_type = %(business_type)s
                """
            listings_on_db = pd.read_sql(
                listings_on_db_sql_statement, con=conn, params=filter_conditions
            )
        search_listings = self.listings_to_add
        if (not search_listings.empty) and (not listings_on_db.empty):
            print((not search_listings.empty),(not listings_on_db.empty))
            all_listings = pd.concat([listings_on_db, search_listings])
            # Calculate interquartile range
            q_low = all_listings["price_per_area"].quantile(0.25)
            q_hi = all_listings["price_per_area"].quantile(0.75)
            inter_q_range = q_hi - q_low
            # Calculate outlier thresholds to remove anything above
            # or below limits assigned
            threshold_high = q_hi + inter_q_range * 1.5
            threshold_low = q_low - inter_q_range * 1.5
            outlier_listings = search_listings[
                (search_listings["price_per_area"] > threshold_high)
                & (search_listings["price_per_area"] < threshold_low)
            ]["listing_id"]
            # Removing price_per_area outlier listings
            cleaned_listings = search_listings.loc[
                ~search_listings["listing_id"].isin(outlier_listings), :
            ]
            self.listings_to_add = cleaned_listings
            # Saving listings removed
            listing_ids_to_remove = self.listing_ids_to_remove
            listing_ids_to_remove.extend(outlier_listings.to_list())
        else:
            self.listings_to_add = search_listings
        return "Outliers removed"

    def remove_duplicated_listings(self):
        """
        Remove listings that are already on the DB
        """
        print("\tRemoving duplicated listings")
        listings = self.listings_to_add
        deduplucated_listings = listings.sort_values(
            "price", ascending=True
        ).drop_duplicates(
            subset=[
                "bedrooms",
                "bathrooms",
                "floor",
                "total_area_m2",
                "zip_code",
                "street_address",
                "street_number",
            ],
            keep="first",
        )
        self.listings_to_add = deduplucated_listings

    def remove_listings_deleted(self):
        """
        Remove listings that haven't been updated for more than a week
        """
        print("\tRemoving deleted listings")
        engine = self._engine
        with engine.begin() as conn:
            old_listings = pd.read_sql(
                """SELECT listing_id
                        FROM fact_listings
                        WHERE
                            updated_at < current_date - 1
                            and neighborhood = %(neighborhood)s
                    """,
                con=conn,
                params={"neighborhood": self.neighborhood},
            )
        if not old_listings.empty:
            old_listings = old_listings.squeeze()
            if isinstance(old_listings, str):
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
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
            "Authorization": "Bearer undefined",
            "Connection": "keep-alive",
            "DNT": "1",
            "Origin": "https://www.zapimoveis.com.br",
            "Referer": "https://www.zapimoveis.com.br/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
            # "X-DeviceId": "0d645541-36ea-45b4-9c59-deb2d736595c",
            "sec-ch-ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "x-domain": ".zapimoveis.com.br",
        }

        return headers

    def save_zip_codes_to_db(self):
        """ """
        print("\tSaving ZIP codes to database")
        zip_to_add = self.zip_codes_to_add
        # Only new zip codes will be added
        zip_to_add = zip_to_add[~zip_to_add.index.isin(self.existing_zip_codes.index)]
        if not zip_to_add.empty:
            with self._engine.begin() as conn:
                zip_to_add.to_sql(
                    name="dim_zip_code",
                    con=conn,
                    if_exists="append",
                    index=True,
                    index_label="zip_code",
                )

    def save_listings_to_db(self):
        """ """
        print("\tSaving records to database")
        if not self.listings_to_add.empty:
            engine = self._engine
            with engine.begin() as conn:
                # Set listing_id as index
                listings_to_add = self.listings_to_add.set_index("listing_id")
                # Delete all listings from db that are in the new search
                conn.execute(
                    text(
                        """
                            DELETE FROM fact_listings
                            WHERE
                                listing_id in :listing_ids
                        """
                    ),
                    {"listing_ids": tuple(listings_to_add.index)},
                )
                # Save new listings from the neighborhood search
                listings_to_add.to_sql(
                    name="fact_listings",
                    con=conn,
                    if_exists="append",
                    index=True,
                    index_label="listing_id",
                )

    def get_existing_zip_codes(self):
        """
        Read db table
        Returns:

        """
        engine = self._engine
        with engine.begin() as conn:
            data = pd.read_sql(
                "SELECT * from dim_zip_code", con=conn, index_col="zip_code"
            )
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
        self.listings = []

    @backoff.on_exception(backoff.expo, r.exceptions.RequestException, max_tries=8)
    def get_page(self):
        """
        Get results from a house search at Zap Imoveis

        Returns:

        """
        number_of_listings_per_page = self.zap_search.number_of_listings_per_page
        initial_listing = number_of_listings_per_page * self.page_number
        headers = self.zap_search.get_request_headers()
        params = {
            "user": "0d645541-36ea-45b4-9c59-deb2d736595c",
            "portal": "ZAP",
            "categoryPage": "RESULT",
            "developmentsSize": "0",
            "superPremiumSize": "0",
            "business": self.business_type,
            "parentId": "null",
            "listingType": "USED",
            "unitTypesV3": self.unit_type,
            "unitTypes": self.unit_type,
            "usableAreasMin": self.min_area,
            "priceMax": self.max_price,
            "priceMin": self.min_price,
            "addressCity": self.city,
            "addressState": self.state,
            "addressNeighborhood": self.neighborhood,
            "page": "1",
            "from": initial_listing,
            "size": number_of_listings_per_page,
            "usageTypes": self.usage_type,
            "levels": "NEIGHBORHOOD",
        }
        scraper = cloudscraper.create_scraper()
        response = scraper.get(
            "https://glue-api.zapimoveis.com.br/v2/listings",
            params=params,
            headers=headers,
        )
        page_data = response.json()
        self.page_data = page_data

    def get_listings(self):
        """
        Get only listings from a ZapImoveis page API response

        """

        listings = (
            self.page_data.get("search", {}).get("result", {}).get("listings", None)
        )
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
        """Check if the page has less listings than the maximum allowed"""
        number_of_page_listings_less_than_maximum = (
            len(self.listings) < self.zap_search.number_of_listings_per_page
        )
        return number_of_page_listings_less_than_maximum

    def convert_zap_page_zip_code_to_df(self):
        """
        Convert zip code dict to df
        """
        zip_code_df = pd.DataFrame.from_dict(
            self.zip_code_to_add, columns=["complement"], orient="index"
        )
        self.zip_code_df = zip_code_df

    def create_zap_items(self):
        """
        Create new zap items only for lthos listings ids that aren't already
        in the DB
        """
        for listing in self.listings:
            listing_id = listing.get("listing").get("id")
            if listing_id not in self.zap_search.existing_listing_ids_in_db:
                item = ZapItem(listing, self)
                self.add_zap_item(item)

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
        self._db_engine = zap_page.zap_search._engine
        # Getting listing data
        self._listing_data = listing
        self.listing_id = self.get_listing_id()
        self.description = self.get_listing_title()
        # Getting time related info
        self.listing_date = self.get_listing_date()
        self.updated_at = self.get_update_date()
        self.new_listing = self.is_new_listing()
        # Getting house attributes
        self.unit_type = self.get_unit_type()
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
        self.latitude, self.longitude = self.get_longitude_latitude()
        self.precision = None
        self.green_density = self.get_green_density()
        # Getting cost data
        self.price = self.get_listing_price()
        self.condo_fee = self.get_condo_fee()
        self.price_per_area = self.calculate_price_per_area()
        # Getting reference data
        self.url = self.create_listing_url()
        self.link = self.create_html_link()
        # Advertizer info
        self.advertizer = self.get_advitizer_name()
        self.primary_phone = self.get_primary_phone()
        self.recent_account = self.is_recent_account()

    def get_green_density(self):
        with self._db_engine.begin() as conn:
            query = text(
                """
                SELECT green_density
                FROM fact_image_analysis
                WHERE min_lat <= :latitude AND :latitude <= max_lat
                AND min_lon <= :longitude AND :longitude <= max_lon
            """
            )
            result = conn.execute(
                query,
                parameters={"latitude": round(self.latitude,3), "longitude": round(self.longitude,3)},
            )
            green_densities = result.first()
        if green_densities is None:
            print(f"Point is not in any bounding box: {self.latitude}, {self.longitude}")
            min_lat, max_lat, min_lon, max_lon = transform.define_bounding_box(
                self.latitude, self.longitude
            )
            image = extract.get_sat_image(min_lat, max_lat, min_lon, max_lon)
            green_density = transform.calculate_green_density(image)
            extract.add_green_density_to_db(
                self._db_engine, min_lat, max_lat, min_lon, max_lon, green_density
            )
        else:
            green_density = green_densities[0]
        return green_density

    def is_recent_account(self):
        """
        Check if account is more recent than 30 days
        """
        account_date_str = self._listing_data.get("account", {}).get(
            "createdDate", None
        )
        if account_date_str is None:
            return True
        # if account more recent than 30 days
        account_date = datetime.strptime(account_date_str, "%Y-%m-%dT%H:%M:%SZ")
        # Get the current date
        current_date = datetime.utcnow()
        # Calculate the date 30 days ago
        date_30_days_ago = current_date - timedelta(days=30)
        # Check if the account date is more recent than 30 days ago
        return account_date > date_30_days_ago

    def get_primary_phone(self):
        """
        Get the primary phone number
        """
        return self._listing_data["account"]["phones"]["primary"]

    def get_advitizer_name(self):
        """
        Get Advertizer's name'
        """
        return self._listing_data["account"]["name"]

    def create_html_link(self):
        """
        Create HTML link to listing

        """
        return f'<a href="{self.url}">{transform.wrap_string_with_fill(self.description, 50)}</a>'

    def create_listing_url(self):
        """
        Create listing URL
        """
        return "https://www.zapimoveis.com.br" + self._listing_data["link"]["href"]

    def get_complete_address(self):
        """
        Create complete address for a listing
        """
        return ", ".join(
            [
                self.street_address + " " + str(self.street_number),
                self.neighborhood,
                self.zip_code,
                self.city,
                self.state,
                self.country,
            ]
        )

    def get_street_address(self):
        """
        Get street name from listing
        """
        return self._listing_data["link"]["data"]["street"]

    def get_zip_code(self):
        """
        Get zip code from listing
        """
        return self._listing_data["listing"]["address"]["zipCode"]

    def get_neighborhood(self):
        """
        Get neighborhood from listing
        """
        return self._listing_data.get("listing").get("address").get("neighborhood")

    def get_city(self):
        """
        Get city from listing
        """
        return self._listing_data["listing"]["address"]["city"]

    def get_state_acronym(self):
        """
        Get state acronym from listing
        """
        return self._listing_data["listing"]["address"]["stateAcronym"]

    def get_listing_country(self):
        """
        Get country from listing
        """
        return self._listing_data["listing"]["address"]["country"]

    def calculate_price_per_area(self):
        """
        Calculate price per area for a listing
        """
        return self.price / self.total_area_m2

    def get_usable_area(self):
        """
        Get first usable area for a listing
        """
        return int(
            self._listing_data["listing"]["usableAreas"][0]
            if len(self._listing_data["listing"]["usableAreas"]) > 0
            else 0
        )

    def get_floor_number(self):
        """
        Get floor number from listing
        """
        return self._listing_data.get("listing", {}).get("unitFloor", None)

    def get_number_of_parking_spaces(self):
        """
        Get number of parking slots
        """
        return (
            self._listing_data["listing"]["parkingSpaces"][0]
            if len(self._listing_data["listing"]["parkingSpaces"]) > 0
            else 0
        )

    def get_number_of_bathrooms(self):
        """
        Get number of bathrooms available for a listing
        """
        return int(
            self._listing_data["listing"]["bathrooms"][0]
            if len(self._listing_data["listing"]["bathrooms"]) > 0
            else 0
        )

    def get_number_of_bedrooms(self):
        """
        Get the number of bedrooms available for a listing
        """
        return int(
            self._listing_data["listing"]["bedrooms"][0]
            if len(self._listing_data["listing"]["bedrooms"]) > 0
            else 0
        )

    def get_unit_type(self):
        """
        Get the number of bedrooms available for a listing
        """
        return self._listing_data["listing"]["unitTypes"][0]

    def get_condo_fee(self):
        """
        Get the condo fee for a listing
        """
        listing_data = self._listing_data
        return (
            int(listing_data["listing"]["pricingInfos"][0].get("monthlyCondoFee", 0))
            if len(listing_data["listing"]["pricingInfos"]) > 0
            else 0
        )

    def get_listing_price(self):
        """
        Get the listing price for properties or sale or rent
        """
        business_type = self.business_type
        pricing_list = self._listing_data["listing"]["pricingInfos"]
        price = 0
        for pricing in pricing_list:
            if business_type in pricing.values() and business_type == "SALE":
                price = int(pricing.get("price", 0))
            elif business_type in pricing.values() and business_type == "RENTAL":
                price = int(
                    pricing.get("rentalInfo", {}).get("monthlyRentalTotalPrice", 0)
                )
        return price

    def get_listing_title(self):
        """
        Get the listing title
        """
        return self._listing_data["listing"]["title"]

    def get_listing_date(self):
        """
        Get the date when the listing was created
        """
        return datetime.fromisoformat(
            self._listing_data["listing"]["createdAt"].replace("Z", "+00:00")
        ).date()

    def get_update_date(self):
        return datetime.date(pd.to_datetime(self._listing_data["listing"]["updatedAt"]))

    def get_listing_id(self):
        """
        Get the id of the listing
        """
        return self._listing_data.get("listing").get("sourceId")

    def get_location_type(self):
        """
        Get the location type of the listing, e.g. "Rua" or "Avenida"

        """
        street_address_split = self.street_address.split()
        if street_address_split:
            return self.street_address.split()[0]
        return "N/A"

    def get_street_number(self):
        """
        Get the street number of the listing, if it is not available guess one from the zip code
        """
        assigned_number = self._listing_data["link"]["data"]["streetNumber"]
        # If assigned number looks like a number, return it
        if assigned_number.isnumeric():
            return int(assigned_number)
        # if it is not empty, but not a number, return 13
        elif assigned_number:
            return 13
        else:
            return self.get_random_street_number_from_zipcode()

    def get_construction_year(self):
        """
        Get the construction year of the listing, if it is not available fill with -1
        """
        deliver_date = self._listing_data.get("listing", {}).get("deliveredAt", 0)
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
                    street_complement = existing_zip_codes.loc[
                        self.zip_code, "complement"
                    ]
                # Get street numbers contained on stree complement
                street_complement_numbers = [
                    *map(int, re.findall(r"\d+", street_complement))
                ]
                if street_complement_numbers:
                    # If there are two values,
                    # randomly assing one between max and min values
                    if (
                        max(street_complement_numbers) - min(street_complement_numbers)
                    ) > 10:
                        num_max = int(max(street_complement_numbers))
                        num_min = int(min(street_complement_numbers))
                        random_number = round(random.uniform(num_min, num_max))
                    # If has one or more values and the word "fim" on it,
                    # them we have the minimum possible street number
                    elif (
                        len(street_complement_numbers) >= 1
                        and "fim" in street_complement
                    ):
                        num_min = int(min(street_complement_numbers))
                        random_number = round(random.uniform(num_min, num_min + 100))
                    # If has one or more values and the word "até" on it,
                    # them we have the maximum possible street number
                    elif (
                        len(street_complement_numbers) >= 1
                        and "até" in street_complement
                    ):
                        num_max = int(max(street_complement_numbers))
                        random_number = round(random.uniform(1, num_max))
                else:
                    # Creating a random street number
                    random_number = 1
            # If there is an error with connection
            # or data type street number will be 1
            except (ConnectionError, TypeError) as error:
                print(f"Found error: {error}")
                random_number = 13
        return random_number

    @backoff.on_exception(backoff.expo, r.exceptions.RequestException, max_tries=10)
    def download_street_complement(self, zip_code):
        """
        Get street complement from BrasilAberto.com API
        Args:
            zip_code (int): Zip code for location
        """
        response = r.get(
            f"https://api.brasilaberto.com/v1/zipcode/{zip_code}.json",
            headers={"Bearer": os.environ["BRASIL_ABERTO_API_KEY_FREE"]},
        )
        zip_data = response.json()
        street_complement = zip_data.get("result").get("complement", "")
        return street_complement

    def get_longitude_latitude(self):
        """
        Get latitude, if not available calculate it based on the street address
        """

        latitude = (
            self._listing_data.get("listing", {})
            .get("displayAddressGeolocation", {})
            .get("lat", np.nan)
        )
        longitude = (
            self._listing_data.get("listing", {})
            .get("displayAddressGeolocation", {})
            .get("lon", np.nan)
        )
        if not (np.isnan(latitude) or np.isnan(longitude)):
            self.precision = "exact"
            latitude = latitude + np.random.random() / 1000
            longitude = longitude + np.random.random() / 1000
        else:
            self.precision = "approximate"
        return latitude, longitude

    def is_new_listing(self):
        """
        Check if listing is new or not
        """
        current_date = datetime.now().date()
        week_old = current_date - timedelta(days=7)

        if self.listing_date >= week_old:
            return True
        else:
            return False
