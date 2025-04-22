# ETL pipeline to extract listings from ZAP Imóveis website,
# process them and save to the database
from src.classes import ZapPage, ZapNeighborhood, ZapItem
from dotenv import load_dotenv
from src import extract, transform
import logging
import random

# Configure logging
logging.basicConfig(format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)
# Create logger object
logger = logging.getLogger(__name__)

def main(
    usage_type: str = "RESIDENTIAL",
    min_area: int = 30,
    min_price: int = 200000,
    max_price: int = 2000000,
):
    """
    Perform a search on ZapImvoeis based on filters, scraping listings,
    processing them and saving the output to a database
    Args:
        business_type (str): Type of business, SALE or RENTAL
        max_price (int): Max price to scrape
        min_area (int): Min area to scrape
        min_price (int): Min listing price to scrape
        unit_type (str): Type of construction, APARTMENT or HOME
        usage_type (str): Type of usage, RESIDENTIAL

    """

    # Load credential values
    load_dotenv()
    # Get state, city and neighborhoods to be search through command prompt
    state, city, unit_type, unit_type_v3, unit_subtype, business_type, neighborhoods = extract.get_search_parameters()
    # Generate a random integer between 0 and 1000000 for session number
    session_number = random.randint(0, 1000000)
    if business_type == "RENTAL":
        max_price = int(max_price/100)
        min_price = int(min_price/100)

    for neighborhood in neighborhoods:
        logger.info(f"Getting listings from neighborhood {neighborhood}")
        # Start from page_number 0
        page_number = 0
        # Initialize a ZapSearch item that
        # consists of searching a whole neighborhood
        zap_neighborhood = ZapNeighborhood(
            state,
            city,
            neighborhood,
            unit_type,
            unit_type_v3,
            unit_subtype,
            business_type,
            max_price,
            min_price,
            min_area,
            session_number
        )
        # Delete listings that are not available
        zap_neighborhood.remove_old_listings()
        # Get existing listing ids from a neighborhood
        zap_neighborhood.get_existing_ids()
        # Get existing zip codes from a neighborhood
        zap_neighborhood.get_existing_zip_codes()
        # Get image analysis for a neighborhood
        zap_neighborhood.get_image_analysis()
        # Get traffic analysis for a neighborhood
        zap_neighborhood.get_traffic_analysis()
        # Iterate through all pages on a neighborhood
        while True:
            logger.info(f"\tGetting page {page_number} on {neighborhood}")
            # Initialize a ZapPage object with data for each page_number
            # searched
            zap_page = ZapPage(page_number, zap_neighborhood)
            # Get response for API call on a page_number
            zap_page.get_page()
            # Get all listings from a ZapPage
            zap_page.get_listings()
            # Create ZapItem object for each item in a page
            for listing in zap_page.listings:
                try:
                    item = ZapItem(listing, zap_page)
                    zap_page.add_zap_item(item)
                except Exception as e:
                    logger.error(e)
                    continue
            # Save items to ZapSearch
            zap_neighborhood.append_zap_page(zap_page)
            # If there number of listings reached the total, finish the search
            if zap_page.check_if_search_ended():
                break
            # Go to next page
            page_number += 1
        # Convert output to standard format before saving
        zap_neighborhood.concat_zip_codes()
        zap_neighborhood.concat_listings()
        # Treating listings
        zap_neighborhood.remove_fraudsters()
        zap_neighborhood.remove_outliers()
        zap_neighborhood.remove_duplicated_listings()
        # highlight good deals
        zap_neighborhood.calculate_price_per_area_first_quartile()
        # Save results to db
        zap_neighborhood.save_image_analysis_to_db()
        zap_neighborhood.save_traffic_analysis_to_db()
        zap_neighborhood.save_listings_to_db()
        zap_neighborhood.save_zip_codes_to_db()
        # Close engine
        zap_neighborhood.close_engine()
    transform.group_green_density(city)
    transform.group_n_bus_lanes(city)
    transform.flag_remodeled_properties()

if __name__ == "__main__":

    main()
