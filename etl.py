# ETL pipeline to extract listings from ZAP Imóveis website,
# process them and save to the database
from src.classes import ZapPage, ZapNeighborhood
from dotenv import load_dotenv
from src import extract
def main(
    business_type: str = "SALE",
    usage_type: str = "RESIDENTIAL,RESIDENTIAL",
    unit_type: str = "APARTMENT,HOME",
    min_area: int = 30,
    min_price: int = 200000,
    max_price: int = 2000000,
):
    """
    Perform a search on ZapImvoeis based on filters, scraping listings,
    processing them and saving the output to a database
    Args:
        business_type (str): Type of business, SALE or RENT
        max_price (int): Max price to scrape
        min_area (int): Min area to scrape
        min_price (int): Min listing price to scrape
        unit_type (str): Type of construction, APARTMENT or HOME
        usage_type (str): Type of usage, RESIDENTIAL

    """

    # Load credential values
    load_dotenv()
    # Get state, city and neighborhoods to be search through command prompt
    state, city, neighborhoods = extract.get_search_parameters()

    for neighborhood in neighborhoods:
        print(f"Getting listings from neighborhood {neighborhood}")
        # Start from page_number 0
        page_number = 0
        # Initialize a ZapSearch item that
        # consists of searching a whole neighborhood
        zap_neighborhood = ZapNeighborhood(
            state,
            city,
            neighborhood,
            unit_type,
            usage_type,
            business_type,
            max_price,
            min_price,
            min_area,
        )
        # Delete listings that are not available
        zap_neighborhood.remove_listings_deleted()
        # Get existing listing ids from a neighborhood
        zap_neighborhood.get_existing_ids()
        # Get existing zip codes from a neighborhood
        zap_neighborhood.get_existing_zip_codes()
        # Get image analysis for a neighborhood
        zap_neighborhood.get_image_analysis()
        # Iterate through all pages on a neighborhood
        while True:
            print(f"\tPage #{page_number} on {neighborhood}")
            # Initialize a ZapPage object with data for each page_number
            # searched
            zap_page = ZapPage(page_number, zap_neighborhood)
            # Get response for API call on a page_number
            zap_page.get_page()
            # Get all listings from a ZapPage
            zap_page.get_listings()
            # Create ZapItem object for each item in a page
            zap_page.create_zap_items()
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
        zap_neighborhood.save_listings_to_db()
        zap_neighborhood.save_zip_codes_to_db()
        # Close engine
        zap_neighborhood.close_engine()


if __name__ == "__main__":

    main()
