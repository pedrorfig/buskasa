
# ETL pipeline to extract listings from ZAP Imóveis website, process them and save to the database
from zapimoveis_scraper.classes import ZapPage, ZapSearch
from dotenv import load_dotenv

# Load credential values
load_dotenv()

# Define which variables will be used for the search
city = 'São Paulo'
state = 'São Paulo'
business_type = 'SALE'
usage_type = 'RESIDENTIAL'
unit_type = 'APARTMENT,HOME'
min_area = 80
min_price = 600000
max_price = 1500000

neighborhoods = ['Bela Vista', 'Consolação','Consolação', 'Cerqueira César', 'Higienópolis',
                 'Vila Mariana', 'Jardim Paulista', 'Ibirapuera', 'Jardins', 'Itaim Bibi',
                 'Pinheiros', 'Paraíso',  'Vila Olímpia', 'Sumaré', 'Sumarezinho',  'Perdizes',
                 'Pacaembu', 'Vila Madalena', 'Moema','Jardim Europa', 'Vila Nova Conceição']
# neighborhoods = ['Bela Vista']

def extract(business_type, city, max_price, min_area, min_price, neighborhood, state, unit_type, usage_type):
    """
    Perform web-scrapping from Zapimoveis

    Args:
        business_type:
        city:
        max_price:
        min_area:
        min_price:
        neighborhood:
        state:
        unit_type:
        usage_type:

    Returns:

    """
    print(f"Getting listings from neighborhood {neighborhood}")
    # Start from page_number 0
    page_number = 0
    # Initialize a ZapSearch item that consists of searching a whole neighborhood
    zap_search = ZapSearch(state, city, neighborhood, unit_type, usage_type, business_type, max_price, min_area, min_price)
    # Delete listings that are not available
    zap_search.remove_listings_deleted()
    # Get existing listing ids from a neighborhood
    zap_search.get_existing_ids()
    # Get existing zip codes from a neighborhood
    zap_search.get_existing_zip_codes()
    # Iterate through all pages on a neighborhood
    while True:
        print(f"Page #{page_number} on {neighborhood}")
        # Initialize a ZapPage object with data for each page_number searched
        zap_page = ZapPage(page_number, zap_search)
        # Get response for API call on a page_number
        zap_page.get_page()
        # Get all listings from a ZapPage
        zap_page.get_listings()
        # If there number of listings reached the total, finish the search
        if zap_page.check_if_search_ended():
            break
        # Create ZapItem object for each item in a page
        zap_page.create_zap_items()
        # Save items to ZapSearch object
        zap_search.append_zap_pages(zap_page)
        # Save list of all listings scrapped
        zap_search.save_listings_to_check(zap_page.listings_to_check)
        # Go to next page
        page_number += 1

    return zap_search


def transform(zap_search):
    """

    Args:
        zap_search:

    Returns:

    """
    # Convert output to standard format before saving
    zap_search.concat_zip_codes()
    zap_search.concat_listings()
    # Treating listings
    zap_search.remove_fraudsters()
    zap_search.remove_outliers()
    # highlight good deals
    zap_search.calculate_price_per_area_first_quartile()

    return zap_search

def save(zap_search):
    """

    Args:
        zap_search:
    """
    # Save results to db
    zap_search.save_listings_to_db()
    zap_search.save_zip_codes_to_db()
    # Close engine
    zap_search.close_engine()

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
        zap_search = extract(business_type, city, max_price, min_area, min_price, neighborhood, state, unit_type,
                           usage_type)
        zap_search = transform(zap_search)
        save(zap_search)

if __name__ == '__main__':
    #  run EKD's algorithm to the list of given episodes
    search(business_type, state, city, neighborhoods, usage_type, unit_type, min_area, min_price, max_price)
