from zapimoveis_scraper.classes import ZapPage, ZapSearch
from dotenv import load_dotenv

load_dotenv()

city = 'São Paulo'
state = 'São Paulo'
business_type = 'SALE'
usage_type = 'RESIDENTIAL'
unit_type = 'APARTMENT'
min_area = 60
min_price = 100000
max_price = 2000000
# neighborhoods = ['Pinheiros', 'Vila Madalena']
neighborhoods = ['Pinheiros', 'Vila Madalena','Bela Vista', 'Vila Mariana', 'Jardim Paulista', 'Jardins',
                 'Jardim Europa', 'Consolação', 'Cerqueira César', 'Higienópolis', 'Itaim Bibi', 'Ibirapuera',
                 'Vila Nova Conceição', 'Vila Olímpia', 'Sumaré', 'Perdizes', 'Pacaembu']
def extract(business_type, city, max_price, min_area, min_price, neighborhood, state, unit_type, usage_type):
    page = 0
    print(f"Getting listings from neighborhood {neighborhood}")
    # Initialize a ZapSearch item that consists of searching a whole neighborhood
    zap_search = ZapSearch(neighborhood, business_type)
    # Get existing listing ids from a neighborhood
    zap_search.get_existing_ids()
    # Get existing zip codes from a neighborhood
    zap_search.get_existing_zip_codes()
    # Iterate through all pages on a neighborhood
    while True:
        print(f"Page #{page} on {neighborhood}")
        # Initialize a ZapPage object with data for each page searched
        zap_page = ZapPage(business_type, state, city, neighborhood, usage_type, unit_type, min_area, min_price,
                           max_price, page, zap_search)
        # Get response for API call on a page
        zap_page.get_page()
        # Get listings from a ZapPage
        listings = zap_page.get_listings()
        if not listings:
            break
        # Create ZapItem from all items in a page
        zap_page.create_zap_items()
        # Save items to ZapSearch item
        zap_search.save_zap_pages(zap_page)
        page += 1

    return zap_search
def transform(zap_search):
    # Convert output to standard format before saving

    zap_search.concat_zip_codes()
    zap_search.concat_listings()
    # Treating listings
    zap_search.remove_fraudsters()
    zap_search.remove_outliers()
    return zap_search

def save(zap_search):
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
