from zapimoveis_scraper.classes import ZapPage, ZapSearch
from dotenv import load_dotenv

load_dotenv()

city = 'São Paulo'
state = 'São Paulo'
business_type = 'SALE'
usage_type = 'RESIDENTIAL'
unit_type = 'APARTMENT'
min_area = 100
min_price = 500000
max_price = 1200000
# neighborhoods = ['Pinheiros', 'Vila Madalena,]
neighborhoods = ['Pinheiros', 'Vila Madalena','Bela Vista', 'Vila Mariana', 'Jardim Paulista', 'Jardins',
                 'Jardim Europa', 'Consolação', 'Cerqueira César', 'Higienópolis', 'Itaim Bibi', 'Ibirapuera',
                 'Vila Nova Conceição', 'Vila Olímpia', 'Sumaré', 'Perdizes', 'Pacaembu']

def save(zap_search):
    # Save results to db
    zap_search.save_listings_to_db()
    zap_search.save_zip_codes_to_db()
    # Close engine
    zap_search.close_engine()


def transform(zap_search):
    # Convert output to standard format before saving

    zap_search.concat_zip_codes()
    zap_search.concat_listings()
    # Treating listings
    zap_search.remove_fraudsters()
    zap_search.remove_outliers()
    return zap_search


def extract(business_type, city, max_price, min_area, min_price, neighborhood, state, unit_type, usage_type):
    page = 0
    print(f"Getting listings from neighborhood {neighborhood}")
    zap_search = ZapSearch()
    while True:
        print(f"Page #{page} on {neighborhood}")
        zap_page = ZapPage(business_type, state, city, neighborhood, usage_type, unit_type, min_area, min_price,
                           max_price, page, zap_search)
        zap_page.get_page()
        listings = zap_page.get_listings()
        zap_search.get_existing_ids()
        zap_search.get_existing_zip_codes()
        if not listings:
            break
        zap_page.create_zap_items()
        zap_search.save_zap_pages(zap_page)
        page += 1

    return zap_search


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
        zap_page = transform(zap_search)
        save(zap_page)

if __name__ == '__main__':
    #  run EKD's algorithm to the list of given episodes
    search(business_type, state, city, neighborhoods, usage_type, unit_type, min_area, min_price, max_price)
