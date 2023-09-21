import zapimoveis_scraper as zap
from dotenv import load_dotenv
import os

load_dotenv()
mapbox_token = os.getenv('MAPBOX_TOKEN')

city = 'São Paulo'
state = 'São Paulo'
# neighborhoods = ['']
neighborhoods = ['Pinheiros']
# neighborhoods = ['Pinheiros', 'Vila Madalena', 'Bela Vista', 'Vila Mariana', 'Jardim Paulista', 'Jardins', 'Jardim Europa', 'Consolação',
#                  'Cerqueira César', 'Higienópolis', 'Itaim Bibi', 'Ibirapuera', 'Vila Nova Conceição', 'Vila Olímpia',
#                  'Sumaré', 'Perdizes', 'Pacaembu']
tipo_negocio = 'SALE'
usage_type = 'RESIDENTIAL'
unit_type = 'APARTMENT'
min_area = 100
max_price = 1200000
max_price_per_area = 6000
min_price_per_area = 3500

if zap.check_if_update_needed(test=True):
    search_results = zap.search(tipo_negocio, state, city, neighborhoods, usage_type, unit_type,
                                min_area, max_price, dataframe_out=True)
    search_results = zap.remove_fraudsters(search_results)
    zap.export_results_to_db(search_results)
    zap.convert_sqlite_to_csv()

