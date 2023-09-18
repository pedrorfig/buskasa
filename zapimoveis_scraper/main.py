import zapimoveis_scraper as zap
from dotenv import load_dotenv
import os

load_dotenv()
mapbox_token = os.getenv('MAPBOX_TOKEN')

city = 'São Paulo'
state = 'São Paulo'
# neighborhoods = ['Pinheiros']
neighborhoods = ['Pinheiros', 'Vila Madalena', 'Bela Vista', 'Vila Mariana', 'Jardim Paulista', 'Jardins', 'Consolação',
                 'Cerqueira César', 'Higienópolis', 'Itaim Bibi', 'Ibirapuera', 'Vila Nova Conceição', 'Vila Olímpia',
                 'Sumaré']
tipo_negocio = 'SALE'
usage_type = 'RESIDENTIAL, RESIDENTIAL'
min_area = 100
max_price = 1200000
max_price_per_area = 6500
min_price_per_area = 3000

if zap.check_if_update_needed(test=True):
    search_results = zap.search(tipo_negocio, state, city, neighborhoods, usage_type, min_area, max_price,dataframe_out=True)
    search_results = zap.filter_results(search_results, max_price_per_area, min_price_per_area)
    search_results = zap.remove_fraudsters(search_results)
    zap.export_results(search_results)
zap.create_map(mapbox_token)
