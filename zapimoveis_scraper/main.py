import zapimoveis_scraper as zap
from dotenv import load_dotenv

load_dotenv()

city = 'São Paulo'
state = 'São Paulo'
tipo_negocio = 'SALE'
usage_type = 'RESIDENTIAL'
unit_type = 'APARTMENT'
min_area = 100
min_price = 1000000
max_price = 1500000
# neighborhoods = ['Pinheiros', 'Vila Madalena,]
neighborhoods = ['Bela Vista', 'Vila Mariana', 'Jardim Paulista', 'Jardins','Jardim Europa', 'Consolação',
                 'Cerqueira César', 'Higienópolis', 'Itaim Bibi', 'Ibirapuera', 'Vila Nova Conceição', 'Vila Olímpia',
                 'Sumaré', 'Perdizes', 'Pacaembu']

search_results = zap.search(tipo_negocio, state, city, neighborhoods, usage_type, unit_type, min_area, min_price, max_price)
