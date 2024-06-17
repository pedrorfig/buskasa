import os

import streamlit as st
from dotenv import load_dotenv

from zapimoveis_scraper import visualization

load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]


visualization.format_page()
@st.experimental_dialog("Bem-vindo ao Bargain Bungalow", width='small')
def welcome_message():
    st.write("Bargain Bungalow usa AI para ajudar você a encontrar os melhores negócios imobiliários.")
    st.write(st.experimental_user.email)
if "first_start" not in st.session_state:
    st.session_state.first_start = False
    welcome_message()

visualization.remove_whitespace()

visualization.increase_logo_size()


(city_data, data) = visualization.create_side_bar_with_filters()
@st.experimental_fragment
def fragment():
    visualization.create_listings_map(mapbox_token, data, city_data)
fragment()

if st.checkbox("Mostrar tabela"):
    st.write(data)
