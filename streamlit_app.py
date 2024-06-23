import os

import streamlit as st
from streamlit_google_auth import Authenticate
from dotenv import load_dotenv
import src.visualization as visualization

load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]

visualization.format_page()

authenticator = Authenticate(
    secret_credentials_path='google_credentials.json',
    cookie_name='my_cookie_name',
    cookie_key='this_is_secret',
    redirect_uri='http://localhost:8501',
    cookie_expiry_days=10
)

# Check if the user is already authenticated
authenticator.check_authentification()

# Display the login button if the user is not authenticated
authenticator.login()


if st.session_state['connected']:
    @st.experimental_dialog(f"Bem-vindo ao Bargain Bungalow {st.session_state.get('user_info',{}).get('name')}", width='small')
    def welcome_message():
        st.write("Bargain Bungalow usa AI para ajudar você a encontrar os melhores negócios imobiliários.")
    if "first_start" not in st.session_state:
        st.session_state.first_start = False
        welcome_message()

    visualization.remove_whitespace()
    visualization.increase_logo_size()

    (city_data, data) = visualization.create_side_bar_with_filters()
    @st.experimental_fragment
    def fragment():
        visualization.create_listings_map(mapbox_token, data, city_data)
        with st.expander("Ver resultados em tabela"):
            st.write(data)
    fragment()
    if st.button('Log out'):
        authenticator.logout()

