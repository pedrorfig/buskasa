import os

import streamlit as st
import src.authentication as authentication
import src.visualization as visualization
from dotenv import load_dotenv

load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]

visualization.format_page()

# Create google authenticator object
authenticator = authentication.get_authenticator()
# Check if the user is already authenticated
authenticator.check_authentification()
# Display the login button if the user is not authenticated
authenticator.login()


if st.session_state["connected"]:

    @st.experimental_dialog(
        f"Bem-vindx {st.session_state.get('user_info', {}).get('name', '').split(' ')[0]}!",
        width="small",
    )
    def welcome_message():
        st.write(
            """Bargain Bungalow usa AI para ajudar você a encontrar
            os melhores negócios imobiliários."""
        )

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
    
    if st.button("Log out"):
        authenticator.logout()
