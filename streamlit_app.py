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
authentication.initialize_connected_as_guest_state()
# Display the login button if the user is not authenticated
authentication.create_login_modal(authenticator)

if st.session_state["connected"] or st.session_state['connected_as_guest']:
    @st.experimental_dialog(
        f"Bem-vindx {st.session_state.get('user_info', {}).get('name', 'Visitante').split(' ')[0]}!",
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
        # st.write(st.session_state['user_info']['email'], st.session_state['listings_clicked'])
        with st.expander("Ver resultados em tabela"):
            st.write(data)
    fragment()
    
    if st.button("Log out"):
        authenticator.logout()
else:
    st.write('You are not connected')
    authorization_url = authenticator.get_authorization_url()
    st.markdown(f'[Login]({authorization_url})')
    st.link_button('Login', authorization_url)

