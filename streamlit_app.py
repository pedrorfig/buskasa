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
    
    visualization.write_welcome_message_modal()
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
# else:
#     st.write('You are not connected')
#     authorization_url = authenticator.get_authorization_url()
#     st.link_button('Login', authorization_url)