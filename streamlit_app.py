import streamlit as st
import src.visualization as visualization
from src.streamlit_google_auth import Authenticate
from dotenv import load_dotenv

load_dotenv()

visualization.format_page()

# Create google authenticator object
auth = Authenticate(
    secret_credentials_path="google_credentials.json",
    cookie_name="bargain_bungalow_cookie_name",
    cookie_key="bargain_bungalow_cookie_key"
)
auth.create_google_credentials_file()
# Check if the user is already authenticated
auth.check_authentification()
auth.initialize_connected_as_guest_state()
# Display the login button if the user is not authenticated
auth.create_login_modal()

if st.session_state["connected"] or st.session_state["connected_as_guest"]:

    visualization.write_welcome_message_modal()
    visualization.remove_whitespace()
    visualization.increase_logo_size()

    (city_data, data) = visualization.create_side_bar_with_filters()

    visualization.load_listings_map_and_table(data=data, city_data=city_data)

    if st.button("Log out"):
        auth.logout()
