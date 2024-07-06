import streamlit as st
from src.app_classes import AppFormater, AppVisualizer
import src.visualization as visualization
from src.streamlit_google_auth import Authenticate
from dotenv import load_dotenv

load_dotenv()
# Initialize app visualization
app_visual = AppVisualizer()

# Initialize app formatation
app_formater = AppFormater()
app_formater.format_page()

# Create authentiation object
auth = Authenticate(
    secret_credentials_path="google_credentials.json",
    cookie_name="bargain_bungalow_cookie_name",
    cookie_key="bargain_bungalow_cookie_key"
)
auth.create_google_credentials_file()
# Check if the user is already authenticated on Google
auth.check_authentification()
auth.initialize_connected_as_guest_state()
# Display the login button if the user is not authenticated
auth.create_login_modal()

if st.session_state["connected"] or st.session_state["connected_as_guest"]:
    app_visual.write_welcome_message_modal_first_start()
    app_formater.remove_whitespace()
    app_formater.increase_logo_size()

    app_visual.create_side_bar_with_filters()
    app_visual.load_listings_map_and_table()

    if st.button("Log out"):
        auth.logout()
