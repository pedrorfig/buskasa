import streamlit as st
from src.app_classes import AppFormater, App
from dotenv import load_dotenv

load_dotenv()

# Initialize app formatation
app_formater = AppFormater()
app_formater.format_page()

# Initialize app visualization
app = App()

app.auth.create_google_credentials_file()
# Check if the user is already authenticated on Google
app.auth.check_authentification()
app.auth.initialize_connected_as_guest_state()
# Display the login button if the user is not authenticated
app.auth.create_login_modal()

if st.session_state["connected"] or st.session_state["connected_as_guest"]:
    app.write_welcome_message_modal_first_start()
    app_formater.remove_whitespace()
    app_formater.increase_logo_size()
    app.create_side_bar_with_filters()
    app.load_listings_map_and_table()
