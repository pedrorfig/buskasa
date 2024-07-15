import streamlit as st
from streamlit_navigation_bar import st_navbar
import os
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
    app.get_user_data()
    app.check_if_user_has_visits()
    with st.spinner("Carregando anúncios..."):
        app.get_listings()
    app.get_listings_visited_by_user()
    app_formater.remove_whitespace()
    app_formater.increase_logo_size()
    # parent_dir = os.path.dirname(os.path.abspath(__file__))
    # logo_path = os.path.join(parent_dir, "assets", "bargain_bungalow.svg")
    # # styles = {
    # #     "nav": {
    # #         "height": "8rem",
    # #     }
    # # }
    # st_navbar(
    #     ["Sobre"],
    #     options={
    #         "show_menu": False,
    #         "use_padding": False,
    #     },
    #     # styles=styles,
    #     # logo_path=logo_path,
    # )
    app.create_side_bar_with_filters()
    app.load_listings_map_and_table()
    st.toast(
        """Nós usamos cookies para recomendar
             os apartamentos que mais combinam com você""",
        icon=":material/cookie:",
    )
