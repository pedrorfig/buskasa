import streamlit as st
from src.app_classes import AppFormater, App
from dotenv import load_dotenv

load_dotenv()

app_formater = AppFormater()
app_formater.format_page()
# Initialize app visualization
app = App()
app.create_login_modal()
# Initialize app formatation
st.toast(
    """Nós usamos cookies para recomendar
            os apartamentos que mais combinam com você""",
    icon=":material/cookie:",
)

if st.session_state.business_type is not None:
    with st.spinner("Carregando anúncios..."):
        app.get_listings()
        app.create_side_bar_with_filters()
        app.load_listings_map()
        app_formater.format_app_layout()
