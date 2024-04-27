import os

import streamlit as st
from dotenv import load_dotenv

from zapimoveis_scraper import visualization

load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]

visualization.format_page()

visualization.remove_whitespace()


st.header("Bargain Bungalow")
st.markdown("Helps you find the best real estate deals in São Paulo")


(city_data, data) = visualization.create_side_bar_with_filters()

visualization.create_listings_map(mapbox_token, data, city_data)

if st.checkbox("Show raw data"):
    st.write(data)
