import streamlit as st
import numpy as np
import plotly.graph_objects as go
from dotenv import load_dotenv
from etl_modules import extract, transform
import datetime
import os


load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]


@st.cache_data
def load_data():
    results = extract.read_listings_sql_table()
    return results


# Create a text element and let the reader know the data is loading.
data_load_state = st.text("Loading data...")
# Load 10,000 rows of data into the dataframe.
data = load_data()
raw_data = data.copy()
# Notify the reader that the data was successfully loaded.
data_load_state.text("Done! (using cached data)")


with st.sidebar:
    neighborhood = st.multiselect(
        'Neighborhood',
        options=data['neighborhood'].unique(),
        placeholder='Select a neighborhood')
    
    location_type = st.selectbox(
        'Location Type',
        options=data['location_type'].unique(),
        placeholder='Select a Location Type',
        index=None)
    
    number_bedrooms = st.multiselect(
        'Number of Bedrooms',
        options=sorted(data['bedrooms'].unique()),
        placeholder='Select number of bedrooms')
    price = st.slider(
        'Range of Price',
        max_value=data['price'].max(),
        min_value=data['price'].min(),
        step=100)
    price_per_area = st.slider(
        'Price per area slider',
        value=[data['price_per_area'].min(), data['price_per_area'].max()]
    )

if neighborhood:
    data = data.query("neighborhood in @neighborhood")
# if location_type:
#     data = data.query("location_type in @location_type")
# if number_bedrooms:
#     data = data.query("bedrooms in @number_bedrooms")
if price:
    data = data.query("price <= @price")
if price_per_area:
    st.write(price_per_area)
    data = data.query("price_per_area <= @price_per_area[1]")

if st.checkbox("Show raw data"):
    st.subheader("Raw data")
    st.write(data)


price_per_area_colorbar = [*raw_data["price_per_area"]]

custom_data = np.stack(
    (
        data["link"],
        data["price"],
        data["price_per_area"],
        data["condo_fee"],
        data["total_area_m2"],
        data["floor"],
    ),
    axis=1,
)

hover_template = (
    "<b>%{customdata[0]}</b> <br>"
    + "Price: R$ %{customdata[1]:,.2f} <br>"
    + "Price per Area: R$/m<sup>2</sup> %{customdata[2]:,.2f} <br>"
    + "Condo Fee: R$ %{customdata[3]:,.2f} <br>"
    + "Usable Area: %{customdata[4]} m<sup>2</sup> <br>"
    + "Floor: %{customdata[5]}"
)

size = 1 / data["price_per_area"]

# Initializing Figure
fig = go.Figure()

fig.add_trace(
    go.Scattermapbox(
        lat=data["latitude"],
        lon=data["longitude"],
        mode="markers",
        name="",
        customdata=custom_data,
        hovertemplate=hover_template,
        showlegend=False,
        marker=go.scattermapbox.Marker(
            size=size,
            sizemin=8,
            symbol="circle",
            colorscale="RdYlBu_r",
            color=data["price_per_area"],
            cmin=min(price_per_area_colorbar),
            cmax=max(price_per_area_colorbar),
        ),
    )
)

new_listings = data[
    data.loc[:, "listing_date"]
    >= (datetime.datetime.today() - datetime.timedelta(days=7)).date()
]

fig.add_trace(
    go.Scattermapbox(
        lat=new_listings["latitude"],
        lon=new_listings["longitude"],
        mode="markers",
        name="",
        customdata=custom_data,
        hovertemplate=hover_template,
        showlegend=False,
        marker=go.scattermapbox.Marker(symbol="star", size=8),
    )
)

fig.update_layout(
    hovermode="closest",
    hoverdistance=50,
    hoverlabel=dict(
        # bgcolor="white",
        font_size=16,
        font_family="Rockwell",
    ),
    width=1500,
    height=1000,
    margin=dict(l=0, r=0, t=0, b=0),
    legend={"bgcolor": "rgba(0,0,0,0)"},
    mapbox=dict(
        style="streets",
        accesstoken=mapbox_token,
        bearing=0,
        center=dict(lat=data["latitude"].mean(), lon=data["longitude"].mean()),
        pitch=0,
        zoom=15,
    ),
)

st.plotly_chart(fig, use_container_width=True)
