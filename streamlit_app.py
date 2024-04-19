import streamlit as st
import numpy as np
import plotly.graph_objects as go
from dotenv import load_dotenv
from etl_modules import extract
import datetime
import os


load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]


def format_page():
    st.set_page_config(layout="wide", page_icon="🏘️", page_title="Bargain Bungalow")


format_page()


@st.cache_data
def load_data():
    results = extract.read_listings_sql_table()
    results = results.sort_values('price_per_area', ascending=False)
    
    return results


# Create a text element and let the reader know the data is loading.
# data_load_state = st.text("Loading data...")
# Load 10,000 rows of data into the dataframe.
data = load_data()
city_data = data.copy()
# Notify the reader that the data was successfully loaded.
# data_load_state.text("Done! (using cached data)")

st.header("Bargain Bungalow")
st.markdown("Helps you find the best house deals in São Paulo")


def create_price_per_area_distribution_histogram(data):
    fig = go.Figure()
    _, bins = np.histogram(data["price_per_area"], bins="auto")
    fig.add_trace(
        go.Histogram(
            x=data["price_per_area"],
            xbins={"size": bins[1] - bins[0]},
            marker={
                "colorscale": "Jet",
                "color": bins,
                "cmin": bins.min(),
                "cmax": bins.max(),
            },
        )
    )

    fig.update_yaxes(showgrid=False, visible=False, showticklabels=False)

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=250,
    )

    return st.plotly_chart(fig, use_container_width=True)


def remove_whitespace():
    padding_top = 1.5
    padding_bottom = 1.5
    st.markdown(
        f"""
            <style>
                .appview-container .main .block-container {{
                    padding-top: {padding_top}rem;
                    padding-bottom: {padding_bottom}rem;
                }}
                .st-emotion-cache-16txtl3 {{
                    padding: {padding_top+1}rem {padding_bottom}rem
                }}
            </style>""",
        unsafe_allow_html=True,
    )


remove_whitespace()


def create_side_bar_with_filters(data, create_price_per_area_distribution_histogram):
    with st.sidebar:
        st.subheader(":black[Filters]")

        st.markdown("Best Deals")
        best_deals = st.checkbox(
            label="Best Deals", value=True, label_visibility="collapsed"
        )
        st.markdown("City")
        city = st.selectbox(
            "city",
            options=data["city"].unique(),
            placeholder="Select a city",
            label_visibility="collapsed",
        )

        st.divider()

        st.markdown("Neighborhood")
        neighborhood = st.multiselect(
            "Neighborhood",
            options=data["neighborhood"].unique(),
            placeholder="Select a neighborhood",
            label_visibility="collapsed",
        )

        st.divider()

        st.markdown("Location Type")
        location_type = st.selectbox(
            "Location Type",
            options=data["location_type"].unique(),
            placeholder="Select a Location Type",
            index=None,
            label_visibility="collapsed",
        )

        st.divider()
        st.markdown("Number of Bedrooms")

        number_bedrooms = st.multiselect(
            "Number of Bedrooms",
            options=sorted(data["bedrooms"].unique()),
            placeholder="Select number of bedrooms",
            label_visibility="collapsed",
        )

        st.divider()

        st.markdown("Price per area")

        if city:
            city_data = data.query("city in @city")
        else:
            city_data = data.copy()

        create_price_per_area_distribution_histogram(city_data)

        price_per_area = st.slider(
            "Price per Area",
            min_value=int(city_data["price_per_area"].min()),
            max_value=int(city_data["price_per_area"].max()),
            step=10,
            value=int(city_data["price_per_area"].max()),
            format="R$/m² %d",
            label_visibility="collapsed",
        )

        st.divider()
        st.markdown("Price")
        price = st.slider(
            "Price",
            max_value=city_data["price"].max(),
            min_value=city_data["price"].min(),
            value=city_data["price"].max(),
            step=1000,
            format="R$ %d",
            label_visibility="collapsed",
        )

    return (
        best_deals,
        city,
        neighborhood,
        location_type,
        number_bedrooms,
        price,
        price_per_area,
    )


(
    best_deals,
    city,
    neighborhood,
    location_type,
    number_bedrooms,
    price,
    price_per_area,
) = create_side_bar_with_filters(data, create_price_per_area_distribution_histogram)


if best_deals:
    data = data.query(f"price_per_area_in_first_quartile == {best_deals}")
else:
    data = data.query(
        f"price_per_area_in_first_quartile == False or price_per_area_in_first_quartile == True"
    )
if city:
    data = data.query("city in @city")
    city_data = data.copy()
if neighborhood:
    data = data.query("neighborhood in @neighborhood")
if location_type:
    data = data.query("location_type in @location_type")
if number_bedrooms:
    data = data.query("bedrooms in @number_bedrooms")
if price:
    data = data.query("price <= @price")
if price_per_area:
    data = data.query("price_per_area <= @price_per_area")


def create_listings_map(mapbox_token, data, city_data):
    price_per_area_colorbar = [*city_data["price_per_area"]]

    custom_data = np.stack(
        (
            data["link"],
            data["price"],
            data["price_per_area"],
            data["condo_fee"],
            data["total_area_m2"]
        ),
        axis=1,
    )

    hover_template = (
        "<b>%{customdata[0]}</b> <br>"
        + "Price: R$%{customdata[1]:,.0f} <br>"
        + "Price per Area: R$/m<sup>2</sup> %{customdata[2]:,.2f} <br>"
        + "Condo Fee: R$ %{customdata[3]:,.2f} <br>"
        + "Usable Area: %{customdata[4]} m<sup>2</sup> <br>"
        + "<extra></extra>"
    )

    marker_size = (1/data["price_per_area"])     

    # Initializing Figure
    fig = go.Figure()

    fig.add_trace(
        go.Scattermapbox(
            lat=data["latitude"],
            lon=data["longitude"],
            mode="markers",
            name="All listings",
            customdata=custom_data,
            hovertemplate=hover_template,
            marker=go.scattermapbox.Marker(
                size=marker_size,
                sizemin=6,
                symbol="circle",
                colorscale="Jet",
                color=data["price_per_area"],
                cmin=min(price_per_area_colorbar),
                cmax=max(price_per_area_colorbar)
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
            marker=go.scattermapbox.Marker(
                symbol="circle", size=5, allowoverlap=True, cauto=False, color="brown"
            ),
            name="New listings",
            hoverinfo="skip",
        )
    )

    fig.update_layout(
        hovermode="closest",
        hoverdistance=30,
        hoverlabel_align = 'left',
        hoverlabel=dict(
            font_size=12,
            font_family="Rockwell",
        ),
        # width=1500,
        height=800,
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(219, 219, 219, 1)",
            # itemsizing="constant",
        ),
        mapbox=dict(
            style="streets",
            accesstoken=mapbox_token,
            bearing=0,
            center=dict(lat=data["latitude"].mean(), lon=data["longitude"].mean()),
            pitch=0,
            zoom=12,
        ),
    )

    st.plotly_chart(fig, use_container_width=True)


create_listings_map(mapbox_token, data, city_data)

if st.checkbox("Show raw data"):
    st.subheader("Raw data")
    st.write(data)
