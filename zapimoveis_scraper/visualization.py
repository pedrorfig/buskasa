# Visualization module
import textwrap

import numpy as np
import plotly.graph_objects as go
import streamlit as st

from etl_modules import extract


def format_page():
    st.set_page_config(layout="wide", page_icon="🏘️", page_title="Bargain Bungalow")


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

    return st.plotly_chart(fig, use_container_width=True,
                           config={'displayModeBar': False})
    

def create_side_bar_with_filters():
    with st.sidebar:
        st.subheader(":black[Filters]")
        
        st.markdown("City")
        city = st.selectbox(
            "city",
            options=extract.get_unique_cities_from_db(),
            placeholder="Select a city",
            index=0,
            label_visibility="collapsed",
        )

        data = extract.get_best_deals_from_city(city)

        data = data.query("city == @city")

        city_data = data.copy()

        st.divider()

        st.markdown("Neighborhood")
        neighborhood = st.multiselect(
            "Neighborhood",
            options=sorted(data["neighborhood"].unique()),
            placeholder="Select a neighborhood",
            label_visibility="collapsed",
        )

        if neighborhood:
            data = data.query("neighborhood in @neighborhood")

        st.divider()

        st.markdown("Location Type")
        location_type = st.selectbox(
            "Location Type",
            options=data["location_type"].unique(),
            placeholder="Select a location type",
            index=None,
            label_visibility="collapsed",
        )

        if location_type:
            data = data.query("location_type in @location_type")

        st.divider()
        st.markdown("Number of Bedrooms")

        number_bedrooms = st.multiselect(
            "Number of Bedrooms",
            options=sorted(data["bedrooms"].unique()),
            placeholder="Select # bedrooms",
            label_visibility="collapsed",
        )

        if number_bedrooms:
            data = data.query("bedrooms in @number_bedrooms")

        st.divider()

        new_listings = st.toggle(label="New Listings", value=False)

        if new_listings:
            data = data.query("new_listing == @new_listings")

        st.divider()

        st.markdown("Price per area")

        create_price_per_area_distribution_histogram(data)

        price_per_area = st.slider(
            "Price per Area",
            min_value=data["price_per_area"].min(),
            max_value=data["price_per_area"].max(),
            step=100.0,
            value=data["price_per_area"].max(),
            format="R$/m² %d",
            label_visibility="collapsed",
        )

        if price_per_area:
            data = data.query("price_per_area <= @price_per_area+1")

        st.divider()
        st.markdown("Price")
        price = st.slider(
            "Price",
            max_value=data["price"].max()+0.01,
            min_value=data["price"].min()+0.001,
            value=data["price"].max()+0.01,
            step=100000.0,
            format="R$ %d",
            label_visibility="collapsed",
        )

        if price:
            data = data.query("price <= @price")
        
        if st.button("Reset cache", type="primary"):
            st.cache_data.clear()

    return (city_data, data)


def customwrap(s, width=30):
    return "<br>".join(textwrap.wrap(s, width=width))


def create_listings_map(mapbox_token, data, city_data):

    custom_data = np.stack(
        (
            data["link"],
            data["price"],
            data["price_per_area"],
            data["condo_fee"],
            data["total_area_m2"],
        ),
        axis=1,
    )

    hover_template = (
        "<b>Price   </b>               R$%{customdata[1]:,.0f}<br>"
        + "<b>Price per Area </b> R$/m<sup>2</sup> %{customdata[2]:,.2f} <br>"
        + "<b>Condo Fee  </b>       R$ %{customdata[3]:,.2f} <br>"
        + "<b>Usable Area</b>      %{customdata[4]} m<sup>2</sup> <br>"
        + "<b>%{customdata[0]}</b> <br>"
        + "<extra></extra>"
    )

    marker_size = 1 / data["price_per_area"]
    price_per_area_colorbar = [*city_data["price_per_area"]]

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
                allowoverlap=True,
                size=marker_size,
                sizemin=6,
                symbol="circle",
                colorscale="Jet",
                color=data["price_per_area"],
                cmin=min(price_per_area_colorbar),
                cmax=max(price_per_area_colorbar),
            ),
        )
    )

    new_listings = data[data.loc[:, "new_listing"]]

    fig.add_trace(
        go.Scattermapbox(
            lat=new_listings["latitude"],
            lon=new_listings["longitude"],
            mode="markers",
            marker=go.scattermapbox.Marker(
                symbol="circle",
                size=5,
                allowoverlap=True,
                # cauto=False,
                color="darkorchid",
            ),
            name="New listings",
            hoverinfo="skip",
        )
    )
    
    dynamic_zoom = get_dynamic_zoom(data)

    fig.update_layout(
        hovermode="closest",
        hoverdistance=30,
        hoverlabel_align="left",
        hoverlabel=dict(font_size=12,
                        font_family="Aptos",
                        bordercolor="silver"),
        # width=1500,
        height=800,
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="ghostwhite",
            itemsizing="constant",
        ),
        mapbox=dict(
            style="streets",
            accesstoken=mapbox_token,
            bearing=0,
            center=dict(lat=data["latitude"].mean(),
                        lon=data["longitude"].mean()),
            pitch=0,
            zoom=12,
        ),
    )

    st.plotly_chart(fig, use_container_width=True,
                    config={'displayModeBar': False})


def get_dynamic_zoom(data):
    standard_zoom = 500/(((data["latitude"].max() - data["latitude"].min())*(data["longitude"].max()-data["longitude"].min())))
    if standard_zoom > 15:
        standard_zoom = 15
    elif standard_zoom < 10:
        standard_zoom = 10
    return standard_zoom 
