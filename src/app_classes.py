import math
import os
import textwrap

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

import src.extract as extract
import src.visualization as visualization

load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]


class AppVisualizer:
    """
    Visualizer class that takes care of the visualization of the app.
    """

    def __init__(self):
        self.city = None

        self.data = pd.DataFrame()
        self.filtered_data = pd.DataFrame()
        self.city_price_per_area_distribution = []
        self.update_map = True
        
    def write_welcome_message_modal_first_start(self):
        @st.experimental_dialog(
            f"Bem-vindx {st.session_state.get('user_info', {}).get('name', 'Visitante').split(' ')[0]}!",
            width="small",
        )
        def welcome_message():
            st.write(
                """Bargain Bungalow usa AI para ajudar você a encontrar
                os melhores negócios imobiliários."""
            )

        if "first_start" not in st.session_state:
            st.session_state.first_start = False
            welcome_message()

    def create_price_per_area_distribution_histogram(self):
        fig = go.Figure()
        _, bins = np.histogram(self.city_price_per_area_distribution, bins="auto")

        fig.add_trace(
            go.Histogram(
                x=self.city_price_per_area_distribution,
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

        return st.plotly_chart(
            fig, use_container_width=True, config={"displayModeBar": False}
        )

    def create_side_bar_with_filters(self):
        # Create sidebar logo
        st.logo(os.path.join("assets", "bargain_bungalow.png"))

        with st.sidebar:
            # Create title for Filter sidebar
            st.subheader(":black[Filtros]")
            # Create filter for city
            st.markdown("Cidade")
            city = st.selectbox(
                "city",
                options=extract.get_unique_cities_from_db(),
                placeholder="Selecione uma cidade",
                index=0,
                label_visibility="collapsed",
            )
            self.city = city

            data = extract.get_best_deals_from_city(city)
            self.data = data
            self.filtered_data = data

            self.city_price_per_area_distribution = [*data["price_per_area"]]

            with st.form("listing_filters"):

                # Create neighborhood filter
                st.markdown("Bairro")
                neighborhood = st.multiselect(
                    "Bairro",
                    options=sorted(data["neighborhood"].unique()),
                    placeholder="Selecione um bairro",
                    label_visibility="collapsed",
                )
                if not neighborhood:
                    neighborhood = data["neighborhood"].unique()

                st.divider()

                st.markdown("Localização")
                location_type = st.selectbox(
                    "Location Type",
                    options=data["location_type"].unique(),
                    placeholder="Selecione um tipo de localização",
                    index=None,
                    label_visibility="collapsed",
                )

                st.divider()
                st.markdown("Número de quartos")
                number_bedrooms = st.selectbox(
                    "Number of Bedrooms",
                    options=sorted(data["bedrooms"].unique(), reverse=False),
                    placeholder="Selecione um número de quartos",
                    label_visibility="collapsed",
                    format_func=lambda x: f"{int(x)}+" if x == x else None,
                    key="number_bedrooms",
                )

                st.divider()
                st.markdown("Mostrar apenas novas ofertas?")
                new_listing = st.toggle(
                    label="New listings",
                    label_visibility="collapsed",
                    value=False,
                    key="new_listings",
                )

                st.divider()
                st.markdown("Preço por Área")

                self.create_price_per_area_distribution_histogram()

                price_per_area = st.slider(
                    "Price per Area (R$/m²)",
                    min_value=math.floor(data["price_per_area"].min() / 100) * 100,
                    max_value=math.ceil(data["price_per_area"].max() / 100) * 100,
                    step=100,
                    value=math.ceil(data["price_per_area"].max() / 100) * 100,
                    format="R$/m² %d",
                    label_visibility="collapsed",
                    key="price_per_area",
                )

                st.divider()
                st.markdown("Preço (R$)")
                price = st.slider(
                    "Price",
                    min_value=math.floor(data["price"].min() / 100000) * 100000,
                    max_value=math.ceil(data["price"].max() / 100000) * 100000,
                    value=math.ceil(data["price"].max() / 100000) * 100000,
                    step=100000,
                    format="R$ %d",
                    label_visibility="collapsed",
                    key="price",
                )

                st.divider()
                st.markdown("Área (m²)")
                area = st.slider(
                    "Area",
                    min_value=math.floor(data["total_area_m2"].min() / 50) * 50,
                    max_value=math.ceil(data["total_area_m2"].max() / 50) * 50,
                    value=(
                        math.floor(data["total_area_m2"].min() / 50) * 50,
                        math.ceil(data["total_area_m2"].max() / 50) * 50,
                    ),
                    step=50,
                    format="m² %d",
                    label_visibility="collapsed",
                    key="area",
                )
                submit = st.form_submit_button("Filtrar anúncios")

            if submit:
                self.filtered_data = self.data.query(
                    """(city == @city) & (neighborhood in @neighborhood) & (new_listing == @new_listing) & (bedrooms >= @number_bedrooms) & (price_per_area <= @price_per_area) & (price <= @price) & (total_area_m2 >= @area[0]) & (total_area_m2 <= @area[1])"""
                )
                self.update_map = True

    def create_listings_map(self):

        mapbox_token = os.environ["MAPBOX_TOKEN"]

        data = self.filtered_data

        custom_data = np.stack(
            (
                data["link"],
                data["price"],
                data["price_per_area"],
                data["condo_fee"],
                data["total_area_m2"],
                data.index,
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
        price_per_area_colorbar = self.city_price_per_area_distribution

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
                # cluster={"enabled": True, 'step':10, 'size':2*(1 / data["price_per_area"])},
                marker=go.scattermapbox.Marker(
                    allowoverlap=True,
                    size=marker_size,
                    sizemin=6,
                    symbol="circle",
                    colorscale="Jet",
                    color=(data["price_per_area"] // 100) * 100,
                    cmin=min(price_per_area_colorbar),
                    cmax=max(price_per_area_colorbar),
                    opacity=1,
                ),
            )
        )

        fig.update_layout(
            hovermode="closest",
            hoverdistance=30,
            hoverlabel_align="left",
            hoverlabel=dict(font_size=12, font_family="Aptos", bordercolor="silver"),
            height=800,
            margin=dict(l=0, r=0, t=50, b=0),
            showlegend=False,
            mapbox=dict(
                style="streets",
                accesstoken=mapbox_token,
                bearing=0,
                center=dict(lat=data["latitude"].mean(), lon=data["longitude"].mean()),
                pitch=0,
                zoom=12,
            ),
        )

        event = st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displayModeBar": False},
            on_select="rerun",
            selection_mode="points",
        )

        if event.selection.points:
            if "listings_clicked" not in st.session_state:
                st.session_state["listings_clicked"] = [
                    event.selection["points"][0]["customdata"][5]
                ]
            else:
                st.session_state["listings_clicked"].append(
                    event.selection["points"][0]["customdata"][5]
                )

        return

    @st.experimental_fragment
    def load_listings_map_and_table(self):
        if self.update_map:
            self.create_listings_map()
            with st.expander("Ver resultados em tabela"):
                st.write(self.filtered_data)
        self.update_map = False


class AppFormater:
    """
    FormatApp class that takes care of the formatting of the app.
    """

    def __init__(self):
        pass

    def format_page(self):
        st.set_page_config(layout="wide", page_icon="🏘️", page_title="Bargain Bungalow")

    def remove_whitespace(self):
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

    def increase_logo_size(self):
        st.markdown(
            """
                <style>
                    div[data-testid="stSidebarHeader"] > img, div[data-testid="collapsedControl"] > img {
                        height: 5rem;
                        width: auto;
                    }
                    div[data-testid="stSidebarHeader"], div[data-testid="stSidebarHeader"] > *,
                    div[data-testid="collapsedControl"], div[data-testid="collapsedControl"] > * {
                        display: flex;
                        align-items: center;
                    }
                </style>
            """,
            unsafe_allow_html=True,
        )
