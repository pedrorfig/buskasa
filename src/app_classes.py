import math
import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

import src.extract as extract

load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]


class App:
    """
    Visualizer class that takes care of the visualization of the app.
    """

    def __init__(self):
        self.city = None
        self.data = pd.DataFrame()
        self.filtered_data = pd.DataFrame()
        self.business_type = None
        self.city_price_per_area_distribution = []
        self._engine = extract.create_db_engine()

    def create_login_modal(self):
        if "business_type" not in st.session_state and "city" not in st.session_state:

            @st.dialog("🎉 Bem-vindo(a) ao Buskasa!", width="small")
            def welcome():
                st.write(
                    """
                        Aqui, você encontra o lar ideal com os critérios que realmente importam! 🏡🌿

                        🔍 Destaques que vão facilitar sua busca:

                        - Custo-benefício otimizado: a gente já seleciona os melhores anúncios pra você.
                        - Silêncio no entorno: veja quão tranquilo é o ambiente ao redor.
                        - Mais verde: filtre por áreas com mais natureza e qualidade de vida.
                        - Sem fraudes: bloqueamos anúncios suspeitos pra você não perder tempo.

                        O que você procura? 🚀
                        """
                )
                col1, col2, col3, col4 = st.columns(
                    [0.25, 0.25, 0.25, 0.25], gap="small"
                )
                if "business_type" not in st.session_state:
                    st.session_state.business_type = None
                if "city" not in st.session_state:
                    st.session_state.city = None

                city = st.selectbox(
                    "Cidade",
                    options=["São Paulo", "Rio de Janeiro"],
                    key="city_modal",
                    index=0,
                )

                with col2:
                    if st.button("Comprar", type="primary"):
                        st.session_state.business_type = "SALE"
                        st.session_state.city = city
                        self.business_type = "SALE"
                        st.rerun()
                with col3:
                    if st.button("Alugar", type="primary"):
                        st.session_state.business_type = "RENTAL"
                        st.session_state.city = city
                        self.business_type = "RENTAL"
                        st.rerun()

            welcome()

    def get_listings(self):
        """
        Read house listings from db table
        Returns:

        """
        engine = self._engine
        with engine.begin() as conn:
            self.data = extract.get_listings(
                conn, st.session_state.business_type, st.session_state.city
            )

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
                hoverinfo="skip",
            )
        )

        fig.update_yaxes(showgrid=False, visible=False, showticklabels=False)
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            height=150,
            dragmode=False,
            hovermode=False,
            uirevision="constant",
        )
        with st.container(height=150, border=False):

            st.plotly_chart(
                fig,
                config={"displayModeBar": False, "responsive": False},
            )

        return

    def create_side_bar_with_filters(self):
        # Create sidebar logo
        st.logo(
            os.path.join("assets", "Buskasa.png"),
        )

        with st.sidebar:
            # Create title for Filter sidebar

            self.filtered_data = self.data.copy()

            with st.form("listing_filters"):
                st.header("Filtros")
                with st.expander(
                    "Localização", expanded=False, icon=":material/location_on:"
                ):

                    self.city_price_per_area_distribution = [
                        *self.data["price_per_area"]
                    ]
                    st.divider()

                    # Create neighborhood filter
                    st.markdown("Bairro")
                    neighborhood = st.multiselect(
                        "Bairro",
                        options=sorted(self.data["neighborhood"].unique()),
                        placeholder="Selecione um bairro",
                        label_visibility="collapsed",
                    )
                    if not neighborhood:
                        neighborhood = self.data["neighborhood"].unique()

                    st.divider()
                    # Type of street location filter
                    st.markdown("Tipo de Localização")
                    location_type = st.multiselect(
                        "Location Type",
                        options=self.data["location_type"].unique(),
                        placeholder="Selecione um tipo de localização",
                        label_visibility="collapsed",
                    )
                    if not location_type:
                        location_type = self.data["location_type"].unique()
                with st.expander(
                    "Características do Imóvel", expanded=False, icon=":material/home:"
                ):
                    st.markdown("Tipo de Imóvel")
                    unit_type = st.multiselect(
                        label="Tipo de imóvel",
                        options=["Apartamentos", "Casas"],
                        placeholder="Apartamentos e Casas",
                        label_visibility="collapsed",
                    )
                    # Determine the condition based on the selection
                    if unit_type == ["Apartamentos"]:
                        unit_type_filter = self.data["unit_type"] == "APARTMENT"
                    elif unit_type == ["Casas"]:
                        unit_type_filter = self.data["unit_type"] == "HOME"
                    else:
                        unit_type_filter = True  # Show all rows
                    st.divider()
                    st.markdown("Número de quartos")
                    number_bedrooms = st.selectbox(
                        "Number of Bedrooms",
                        options=sorted(self.data["bedrooms"].unique(), reverse=False),
                        placeholder="Selecione um número de quartos",
                        label_visibility="collapsed",
                        format_func=lambda x: f"{int(x)}+" if x == x else None,
                        key="number_bedrooms",
                    )
                    st.divider()
                    st.markdown("Área (m²)")
                    area = st.slider(
                        "Area",
                        min_value=math.floor(self.data["total_area_m2"].min() / 50)
                        * 50,
                        max_value=math.ceil(self.data["total_area_m2"].max() / 50) * 50,
                        value=(
                            math.floor(self.data["total_area_m2"].min() / 50) * 50,
                            math.ceil(self.data["total_area_m2"].max() / 50) * 50,
                        ),
                        step=50,
                        format="m² %d",
                        label_visibility="collapsed",
                        key="area",
                    )
                    st.divider()
                    st.markdown("Preço (R$)")

                    price = st.slider(
                        "Price",
                        min_value=(
                            math.floor(self.data["price"].min() / 100000) * 100000
                            if st.session_state.business_type == "SALE"
                            else math.floor(self.data["price"].min() / 1000) * 1000
                        ),
                        max_value=(
                            math.ceil(self.data["price"].max() / 100000) * 100000
                            if st.session_state.business_type == "SALE"
                            else math.floor(self.data["price"].max() / 1000) * 1000
                        ),
                        value=(
                            math.ceil(self.data["price"].max() / 100000) * 100000
                            if st.session_state.business_type == "SALE"
                            else math.floor(self.data["price"].max() / 1000) * 1000
                        ),
                        step=(
                            100000 if st.session_state.business_type == "SALE" else 1000
                        ),
                        format="R$ %d",
                        label_visibility="collapsed",
                        key="price",
                    )
                with st.expander(
                    "Filtros inteligentes",
                    expanded=True,
                    icon=":material/emoji_objects:",
                ):
                    st.markdown("Custo-benefício")

                    self.create_price_per_area_distribution_histogram()

                    price_per_area = st.slider(
                        "Price per Area (R$/m²)",
                        min_value=math.floor(self.data["price_per_area"].min() / 100)
                        * 100,
                        max_value=math.ceil(self.data["price_per_area"].max() / 100)
                        * 100,
                        step=100 if st.session_state.business_type == "SALE" else 10,
                        value=math.ceil(self.data["price_per_area"].max() / 100) * 100,
                        format="R$/m² %d",
                        label_visibility="collapsed",
                        key="price_per_area",
                    )

                    st.divider()
                    st.markdown("Arborização")
                    green_density_map = {
                        "Pouco Verde": "Urbano",
                        "Moderadamente Verde": "Balanceado",
                        "Bastante Verde": "Arborizado",
                    }
                    green_density = st.multiselect(
                        "Green Density",
                        options=(
                            "Pouco Verde",
                            "Moderadamente Verde",
                            "Bastante Verde",
                        ),
                        format_func=lambda x: green_density_map[x],
                        label_visibility="collapsed",
                        key="green_density",
                    )
                    if not green_density:
                        green_density = self.data["green_density_grouped"].unique()

                    st.divider()
                    st.markdown("Movimentação")
                    movement_intensity = st.multiselect(
                        "Green Density",
                        options=["Muito Calmo", "Calmo", "Movimentado", "Agitado"],
                        label_visibility="collapsed",
                        key="n_nearby_bus_lanes_grouped",
                    )
                    if not movement_intensity:
                        movement_intensity = self.data[
                            "n_nearby_bus_lanes_grouped"
                        ].unique()
                    st.divider()
                    st.markdown("Reformado")
                    is_remodeled = st.checkbox("Reformado", key="is_remodeled")
                    if is_remodeled:
                        is_remodeled_filter = self.data["is_remodeled"] == True
                    else:
                        is_remodeled_filter = True

                submit = st.form_submit_button("Filtrar anúncios", type="primary")

            if submit:
                self.filtered_data = self.data.loc[
                    (
                        (self.data["neighborhood"].isin(neighborhood))
                        & (self.data["location_type"].isin(location_type))
                        & (self.data["bedrooms"] >= number_bedrooms)
                        & (self.data["price_per_area"] <= price_per_area)
                        & (self.data["price"] <= price)
                        & (self.data["total_area_m2"] >= area[0])
                        & (self.data["total_area_m2"] <= area[1])
                        & (self.data["green_density_grouped"].isin(green_density))
                        & (self.data["n_nearby_bus_lanes_grouped"].isin(movement_intensity))
                        & (unit_type_filter)
                        & (is_remodeled_filter)
                    )
                ]
            else:
                self.filtered_data = self.data.loc[
                    self.data["city"] == st.session_state.city
                ]
            st.write(len(self.filtered_data))

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
            "<b>%{customdata[0]}</b> <br>"
            + "<b>Preço   </b>               R$%{customdata[1]:,.0f}<br>"
            + "<b>Preço por Área </b> R$/m<sup>2</sup> %{customdata[2]:,.2f} <br>"
            + "<b>Condomínio </b>       R$ %{customdata[3]:,.2f} <br>"
            + "<b>Área útil</b>      %{customdata[4]} m<sup>2</sup> <br>"
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
                showlegend=False,
                marker=go.scattermapbox.Marker(
                    allowoverlap=True,
                    size=marker_size,
                    sizemin=6,
                    symbol="circle",
                    colorscale="Jet",
                    color=(
                        (data["price_per_area"] // 100) * 100
                        if self.business_type == "SALE"
                        else (data["price_per_area"] // 10) * 10
                    ),
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
            autosize=True,
            margin=dict(l=0, r=0, t=0, b=0),
            legend=dict(
                yanchor="top", y=0.99, xanchor="right", x=0.95, bgcolor="White"
            ),
            legend_title_text="Custo-benefício",
            showlegend=True,
            mapbox=dict(
                style="streets",
                accesstoken=mapbox_token,
                bearing=0,
                center=dict(
                    lat=data["latitude"].median(), lon=data["longitude"].median()
                ),
                pitch=0,
                zoom=12.5,
            ),
        )

        # Add a customized legend item with a filled circle
        fig.add_trace(
            go.Scatter(
                x=[None],  # No actual data
                y=[None],  # No actual data
                mode="markers",  # Marker mode to display as circle
                name="Excelente",  # Custom name in legend
                marker=dict(
                    size=10,  # Size of the circle
                    color="blue",  # Fill color of the circle
                    symbol="circle",  # Use 'circle' symbol for a filled circle
                ),
                showlegend=True,
            )
        )

        # Add a customized legend item with a filled circle
        fig.add_trace(
            go.Scatter(
                x=[None],  # No actual data
                y=[None],  # No actual data
                mode="markers",  # Marker mode to display as circle
                name="Ótimo",  # Custom name in legend
                marker=dict(
                    size=10,  # Size of the circle
                    color="green",  # Fill color of the circle
                    symbol="circle",  # Use 'circle' symbol for a filled circle
                ),
                showlegend=True,
            )
        )
        # Add a customized legend item with a filled circle
        fig.add_trace(
            go.Scatter(
                x=[None],  # No actual data
                y=[None],  # No actual data
                mode="markers",  # Marker mode to display as circle
                name="Bom",  # Custom name in legend
                marker=dict(
                    size=10,  # Size of the circle
                    color="red",  # Fill color of the circle
                    symbol="circle",  # Use 'circle' symbol for a filled circle
                ),
                showlegend=True,
            )
        )

        fig.update_yaxes(showgrid=False, visible=False, showticklabels=False)
        fig.update_xaxes(showgrid=False, visible=False, showticklabels=False)

        st.plotly_chart(
            fig,
            config={"displayModeBar": False, "scrollZoom": True},
            use_container_width=True,
        )

        return

    @st.fragment
    def load_listings_map(self):
        self.create_listings_map()


class AppFormater:
    """
    AppFormater class that takes care of the formatting of the app.
    """

    def __init__(self):
        pass

    def format_page(self):
        st.set_page_config(
            layout="wide",
            page_icon="🏘️",
            page_title="Buskasa",
            initial_sidebar_state="expanded",
            menu_items={
                "Get help": "https://www.linkedin.com/in/pedro-figueiredo-77377872/",
            },
        )

    def format_app_layout(self):
        with open("assets/style.css") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
