from datetime import datetime
import math
import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

import src.extract as extract
import src.save as save
from src.streamlit_google_auth import Authenticate
from sqlalchemy import Table, Column, Integer, String, MetaData, text
from sqlalchemy.dialects.postgresql import insert


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
        self.city_price_per_area_distribution = []
        self.listings_visited_by_user = []
        self.auth = Authenticate(
            secret_credentials_path="google_credentials.json",
            cookie_name="bargain_bungalow_cookie_name",
            cookie_key="bargain_bungalow_cookie_key",
        )
        self.user_email = ""
        self.name = ""
        self.user_type = ""
        self._engine = extract.create_db_engine()

    def get_user_data(self):

        if "user_info" in st.session_state:
            self.user_email = st.session_state["user_info"]["email"]
            self.user_name = st.session_state["user_info"]["name"]
        self.user_type = "Registered" if self.user_email else "Guest"

    def check_if_user_has_visits(self):
        engine = self._engine
        with engine.connect() as conn:
            user_has_visits = pd.read_sql(
                """
                    SELECT
                        CASE
                            WHEN COUNT(*) > 0 then True
                            ELSE False
                        END AS has_visits
                    FROM fact_listings_visited
                    WHERE "user" = %(user)s
                    """,
                con=conn,
                params={"user": self.user_email},
            ).iloc[0, 0]
        self.user_has_visits = user_has_visits

    def get_listings(self):
        """
        Read house listings from db table
        Returns:

        """
        engine = self._engine
        with engine.connect() as conn:
            self.data = extract.get_listings(
                conn, self.user_has_visits, self.user_email
            )

    def write_welcome_message_modal_first_start(self):
        @st.experimental_dialog(
            f"Bem-vindx {st.session_state.get('user_info', {}).get('name', 'Visitante').split(' ')[0]}!",
            width="small",
        )
        def welcome_message():
            st.write(
                """Buskasa usa IA para ajudar você a encontrar
                os melhores negócios imobiliários."""
            )

        if "first_start" not in st.session_state:
            st.session_state.first_start = False
            welcome_message()

    def get_listings_visited_by_user(self):
        engine = self._engine
        with engine.connect() as conn:
            # Checking for existing listing_ids on the database
            # according to specified filters
            filter_conditions = {
                "user": st.session_state["user_info"]["email"],
            }
            visited_listing_id_sql_statement = r"""
                SELECT visited_listing_id
                FROM fact_listings_visited
                WHERE "user" = %(user)s
                """
            ids = pd.read_sql(
                visited_listing_id_sql_statement, con=conn, params=filter_conditions
            )
            self.listings_visited_by_user = [*ids["visited_listing_id"]]

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
            height=250,
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
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
            st.subheader(":black[Filtros]")

            self.filtered_data = self.data.copy()

            with st.form("listing_filters"):

                # Create filter for city
                st.markdown("Cidade")
                city = st.selectbox(
                    "city",
                    options=self.data["city"].unique(),
                    placeholder="Selecione uma cidade",
                    index=0,
                    label_visibility="collapsed",
                )

                self.city_price_per_area_distribution = [*self.data["price_per_area"]]

                st.divider()
                st.markdown("Tempo de anúncios")
                new_listing = st.selectbox(
                    label="New listings",
                    label_visibility="collapsed",
                    options=["Todos", "Apenas recentes (até 7 dias)"],
                    key="new_listings",
                )
                if self.user_type == "Registered":
                    st.divider()
                    # Create visited listings filter

                    st.markdown("Visualização de anúncios")
                    visited_listings = st.selectbox(
                        label="Visualização de anúncios",
                        label_visibility="collapsed",
                        options=[
                            "Todos",
                            "Apenas já visualizados",
                            "Apenas não visualizados",
                        ],
                    )

                st.divider()
                st.markdown("Tipo de Imóvel")
                unit_type = st.selectbox(
                    label="Tipo de imóvel",
                    options=["Todos", "Apartamentos", "Casas"],
                    placeholder="Selecione um tipo de imóvel",
                    label_visibility="collapsed",
                    index=0,
                )
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

                st.markdown("Localização")
                location_type = st.selectbox(
                    "Location Type",
                    options=self.data["location_type"].unique(),
                    placeholder="Selecione um tipo de localização",
                    index=None,
                    label_visibility="collapsed",
                )
                if not location_type:
                    location_type = self.data["location_type"].unique()

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
                st.markdown("Preço por Área")

                self.create_price_per_area_distribution_histogram()

                price_per_area = st.slider(
                    "Price per Area (R$/m²)",
                    min_value=math.floor(self.data["price_per_area"].min() / 100) * 100,
                    max_value=math.ceil(self.data["price_per_area"].max() / 100) * 100,
                    step=100,
                    value=math.ceil(self.data["price_per_area"].max() / 100) * 100,
                    format="R$/m² %d",
                    label_visibility="collapsed",
                    key="price_per_area",
                )

                st.divider()
                st.markdown("Preço (R$)")
                price = st.slider(
                    "Price",
                    min_value=math.floor(self.data["price"].min() / 100000) * 100000,
                    max_value=math.ceil(self.data["price"].max() / 100000) * 100000,
                    value=math.ceil(self.data["price"].max() / 100000) * 100000,
                    step=100000,
                    format="R$ %d",
                    label_visibility="collapsed",
                    key="price",
                )

                st.divider()
                st.markdown("Área (m²)")
                area = st.slider(
                    "Area",
                    min_value=math.floor(self.data["total_area_m2"].min() / 50) * 50,
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
                submit = st.form_submit_button("Filtrar anúncios")

            if submit:
                self.filtered_data = self.data.loc[
                    (
                        (self.data["neighborhood"].isin(neighborhood))
                        & (self.data["city"] == city)
                        & (self.data["location_type"].isin(location_type))
                        & (self.data["bedrooms"] >= number_bedrooms)
                        & (self.data["price_per_area"] <= price_per_area)
                        & (self.data["price"] <= price)
                        & (self.data["total_area_m2"] >= area[0])
                        & (self.data["total_area_m2"] <= area[1])
                        & (
                            (
                                self.data["unit_type"] == "APARTMENT"
                                if unit_type == "Apartamentos"
                                else self.data["unit_type"] == "HOME"
                            )
                            if unit_type in ["Apartamentos", "Casas"]
                            else True
                        )
                        & (
                            self.data["new_listing"] == True
                            if new_listing == "Apenas recentes (até 7 dias)"
                            else True
                        )
                        & (
                            (
                                self.data["user"].isnull()
                                if visited_listings == "Apenas não visualizados"
                                else self.data["user"].notnull()
                            )
                            if visited_listings
                            in ["Apenas não visualizados", "Apenas já visualizados"]
                            else True
                        )
                    )
                ]

            if st.button("Log out"):
                self.auth.logout()

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
            margin=dict(l=0, r=0, t=0, b=0),
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
        with st.container(border=True):
            event = st.plotly_chart(
                fig,
                use_container_width=True,
                config={"displayModeBar": False},
                on_select="rerun",
                selection_mode="points",
            )

        self.save_listings_visited_by_user_to_db(event)

        return

    def save_listings_visited_by_user_to_db(self, event):
        if "user_info" in st.session_state:
            if event.selection.points:
                engine = self._engine
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                                INSERT INTO fact_listings_visited ("user", "visited_listing_id")
                                VALUES (:user, :visited_listing_id)
                                ON CONFLICT ("user", "visited_listing_id")
                                DO NOTHING;
                            """
                        ),
                        {
                            "user": self.user_email,
                            "visited_listing_id": event.selection["points"][0][
                                "customdata"
                            ][5],
                        },
                    )

    @st.experimental_fragment
    def load_listings_map_and_table(self):
        self.create_listings_map()
        with st.expander("Ver resultados em tabela", icon=":material/menu:"):
            st.write(self.filtered_data)
        st.write(f"Última atualização: {datetime.today().date()}")


class AppFormater:
    """
    AppFormater class that takes care of the formatting of the app.
    """

    def __init__(self):
        pass

    def format_page(self):
        st.set_page_config(layout="wide", page_icon="🏘️", page_title="Buskasa")

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
                    .st-emotion-cache-uzeiqp e1nzilvr4 {{
                        padding: 16px 16px
                    }}
                    hr {{
                        margin: 0px;
                    }}
                    .st-emotion-cache-lt1jbb {{
                        gap: 0.5rem
                    }}
                    .element-container st-emotion-cache-d65s76 e1f1d6gn4 {{
                        heigh: 0px
                    }}
                    .mapboxgl-map {{
                        all: uset
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
