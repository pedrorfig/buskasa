import copy
import numpy as np
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dotenv import load_dotenv
from etl_modules import extract
import os

load_dotenv()

mapbox_token = os.environ['MAPBOX_TOKEN']

results = extract.read_listings_sql_table()

app = Dash(external_stylesheets=[dbc.themes.SLATE])
server = app.server

mapbox_scatter_chart = dbc.Card([dcc.Graph(figure={}, id="mapbox_scatter_chart", className="h-100")], className="h-100")

histogram_chart = dcc.Graph(figure={}, config={'displayModeBar': False}, id="histogram_chart")

controls = \
    dbc.Card([
        html.H4("Filters", className='card-header'),
        html.Ul([
            html.Li([
                html.H5("Business Type", className='card-title'),
                dcc.Dropdown(
                    id="business_type",
                    options=results['business_type'].unique(),
                    value='SALE',
                    clearable=False,
                    multi=False,
                    style={'color': 'black'}
                )], className='list-group-item'),
            html.Li([
                html.H5("Neighborhood", className='card-title'),
                dcc.Dropdown(
                    id="neighborhood",
                    options=results['neighborhood'].unique(),
                    value=None,
                    clearable=True,
                    multi=True,
                    style={'color': 'black'}
                )], className='list-group-item'),
            html.Li([
                html.H5("Location Type", className='card-title'),
                dcc.Dropdown(
                    id="location_type",
                    options=sorted(results['location_type'].unique()),
                    value=None,
                    clearable=True,
                    multi=True,
                    style={'color': 'black'}
                )], className='list-group-item'),
            html.Li([
                html.H5("Number of Bedrooms", className='card-title'),
                dcc.Dropdown(
                    id="bedrooms",
                    options=sorted(results['bedrooms'].unique()),
                    value=None,
                    clearable=True,
                    multi=True,
                    style={'color': 'black'},
                )], className='list-group-item'),
            html.Li([
                html.H5(r'Price per Area (R$/m²)', className='card-title'),
                histogram_chart,
                dcc.RangeSlider(
                    id="price_per_area",
                    min=results['price_per_area'].min(),
                    max=results['price_per_area'].max(),
                    step=1,
                    marks=None,
                    allowCross=False,
                    tooltip={"placement": "bottom", "always_visible": True},
                    className='px-1'
                )
            ], className='list-group-item')
        ], className='list-group list-group-flush')
    ])
modal = dbc.Modal([dbc.ModalHeader(dbc.ModalTitle("Welcome house hunter!")),
                   dbc.ModalBody(
                       [
                           html.P(
                               "Bargain Bungalow helps you by a curated list of house listings from São Paulo posted on the biggest real state search engine."),
                           html.P('🤑 It helps you find good deals'),
                           html.P('🧐 Removes fraudsters from the house listings')
                       ]
                   )],
                  id="modal", size="lg", is_open=True, backdrop=True, fade=True, centered=True)

# App layout
app.layout = dbc.Container(children=[
    dbc.Row([
        dbc.Col([
            html.H1("Bargain Bungalow")
        ])
    ], justify="center"),
    dbc.Row(
        [
            dbc.Col(controls, width=3),
            dbc.Col(mapbox_scatter_chart, width=9)
        ], className='mb-1'
    ),
    dbc.Row(
        [
            dbc.Col([
                html.P(["Meet the creator ",
                        html.A(["Pedro Figueiredo"], href="https://www.linkedin.com/in/pedro-figueiredo-77377872/")
                        ], className="position-absolute bottom-0 start-50 translate-middle-y mb-0"),
            ], align="center", width=4)
        ], justify="center"),
    modal,
], fluid=True
)


@app.callback(
    Output("bedrooms", "options"),
    Input('neighborhood', 'value'),
    Input('business_type', 'value'),
    Input('price_per_area', 'value'),
    Input('location_type', 'value')
)
def chained_callback_bedrooms(neighborhood, business_type, price_per_area, location_type):
    dff = copy.deepcopy(results)
    if neighborhood:
        dff = dff.query("neighborhood == @neighborhood")
    if business_type:
        dff = dff.query("business_type == @business_type")
    if price_per_area:
        dff = dff[dff['price_per_area'].between(price_per_area[0], price_per_area[1])]
    if location_type:
        dff = dff.query("location_type == @location_type")
    return sorted(dff["bedrooms"].unique())


@app.callback(
    Output("business_type", "options"),
    Input('neighborhood', 'value'),
    Input('bedrooms', 'value'),
    Input('price_per_area', 'value'),
    Input('location_type', 'value')
)
def chained_callback_business_type(neighborhood, bedrooms, price_per_area, location_type):
    dff = copy.deepcopy(results)
    if neighborhood:
        dff = dff.query("neighborhood == @neighborhood")
    if bedrooms:
        dff = dff.query("bedrooms == @bedrooms")
    if price_per_area:
        dff = dff[dff['price_per_area'].between(price_per_area[0], price_per_area[1])]
    if location_type:
        dff = dff.query("location_type == @location_type")
    return sorted(dff["business_type"].unique())


@app.callback(
    Output("price_per_area", "value"),
    Input('neighborhood', 'value'),
    Input('bedrooms', 'value'),
    Input('business_type', 'value'),
    Input('location_type', 'value')
)
def chained_callback_price_per_area(neighborhood, bedrooms, business_type, location_type):
    dff = copy.deepcopy(results)
    if neighborhood:
        dff = dff.query("neighborhood == @neighborhood")
    if bedrooms:
        dff = dff.query("bedrooms == @bedrooms")
    if business_type:
        dff = dff.query("business_type == @business_type")
    if location_type:
        dff = dff.query("location_type == @location_type")
    return [dff["price_per_area"].min(), dff["price_per_area"].max()]


@app.callback(
    Output("mapbox_scatter_chart", "figure"),
    Input('location_type', 'value'),
    Input("neighborhood", "value"),
    Input("bedrooms", "value"),
    Input("business_type", "value"),
    Input('price_per_area', 'value')
)
def generate_mapbox_chart(location_type, neighborhood, bedrooms, business_type, price_per_area, mapbox_token=mapbox_token):
    """
   Generate a scatterplot on a mapbox map based on the selected filters.

    Args:
        location_type:
        neighborhood (str): The selected neighborhood.
        bedrooms (int): The selected number of bedrooms.
        business_type (int): The selected number of business_type.
        price_per_area (list): Whether to filter by price per area.
        mapbox_token (str, optional): The Mapbox access token. Defaults to mapbox_token.

    Returns:
        plotly.graph_objects.Figure: The generated scatterplot figure.
    """
    results_copy = copy.deepcopy(results)

    if neighborhood:
        results_copy = results_copy.query("neighborhood == @neighborhood")

    if bedrooms:
        results_copy = results_copy.query("bedrooms == @bedrooms")

    if business_type:
        results_copy = results_copy.query("business_type == @business_type")

    if location_type:
        results_copy = results_copy.query("location_type == @location_type")

    price_per_area_colorbar = [*results_copy['price_per_area']]

    if price_per_area:
        results_copy = results_copy[results_copy['price_per_area'].between(price_per_area[0], price_per_area[1])]

    custom_data = np.stack((results_copy['link'], results_copy['price'], results_copy['price_per_area'],
                            results_copy['condo_fee'], results_copy['total_area_m2'], results_copy['floor']),
                           axis=1)

    hover_template = ('<b>%{customdata[0]}</b> <br>' +
                      'Price: R$ %{customdata[1]:,.2f} <br>' +
                      'Price per Area: R$/m<sup>2</sup> %{customdata[2]:,.2f} <br>' +
                      'Condo Fee: R$ %{customdata[3]:,.2f} <br>' +
                      'Usable Area: %{customdata[4]} m<sup>2</sup> <br>' +
                      'Floor: %{customdata[5]}')

    size = 1 / results_copy['price_per_area']

    # Initializing Figure
    fig = go.Figure()

    fig.add_trace(
        go.Scattermapbox(
            lat=results_copy['latitude'],
            lon=results_copy['longitude'],
            mode='markers',
            name='',
            customdata=custom_data,
            hovertemplate=hover_template,
            showlegend=False,
            marker=go.scattermapbox.Marker(
                size=size,
                sizemin=8,
                symbol="circle",
                # sizeref=0.00001,
                colorscale='RdYlGn_r',
                color=results_copy['price_per_area'],
                cmin=min(price_per_area_colorbar),
                cmax=max(price_per_area_colorbar)
            ),
        )
    )

    approximate_listings = results_copy[results_copy.loc[:, 'precision'] == 'approximate']

    fig.add_trace(
        go.Scattermapbox(
            lat=approximate_listings['latitude'],
            lon=approximate_listings['longitude'],
            mode='markers',
            name='',
            customdata=custom_data,
            hovertemplate=hover_template,
            showlegend=False,
            marker=go.scattermapbox.Marker(
                symbol="triangle",
                size=8
            ),
        )
    )

    fig.update_layout(
        hovermode='closest',
        hoverdistance=50,
        hoverlabel=dict(
            bgcolor="white",
            font_size=16,
            font_family="Rockwell"
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        legend={'bgcolor': 'rgba(0,0,0,0)'},
        mapbox=dict(
            style='dark',
            accesstoken=mapbox_token,
            bearing=0,
            center=dict(
                lat=results_copy['latitude'].mean(),
                lon=results_copy['longitude'].mean()
            ),
            pitch=0,
            zoom=15
        ),

    )
    return fig


@app.callback(
    Output("histogram_chart", "figure"),
    Input('location_type', 'value'),
    Input("neighborhood", "value"),
    Input("bedrooms", "value"),
    Input("business_type", "value")
)
def generate_histogram_chart(location_type, neighborhood, bedrooms, business_type):
    """
   Generate a histogram on price per area values

    Args:
        location_type:
        neighborhood (str): The selected neighborhood.
        bedrooms (int): The selected number of bedrooms.
        business_type (int): The selected number of business_type.
    Returns:
        plotly.graph_objects.Figure: The generated scatterplot figure.
    """
    results_copy = copy.deepcopy(results)

    if neighborhood:
        results_copy = results_copy.query("neighborhood == @neighborhood")

    if bedrooms:
        results_copy = results_copy.query("bedrooms == @bedrooms")

    if business_type:
        results_copy = results_copy.query("business_type == @business_type")

    if location_type:
        results_copy = results_copy.query("location_type == @location_type")

    fig = go.Figure()
    hist, bins = np.histogram(results_copy['price_per_area'], bins='auto')
    fig.add_trace(
        go.Histogram(
            x=results_copy['price_per_area'],
            xbins={'size': bins[1] - bins[0]},
            marker={'colorscale': 'RdYlGn_r', 'color': bins,
                    'cmin': bins.min(), 'cmax': results_copy['price_per_area'].max()
                    }
        )
    )

    fig.update_yaxes(showgrid=False, visible=False, showticklabels=False)
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=320,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )

    return fig


if __name__ == "__main__":
    app.run_server(debug=True)
