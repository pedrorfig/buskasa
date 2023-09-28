import copy
import numpy as np
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import zapimoveis_scraper as zap
import plotly.graph_objects as go
from dotenv import load_dotenv
import os

load_dotenv()

mapbox_token = os.environ['MAPBOX_TOKEN']

results = zap.read_listings_sql_table()

app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

controls = \
    dbc.Card([
        html.Div([
            html.P("Neighborhood"),
            dcc.Dropdown(
                id="neighborhood",
                options=results['neighborhood'].unique(),
                value=None,
                clearable=True,
                multi=True
            )]
        ),
        html.Div([
            html.P("Location Type"),
            dcc.Dropdown(
                id="location_type",
                options=sorted(results['location_type'].unique()),
                value=None,
                clearable=True
            )]
        ),
        html.Div([
            html.P(r'Price per Area (R$/m²)'),
            dcc.RangeSlider(
                id="price_per_area",
                min=results['price_per_area'].min(),
                max=results['price_per_area'].max(),
                step=1,
                marks=None,
                tooltip={"placement": "bottom", "always_visible": True}
            )]
        ),
        html.Div([
            html.P("Bedrooms"),
            dcc.Dropdown(
                id="bedrooms",
                options=sorted(results['bedrooms'].unique()),
                value=None,
                clearable=True,
                multi=True
            )]
        ),
        html.Div([
            html.P("Bathrooms"),
            dcc.Dropdown(
                id="bathrooms",
                options=sorted(results['bathrooms'].unique()),
                value=None,
                clearable=True,
                multi=True
            )]
        ),
    ],
        body=True,
        style={'height': '90vh'}
    )

graph = dbc.Card(
    dcc.Graph(figure={}, id="graph", className="h-100"),
    style={'height': '90vh'}
)

app.layout = dbc.Container(
    [
        html.H1("Best Deals in São Paulo"),
        html.Hr(),
        dbc.Row([
            dbc.Col(controls, md=4),
            dbc.Col(graph, md=8)
        ], align='center'
        )
    ],
    fluid=True,
)

# TODO: check why circular dependency is happening between location type and price_per_area
@app.callback(
    Output("location_type", "value"),
    Input('neighborhood', 'value'),
    Input('bedrooms', 'value'),
    Input('bathrooms', 'value'),
    # Input("price_per_area", "value")
)
def chained_callback_location_type(neighborhood, bedrooms, bathrooms):
    dff = copy.deepcopy(results)
    if neighborhood:
        dff = dff.query("neighborhood == @neighborhood")
    if bedrooms:
        dff = dff.query("bedrooms == @bedrooms")
    if bathrooms:
        dff = dff.query("bathrooms == @bathrooms")
    # if price_per_area:
    #     dff = dff[dff['price_per_area'].between(price_per_area[0], price_per_area[1])]
    return sorted(dff["location_type"].unique())


@app.callback(
    Output("neighborhood", "options"),
    Input('bedrooms', 'value'),
    Input('bathrooms', 'value'),
    Input('price_per_area', 'value'),
    Input('location_type', 'value')
)
def chained_callback_neighborhood(bedrooms, bathrooms, price_per_area, location_type):
    dff = copy.deepcopy(results)
    if bedrooms:
        dff = dff.query("bedrooms == @bedrooms")
    if bathrooms:
        dff = dff.query("bathrooms == @bathrooms")
    if price_per_area:
        dff = dff[dff['price_per_area'].between(price_per_area[0], price_per_area[1])]
    if location_type:
        dff = dff.query("location_type == @location_type")
    return sorted(dff["neighborhood"].unique())


@app.callback(
    Output("bedrooms", "options"),
    Input('neighborhood', 'value'),
    Input('bathrooms', 'value'),
    Input('price_per_area', 'value'),
    Input('location_type', 'value')
)
def chained_callback_bedrooms(neighborhood, bathrooms, price_per_area, location_type):
    dff = copy.deepcopy(results)
    if neighborhood:
        dff = dff.query("neighborhood == @neighborhood")
    if bathrooms:
        dff = dff.query("bathrooms == @bathrooms")
    if price_per_area:
        dff = dff[dff['price_per_area'].between(price_per_area[0], price_per_area[1])]
    if location_type:
        dff = dff.query("location_type == @location_type")
    return sorted(dff["bedrooms"].unique())


@app.callback(
    Output("bathrooms", "options"),
    Input('neighborhood', 'value'),
    Input('bedrooms', 'value'),
    Input('price_per_area', 'value'),
    Input('location_type', 'value')
)
def chained_callback_bathrooms(neighborhood, bedrooms, price_per_area, location_type):
    dff = copy.deepcopy(results)
    if neighborhood:
        dff = dff.query("neighborhood == @neighborhood")
    if bedrooms:
        dff = dff.query("bedrooms == @bedrooms")
    if price_per_area:
        dff = dff[dff['price_per_area'].between(price_per_area[0], price_per_area[1])]
    if location_type:
        dff = dff.query("location_type == @location_type")
    return sorted(dff["bathrooms"].unique())


@app.callback(
    Output("price_per_area", "value"),
    Input('neighborhood', 'value'),
    Input('bedrooms', 'value'),
    Input('bathrooms', 'value'),
    Input('location_type', 'value')
)
def chained_callback_price_per_area(neighborhood, bedrooms, bathrooms, location_type):
    dff = copy.deepcopy(results)
    if neighborhood:
        dff = dff.query("neighborhood == @neighborhood")
    if bedrooms:
        dff = dff.query("bedrooms == @bedrooms")
    if bathrooms:
        dff = dff.query("bathrooms == @bathrooms")
    if location_type:
        dff = dff.query("location_type == @location_type")
    return [dff["price_per_area"].min(), dff["price_per_area"].max()]


@app.callback(
    Output("graph", "figure"),
    Input('location_type', 'value'),
    Input("neighborhood", "value"),
    Input("bedrooms", "value"),
    Input("bathrooms", "value"),
    Input('price_per_area', 'value')
)
def generate_chart(location_type, neighborhood, bedrooms, bathrooms, price_per_area, mapbox_token=mapbox_token):
    """
   Generate a scatterplot on a mapbox map based on the selected filters.

    Args:
        location_type:
        neighborhood (str): The selected neighborhood.
        bedrooms (int): The selected number of bedrooms.
        bathrooms (int): The selected number of bathrooms.
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

    if bathrooms:
        results_copy = results_copy.query("bathrooms == @bathrooms")

    if price_per_area:
        results_copy = results_copy[results_copy['price_per_area'].between(price_per_area[0], price_per_area[1])]

    if location_type:
        results_copy = results_copy.query("location_type == @location_type")

    size = 1 / results_copy['price_per_area']

    new_listings = results_copy[results_copy.loc[:, 'new_listing'] == 1]

    approximate_listings = results_copy[results_copy.loc[:, 'precision'] == 'approximate']

    hover_template = ('<b>%{customdata[0]}</b> <br>' +
                      'Price: R$ %{customdata[1]:,.2f} <br>' +
                      'Price per Area: R$/m<sup>2</sup> %{customdata[2]:,.2f} <br>' +
                      'Condo Fee: R$ %{customdata[3]:,.2f} <br>' +
                      'Usable Area: %{customdata[4]} m<sup>2</sup> <br>' +
                      'Floor: %{customdata[5]}')

    custom_data = np.stack((results_copy['link'], results_copy['price'], results_copy['price_per_area'],
                            results_copy['condo_fee'], results_copy['total_area_m2'], results_copy['floor']),
                           axis=1)
    fig = go.Figure()

    fig.add_trace(
        go.Scattermapbox(
            lat=results_copy['latitude'],
            lon=results_copy['longitude'],
            mode='markers',
            name='',
            customdata=custom_data,
            hovertemplate=hover_template,
            marker=go.scattermapbox.Marker(
                size=size,
                sizemin=8,
                symbol="circle",
                sizeref=0.00001,
                colorscale='plotly3_r',
                color=results_copy['price_per_area'],
                colorbar=dict(title='Price per Area (R$/m<sup>2</sup>)')
            ),
        )
    )

    fig.add_trace(
        go.Scattermapbox(
            lat=approximate_listings['latitude'],
            lon=approximate_listings['longitude'],
            mode='markers',
            name='',
            customdata=custom_data,
            hovertemplate=hover_template,
            marker=go.scattermapbox.Marker(
                symbol="triangle",
                size=8,
                colorscale='plotly3_r'
            ),
        )
    )

    fig.add_trace(
        go.Scattermapbox(
            lat=new_listings['latitude'],
            lon=new_listings['longitude'],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=5,
                color='yellow'
            ),
            hoverinfo='none'
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
        mapbox=dict(
            style='outdoors',
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


if __name__ == "__main__":
    app.run_server(debug=True)
