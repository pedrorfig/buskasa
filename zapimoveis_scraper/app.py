import copy

import numpy as np
from dash import Dash, dcc, html, Input, Output
import zapimoveis_scraper as zap
import plotly.graph_objects as go
from dotenv import load_dotenv
import os

load_dotenv()
mapbox_token = os.getenv('MAPBOX_TOKEN')

max_price_per_area = 6000
min_price_per_area = 3500

results = zap.filter_results(min_price_per_area, max_price_per_area)

app = Dash(__name__)

app.layout = html.Div(
    [
        html.H1("Best Deals in São Paulo"),
        html.P("Neighborhood"),
        dcc.Dropdown(
            id="neighborhood",
            options=results['neighborhood'].unique(),
            value=results['neighborhood'][0]
        ),
        html.P("Bedrooms"),
        dcc.Dropdown(
            id="bedrooms",
            options=sorted(results['bedrooms'].unique()),
            value=results['bedrooms'][0],
        ),
        html.P("Bathrooms"),
        dcc.Dropdown(
            id="bathrooms",
            options=sorted(results['bathrooms'].unique()),
            value=results['bathrooms'][0]
        ),

        dcc.Graph(figure={}, id="graph")
    ]
)

@app.callback(
    Output("neighborhood", "options"),
    Input('bedrooms', 'value'),
    Input('bathrooms', 'value')
)
def chained_callback_neighborhood(bedrooms, bathrooms):
    dff = copy.deepcopy(results)
    if bedrooms is not None:
        dff = dff.query("bedrooms == @bedrooms")
    if bathrooms is not None:
        dff = dff.query("bathrooms == @bathrooms")
    return sorted(dff["neighborhood"].unique())
@app.callback(
    Output("bedrooms", "options"),
    Input('neighborhood', 'value'),
    Input('bathrooms', 'value')
)
def chained_callback_bedrooms(neighborhood, bathrooms):
    dff = copy.deepcopy(results)
    if neighborhood is not None:
        dff = dff.query("neighborhood == @neighborhood")
    if bathrooms is not None:
        dff = dff.query("bathrooms == @bathrooms")
    return sorted(dff["bedrooms"].unique())
@app.callback(
    Output("bathrooms", "options"),
    Input('neighborhood', 'value'),
    Input('bedrooms', 'value')
)
def chained_callback_bathrooms(neighborhood, bedrooms):
    dff = copy.deepcopy(results)
    if neighborhood is not None:
        dff = dff.query("neighborhood == @neighborhood")
    if bedrooms is not None:
        dff = dff.query("bedrooms == @bedrooms")
    return sorted(dff["bathrooms"].unique())

@app.callback(
    Output("graph", "figure"),
    Input("neighborhood", "value"),
    Input("bedrooms", "value"),
    Input("bathrooms", "value"),
)
def generate_chart(neighborhood, bedrooms, bathrooms, mapbox_token=mapbox_token):
    results_copy = copy.deepcopy(results)

    if neighborhood is not None:
        results_copy = results_copy.query("neighborhood == @neighborhood")

    if bedrooms is not None:
        results_copy = results_copy.query("bedrooms == @bedrooms")

    if bathrooms is not None:
        results_copy = results_copy.query("bathrooms == @bathrooms")

    size = 1 / results_copy['price_per_area']

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
                sizemin=5,
                sizeref=0.00001,
                colorscale='plotly3_r',
                color=results_copy['price_per_area'],
                colorbar=dict(title='Price per Area (R$/m<sup>2</sup>)')
            ),
        )
    )

    fig.update_layout(
        width=1600,
        height=1000,
        hovermode='closest',
        hoverdistance=50,
        hoverlabel=dict(
            bgcolor="white",
            font_size=16,
            font_family="Rockwell"
        ),
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