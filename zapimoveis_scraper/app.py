import numpy as np
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
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
        html.H4("Best Deals in São Paulo"),
        html.P("Neighborhood"),
        dcc.Dropdown(
            id="neighborhood",
            options=results['neighborhood'].unique(),
            value="scatter_mapbox",
            clearable=False,
        ),
        dcc.Graph(figure={}, id="graph")
    ]
)
@app.callback(
    Output("graph", "figure"),
    Input("neighborhood", "value"),
)
def generate_chart(neighborhood, mapbox_token=mapbox_token):

    search_results = results[results['neighborhood']==neighborhood]

    size = 1 / results['price_per_area']

    hover_template = ('<b>%{customdata[0]}</b> <br>' +
                      'Price: R$ %{customdata[1]:,.2f} <br>' +
                      'Price per Area: R$/m<sup>2</sup> %{customdata[2]:,.2f} <br>' +
                      'Condo Fee: R$ %{customdata[3]:,.2f} <br>' +
                      'Usable Area: %{customdata[4]} m<sup>2</sup> <br>' +
                      'Floor: %{customdata[5]}')

    custom_data = np.stack((results['link'], results['price'], results['price_per_area'],
                            results['condo_fee'], results['total_area_m2'], results['floor']),
                           axis=1)
    fig = go.Figure()

    fig.add_trace(
        go.Scattermapbox(
            lat=results['latitude'],
            lon=results['longitude'],
            mode='markers',
            name='',
            customdata=custom_data,
            hovertemplate=hover_template,
            marker=go.scattermapbox.Marker(
                size=size,
                sizemin=5,
                sizeref=0.00001,
                colorscale='plotly3_r',
                color=results['price_per_area'],
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
                lat=search_results['latitude'].mean(),
                lon=search_results['longitude'].mean()
            ),
            pitch=0,
            zoom=15
        ),
    )
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)