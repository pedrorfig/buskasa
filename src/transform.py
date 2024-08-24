import textwrap
import pandas as pd
from sqlalchemy import text

import src.extract as extract


def convert_to_dataframe(data):
    """
    Simple function to convert the data from objects to a pandas DataFrame
    Args:
        data (list of ZapItem): Empty default dictionary
    """
    # Iterate through your objects
    obj_data = []
    for obj in data:
        # Get all attributes of the current object using vars()
        object_dict = vars(obj)
        # Append the dictionary to the data list
        obj_data.append(object_dict)
    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(obj_data)
    cols_to_drop = []
    for col in df.columns:
        if col.startswith('_'):
            cols_to_drop.append(col)
    df = df.drop(columns=cols_to_drop)
    return df


def wrap_string_with_fill(text, width):
    wrapped_text = textwrap.fill(text, width)
    return wrapped_text.replace('\n', '<br>')


def define_bounding_box(latitude, longitude, height=0.01, width=0.01):
    # Calculate the minimum and maximum latitude

    latitude_multiplier = latitude // height
    longitude_multiplier = longitude // width

    min_lat = round(latitude_multiplier * height, 3)
    max_lat = round(min_lat + height, 3)

    # Calculate the minimum and maximum longitude
    min_lon = round(longitude_multiplier * width, 3)
    max_lon = round(min_lon + width, 3)

    return min_lat, max_lat, min_lon, max_lon


def calculate_green_density(image):

    if image is not None:
        # Convert the image to RGB mode
        image = image.convert("RGB")

        # Get the size of the image
        width, height = image.size

        # Initialize counters
        total_pixels = width * height
        green_pixels = 0
        # Iterate over each pixel in the image
        for x in range(width):
            for y in range(height):
                # Get the RGB values of the pixel
                r, g, b = image.getpixel((x, y))
                # Check if the pixel is green
                threshold = 10
                if (g > r + threshold):
                    green_pixels += 1

        # Calculate the tree density
        green_density = green_pixels / total_pixels
    else:
        green_density = 0
    return green_density



def group_green_density():
    # Connect to the PostgreSQL database
    engine = extract.create_db_engine()
    with engine.begin() as conn:
        query = text(
            """with quartile_green_density as (select
                        CASE
                            WHEN NTILE(3) OVER (
                            ORDER BY
                                green_density
                            ) = 3 THEN 'Bastante Verde'
                            WHEN NTILE(3) OVER (
                            ORDER BY
                                green_density
                            ) = 2 THEN 'Moderadamente Verde'
                            WHEN NTILE(3) OVER (
                            ORDER BY
                                green_density
                            ) = 1 and is_next_to_park is False THEN 'Pouco Verde'
                            WHEN is_next_to_park is True THEN 'Moderadamente Verde'
                        END as green_density_grouped,
                        listing_id
                        from
                        fact_listings)
                        UPDATE fact_listings SET green_density_grouped = quartile_green_density.green_density_grouped
                        FROM quartile_green_density
                        WHERE fact_listings.listing_id = quartile_green_density.listing_id;
                """)
        conn.execute(query)
    return
def group_n_bus_lanes():
    # Connect to the PostgreSQL database
    engine = extract.create_db_engine()
    with engine.begin() as conn:
        query = text(
            """WITH quartile_n_nearby_bus_lanes AS (
                        select CASE
                                    WHEN NTILE(4) OVER (ORDER BY n_nearby_bus_lanes) = 1 THEN 'Muito Calmo'
                                    WHEN NTILE(4) OVER (ORDER BY n_nearby_bus_lanes) = 2 THEN 'Calmo'
                                    WHEN NTILE(4) OVER (ORDER BY n_nearby_bus_lanes) = 3 THEN 'Movimentado'
                                    WHEN NTILE(4) OVER (ORDER BY n_nearby_bus_lanes) = 4 THEN 'Agitado'
                                END as n_nearby_bus_lanes_grouped,
                                listing_id
                        from fact_listings) 
                    UPDATE fact_listings SET n_nearby_bus_lanes_grouped = quartile_n_nearby_bus_lanes.n_nearby_bus_lanes_grouped
                    FROM quartile_n_nearby_bus_lanes
                    WHERE fact_listings.listing_id = quartile_n_nearby_bus_lanes.listing_id;
                """)
        conn.execute(query)
    return
