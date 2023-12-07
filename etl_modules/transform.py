import pandas as pd
import sqlalchemy


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


def filter_first_quartile(listings):
    listings['good_deal'] = listings['price_per_area'] < \
                            listings.groupby(by=['business_type', 'state', 'city', 'neighborhood'])[
                                'price_per_area'].transform(lambda x: x.quantile(0.25))
    listings = listings.loc[listings['good_deal'] == True]
    return listings
