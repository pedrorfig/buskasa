import textwrap

import pandas as pd


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