import os

import streamlit as st
import src.authentication as authentication
import src.visualization as visualization
from dotenv import load_dotenv

load_dotenv()

mapbox_token = os.environ["MAPBOX_TOKEN"]

class App:
    """
    App object
    """

    def __init__(self):