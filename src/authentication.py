from src.streamlit_google_auth import Authenticate
import socket
import json
import streamlit as st
from dotenv import load_dotenv
import os

load_dotenv()

def create_google_credentials_file(redirect_uri):
    # Data to be written
    dictionary = {
        "web": {
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "project_id": os.environ["GOOGLE_PROJECT_ID"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
            "redirect_uris": [
                'https://bargain-bungalow.streamlit.app',
                'http://localhost:8501'
            ],
            "javascript_origins": [
                'https://bargain-bungalow.streamlit.app',
                'http://localhost:8501'
            ],
        }
    }
    with open("google_credentials.json", "w") as outfile:
        json.dump(dictionary, outfile)


def is_running_locally() -> bool:
    try:
        # Get the hostname of the machine
        hostname = socket.gethostname()
        # Get the IP address of the machine
        local_ip = socket.gethostbyname(hostname)
        # Check if the IP address is in the range of local IP addresses
        if (
            local_ip.startswith("192.")
            or hostname == "SAOX1Y6-58781"
        ):
            st.write(local_ip, hostname)
            return True
        return False
    except Exception as e:
        print(f"Error determining local IP: {e}")
        return False


def create_redirect_uri() -> str:
    if is_running_locally():
        redirect_uri = "http://localhost:8501"
    else:
        redirect_uri = "https://bargain-bungalow.streamlit.app"
    return redirect_uri


def get_authenticator():
    redirect_uri = create_redirect_uri()
    st.write(redirect_uri)
    create_google_credentials_file(redirect_uri)
    authenticator = Authenticate(
        secret_credentials_path="google_credentials.json",
        cookie_name="bargain_bungalow_cookie_name",
        cookie_key="bargain_bungalow_cookie_key",
        redirect_uri=redirect_uri,
    )
    return authenticator


def initialize_connected_as_guest_state():
    if "connected_as_guest" not in st.session_state:
        st.session_state["connected_as_guest"] = False


def create_login_modal(authenticator):
    @st.experimental_dialog("Login", width="large")
    def login():
        authenticator.login()
        st.markdown(
            f"""
      <style>
      [class="row-widget stButton"]{{
          display: flex;
          justify-content: center}}
      [class="st-emotion-cache-1vt4y43 ef3psqc12"]{{
            padding: 8px 12px;
            border-radius: 4px;
            display: flex;
            align-items: center;
            text-decoration: none;
            font-size: 10px;
            background-color: gray;
            color: white;
            cursor:pointer
      }};
      [class="st-emotion-cache-1vt4y43 ef3psqc12:hover"]{{
            background-color: gray;
      }};
      </style>
    """,
            unsafe_allow_html=True,
        )
        st.session_state["connected_as_guest"] = st.button(":white[Entrar como convidado]", type='secondary')
        if st.session_state["connected_as_guest"]:
            st.rerun()

    if not st.session_state["connected_as_guest"] and not st.session_state["connected"]:
        login()
