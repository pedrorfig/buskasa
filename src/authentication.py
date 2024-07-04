from streamlit_google_auth import Authenticate
import socket
import json
from dotenv import load_dotenv
import os

load_dotenv()


def create_google_credentials_file():
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
                "http://localhost:8501",
                "https://bargain-bungalow.streamlit.app/",
            ],
            "javascript_origins": [
                "http://localhost:8501",
                "https://bargain-bungalow.streamlit.app",
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
            local_ip.startswith("127.")
            or local_ip == "localhost"
            or hostname == "SAOX1Y6-58781"
        ):
            return True
        return False
    except Exception as e:
        print(f"Error determining local IP: {e}")
        return False


def get_authenticator():
    
    if is_running_locally():
        redirect_uri = "http://localhost:8501"
    else:
        redirect_uri = "https://bargain-bungalow.streamlit.app"

    create_google_credentials_file()

    authenticator = Authenticate(
        secret_credentials_path="google_credentials.json",
        cookie_name="bargain_bungalow_cookie_name",
        cookie_key="bargain_bungalow_cookie_key",
        redirect_uri=redirect_uri,
    )
    return authenticator
