# Thanks mkhorasani for his authentification package that I used to build this
# https://github.com/mkhorasani/Streamlit-Authenticator

import json
import os
import socket
import time
from typing import Literal

import google_auth_oauthlib.flow
import streamlit as st
from dotenv import load_dotenv
from googleapiclient.discovery import build

from .cookie import CookieHandler

load_dotenv()

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


class Authenticate:
    def __init__(
        self,
        secret_credentials_path: str,
        cookie_name: str,
        cookie_key: str,
        redirect_uri: str = create_redirect_uri(),
        cookie_expiry_days: float = 30.0,
    ):
        st.session_state["connected"] = st.session_state.get("connected", False)
        self.secret_credentials_path = secret_credentials_path
        self.redirect_uri = redirect_uri
        self.cookie_handler = CookieHandler(cookie_name, cookie_key, cookie_expiry_days)


    def create_google_credentials_file(self):
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
                    os.environ['APP_REDIRECT_URI'],
                    'http://localhost:8501'
                ],
                "javascript_origins": [
                    os.environ['APP_REDIRECT_URI'],
                    'http://localhost:8501'
                ],
            }
        }
        with open("google_credentials.json", "w") as outfile:
            json.dump(dictionary, outfile)


    def login(
        self, color: Literal["white", "blue"] = "blue", justify_content: str = "center"
    ) -> tuple:
        if not st.session_state["connected"]:
            authorization_url = self.get_authorization_url()

            html_content = f"""
                            <div style="display: flex; justify-content: {justify_content};">
                                <a href="{authorization_url}" target="_blank" style="background-color: {'#fff' if color == 'white' else '#4285f4'}; color: {'#000' if color == 'white' else '#fff'}; text-decoration: none; text-align: center; font-size: 16px; margin: 4px 2px; cursor: pointer; padding: 8px 12px; border-radius: 4px; display: flex; align-items: center;">
                                    <img src="https://lh3.googleusercontent.com/COxitqgJr1sJnIDe8-jiKhxDx1FrYbtRHKJ9z_hELisAlapwE9LUPh6fcXIfb5vwpbMl4xl9H9TRFPc5NOO8Sb3VSgIBrfRYvW6cUA" alt="Google logo" style="margin-right: 8px; width: 26px; height: 26px; background-color: white; border: 2px solid white; border-radius: 4px;">
                                    Entrar com Google
                                </a>
                            </div>
                            """

            st.markdown(html_content, unsafe_allow_html=True)
            

    def get_authorization_url(self) -> str:
        flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
            self.secret_credentials_path,  # replace with you json credentials from your google auth app
            scopes=[
                "openid",
                "https://www.googleapis.com/auth/userinfo.profile",
                "https://www.googleapis.com/auth/userinfo.email",
            ],
            redirect_uri=self.redirect_uri,
        )

        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
        )
        return authorization_url
    
    def initialize_connected_as_guest_state(self):
        if "connected_as_guest" not in st.session_state:
            st.session_state["connected_as_guest"] = False
    
    def create_login_modal(self):
        @st.experimental_dialog("Login", width="large")
        def login():
            self.login()
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



    def check_authentification(self):
        if not st.session_state["connected"]:
            token = self.cookie_handler.get_cookie()
            if token:
                user_info = {
                    "name": token["name"],
                    "email": token["email"],
                    "picture": token["picture"],
                    "id": token["oauth_id"],
                }
                st.query_params.clear()
                st.session_state["connected"] = True
                st.session_state["user_info"] = user_info
                st.session_state["oauth_id"] = user_info.get("id")
                return

            time.sleep(0.3)

            if not st.session_state["connected"]:
                auth_code = st.query_params.get("code")
                st.query_params.clear()
                if auth_code:
                    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
                        self.secret_credentials_path,  # replace with you json credentials from your google auth app
                        scopes=[
                            "openid",
                            "https://www.googleapis.com/auth/userinfo.profile",
                            "https://www.googleapis.com/auth/userinfo.email",
                        ],
                        redirect_uri=self.redirect_uri,
                    )
                    flow.fetch_token(code=auth_code)
                    credentials = flow.credentials
                    user_info_service = build(
                        serviceName="oauth2",
                        version="v2",
                        credentials=credentials,
                    )
                    user_info = user_info_service.userinfo().get().execute()

                    st.session_state["connected"] = True
                    st.session_state["oauth_id"] = user_info.get("id")
                    st.session_state["user_info"] = user_info
                    self.cookie_handler.set_cookie(
                        user_info.get("name"),
                        user_info.get("email"),
                        user_info.get("picture"),
                        user_info.get("id"),
                    )
                    st.rerun()

    def logout(self):
        st.session_state["logout"] = True
        st.session_state["name"] = None
        st.session_state["username"] = None
        st.session_state["connected"] = None
        self.cookie_handler.delete_cookie()
