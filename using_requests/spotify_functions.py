import base64
import requests
from dotenv import dotenv_values


def get_access_token() -> str:
    """Get access token from Spotify API"""
    # get credentials from .env file
    config = dotenv_values(".env")
    # encode as Base64
    credentials = f"{config['CLIENT_ID']}:{config['CLIENT_SECRET']}"
    credentials_ascii = credentials.encode('ascii')
    credentials_base64 = base64.b64encode(credentials_ascii)
    # make request
    url = 'https://accounts.spotify.com/api/token'
    header = {'Authorization': f'Basic {credentials_base64.decode("ascii")}'}
    data = {'grant_type': 'client_credentials', }
    response = requests.post(url, data=data, headers=header)
    # return access token
    return response.json()['access_token']


def get_track(track_id: str, access_token: str) -> dict:
    """Get track info from Spotify API"""
    url = f'https://api.spotify.com/v1/tracks/{track_id}'
    header = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=header)
    return response.json()


def get_several_tracks(track_ids: list, access_token: str) -> dict:
    """Get several tracks info from Spotify API"""
    url = f'https://api.spotify.com/v1/tracks'
    header = {'Authorization': f'Bearer {access_token}'}
    params = {'ids': ','.join(track_ids)}
    response = requests.get(url, headers=header, params=params)
    return response.json()


def get_audio_features(track_id: str, access_token: str) -> dict:
    """Get audio features from Spotify API"""
    url = f'https://api.spotify.com/v1/audio-features/{track_id}'
    header = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=header)
    return response.json()


def get_several_audio_features(track_ids: list, access_token: str) -> dict:
    """Get several audio features from Spotify API"""
    url = f'https://api.spotify.com/v1/audio-features'
    header = {'Authorization': f'Bearer {access_token}'}
    params = {'ids': ','.join(track_ids)}
    response = requests.get(url, headers=header, params=params)
    return response.json()
