from pymongo import MongoClient
from dotenv import dotenv_values
from using_requests.mongodb_pipelines import * 


def connect_to_db(db_name: str) -> MongoClient:
    """Connect to MongoDB"""
    uri = dotenv_values(".env")['MONGODB_URI']
    client = MongoClient(uri)
    return client[db_name]


def get_tracks_history(db: MongoClient) -> list:
    """Get data from get_tracks_history view"""
    tracks = db.tracks_history.aggregate(tracks_history)
    return list(tracks)


def get_songs_from_history(db: MongoClient) -> list:
    """Get data from get_songs_tracks_history view"""
    songs = db.tracks_history.aggregate(songs_tracks_history)
    return list(songs)


def get_artists_from_history(db: MongoClient) -> list:
    """Get data from get_artists_tracks_history view"""
    artists = db.tracks_history.aggregate(artists_tracks_history)
    return list(artists)


def get_minutes_per_day(db: MongoClient) -> list:
    """Get data from get_minutes_per_day view"""
    minutes = db.tracks_history.aggregate(minutes_per_day)
    return list(minutes)


def get_minutes_per_hour(db: MongoClient) -> list:
    """Get data from get_minutes_per_hour view"""
    minutes = db.tracks_history.aggregate(minutes_per_hour)
    return list(minutes)
