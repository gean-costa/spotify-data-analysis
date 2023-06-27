from mongodb_functions import connect_to_db, get_songs_from_history
from spotify_functions import get_access_token, get_several_tracks, get_several_audio_features

import logging
import math

logging.basicConfig(
    format='[%(levelname)s][%(asctime)s]: %(message)s',
    datefmt='%d/%m/%Y %I:%M:%S %p',
    level=logging.INFO
)


def reshape_list(l: list, n: int = 50) -> list:
    if len(l) > n:
        num_blocks = math.ceil(len(l)/n)
        return [l[(a-1)*n:a*n] for a in range(1, num_blocks+1)]
    return l


def request_insert_songs():

    logging.info('Connecting to MongoDB')
    db = connect_to_db('Spotify')

    logging.info('Getting songs from collection origin: track_history')
    track_ids_from = get_songs_from_history(db)
    track_ids_from = {track_id['track_id'] for track_id in track_ids_from}
    logging.info(f'Found {len(track_ids_from)} songs')

    logging.info('Getting songs from collection destiny: tracks')
    collection_to = db['tracks']
    track_ids_to = list(collection_to.find({}, {'_id': False, 'id': True}))
    track_ids_to = {track_id['id'] for track_id in track_ids_to}
    logging.info(f'Found {len(track_ids_to)} songs')

    logging.info('Getting songs to add')
    track_ids_to_add = track_ids_from - track_ids_to
    logging.info(f'Found {len(track_ids_to_add)} songs to add')

    logging.info('Reshaping list')
    track_ids_to_add = reshape_list(list(track_ids_to_add), n=50)
    logging.info(f'Found {len(track_ids_to_add)} blocks')

    logging.info('Getting access token')
    access_token = get_access_token()

    logging.info('Adding songs to collection destiny')
    for i, track_ids in enumerate(track_ids_to_add):
        logging.info(f'Block {i+1}/{len(track_ids_to_add)}')
        several_tracks = get_several_tracks(
            track_ids=track_ids, access_token=access_token)
        collection_to.insert_many(several_tracks['tracks'])
        logging.info(f'Added {len(several_tracks["tracks"])} songs')

    logging.info('Done')


def request_insert_audio_features():

    logging.info('Connecting to MongoDB')
    db = connect_to_db('Spotify')

    logging.info('Getting songs from collection origin: track_history')
    track_ids_from = get_songs_from_history(db)
    track_ids_from = {track_id['track_id'] for track_id in track_ids_from}
    logging.info(f'Found {len(track_ids_from)} songs')

    logging.info('Getting songs from collection destiny: audio_features')
    collection_to = db['audio_features']
    track_ids_to = list(collection_to.find({}, {'_id': False, 'id': True}))
    track_ids_to = {track_id['id'] for track_id in track_ids_to}
    logging.info(f'Found {len(track_ids_to)} songs')

    logging.info('Getting songs to add')
    track_ids_to_add = track_ids_from - track_ids_to
    logging.info(f'Found {len(track_ids_to_add)} songs to add')

    logging.info('Reshaping list')
    track_ids_to_add = reshape_list(list(track_ids_to_add), n=100)
    logging.info(f'Found {len(track_ids_to_add)} blocks')

    logging.info('Getting access token')
    access_token = get_access_token()

    logging.info('Adding songs to collection destiny')
    for i, track_ids in enumerate(track_ids_to_add):
        logging.info(f'Block {i+1}/{len(track_ids_to_add)}')
        several_audio_features = get_several_audio_features(
            track_ids=track_ids, access_token=access_token)
        collection_to.insert_many(several_audio_features['audio_features'])
        logging.info(
            f'Added {len(several_audio_features["audio_features"])} songs')

    logging.info('Done')


if __name__ == '__main__':
    # request_insert_songs()
    request_insert_audio_features()
