tracks_history = [
    {
        '$match': {
            'spotify_episode_uri': None
        }
    }, {
        '$project': {
            '_id': 0, 
            'artist': '$master_metadata_album_artist_name', 
            'album': '$master_metadata_album_album_name', 
            'track': '$master_metadata_track_name', 
            'spotify_track_uri': '$spotify_track_uri', 
            'datetime_start_track': {
                '$dateSubtract': {
                    'startDate': {
                        '$dateFromString': {
                            'dateString': '$ts'
                        }
                    }, 
                    'unit': 'millisecond', 
                    'amount': '$ms_played'
                }
            }, 
            'datetime_end_track': {
                '$dateFromString': {
                    'dateString': '$ts'
                }
            }, 
            'reason_start_track': '$reason_start', 
            'reason_end_track': '$reason_end', 
            'skipped': '$skipped', 
            'shuffle': '$shuffle'
        }
    }
]

artists_tracks_history = [
    {
        '$match': {
            'master_metadata_album_artist_name': {
                '$ne': None
            }
        }
    }, {
        '$sortByCount': '$master_metadata_album_artist_name'
    }, {
        '$project': {
            '_id': 0, 
            'artist': '$_id', 
            'count': '$count'
        }
    }
]

songs_tracks_history = [
    {
        '$match': {
            'spotify_track_uri': {
                '$ne': None
            }
        }
    }, {
        '$group': {
            '_id': {
                'artist': '$master_metadata_album_artist_name', 
                'track': '$master_metadata_track_name', 
                'track_uri': '$spotify_track_uri'
            }, 
            'qtde': {
                '$count': {}
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'artist': '$_id.artist', 
            'track': '$_id.track', 
            'track_uri': '$_id.track_uri', 
            'track_id': {
                '$substr': [
                    '$_id.track_uri', 14, -1
                ]
            }, 
            'count': '$qtde'
        }
    }, {
        '$sort': {
            'count': -1
        }
    }
]

minutes_per_hour = [
    {
        '$match': {
            'master_metadata_track_name': {
                '$ne': None
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'datetime_start_track': {
                '$dateSubtract': {
                    'startDate': {
                        '$dateFromString': {
                            'dateString': '$ts'
                        }
                    }, 
                    'unit': 'millisecond', 
                    'amount': '$ms_played'
                }
            }, 
            'datetime_end_track': {
                '$dateFromString': {
                    'dateString': '$ts'
                }
            }
        }
    }, {
        '$project': {
            'datetime_track': {
                '$dateFromParts': {
                    'year': {
                        '$year': {
                            'date': '$datetime_start_track'
                        }
                    }, 
                    'month': {
                        '$month': {
                            'date': '$datetime_start_track'
                        }
                    }, 
                    'day': {
                        '$dayOfMonth': {
                            'date': '$datetime_start_track'
                        }
                    }, 
                    'hour': {
                        '$hour': {
                            'date': '$datetime_start_track'
                        }
                    }
                }
            }, 
            'time_played_track': {
                '$dateDiff': {
                    'startDate': '$datetime_start_track', 
                    'endDate': '$datetime_end_track', 
                    'unit': 'second'
                }
            }
        }
    }, {
        '$group': {
            '_id': '$datetime_track', 
            'qtde': {
                '$sum': '$time_played_track'
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'datetime_track': '$_id', 
            'total_minutes': {
                '$round': [
                    {
                        '$divide': [
                            '$qtde', 60
                        ]
                    }, 1
                ]
            }
        }
    }, {
        '$sort': {
            'datetime_track': 1
        }
    }
]

minutes_per_day = [
    {
        '$match': {
            'master_metadata_track_name': {
                '$ne': None
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'datetime_start_track': {
                '$dateSubtract': {
                    'startDate': {
                        '$dateFromString': {
                            'dateString': '$ts'
                        }
                    }, 
                    'unit': 'millisecond', 
                    'amount': '$ms_played'
                }
            }, 
            'datetime_end_track': {
                '$dateFromString': {
                    'dateString': '$ts'
                }
            }
        }
    }, {
        '$project': {
            'datetime_track': {
                '$dateFromParts': {
                    'year': {
                        '$year': {
                            'date': '$datetime_start_track'
                        }
                    }, 
                    'month': {
                        '$month': {
                            'date': '$datetime_start_track'
                        }
                    }, 
                    'day': {
                        '$dayOfMonth': {
                            'date': '$datetime_start_track'
                        }
                    }
                }
            }, 
            'time_played_track': {
                '$dateDiff': {
                    'startDate': '$datetime_start_track', 
                    'endDate': '$datetime_end_track', 
                    'unit': 'second'
                }
            }
        }
    }, {
        '$group': {
            '_id': '$datetime_track', 
            'qtde': {
                '$sum': '$time_played_track'
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'datetime_track': '$_id', 
            'weekday': {
                '$dayOfWeek': '$_id'
            }, 
            'total_minutes': {
                '$round': [
                    {
                        '$divide': [
                            '$qtde', 60
                        ]
                    }, 1
                ]
            }
        }
    }, {
        '$sort': {
            'datetime_track': 1
        }
    }
]

