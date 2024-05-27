import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sys
from io import StringIO
import os
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import uuid
import check_data


def get_albums(recently_played):
    album_list = []
    for item in recently_played["items"]:
        album_id = item["track"]["album"]["id"]
        album_name = item["track"]["album"]["name"]
        album_release_date = item["track"]["album"]["release_date"]
        album_total_tracks = item["track"]["album"]["total_tracks"]
        album_url = item["track"]["album"]["external_urls"]["spotify"]

        album = {
            "album_id": album_id,
            "name": album_name,
            "release_date": album_release_date,
            "total_tracks": album_total_tracks,
            "url": album_url,
        }

        album_list.append(album)
    return album_list


def get_artists(recently_played):
    artist_list = []
    for item in recently_played["items"]:
        # petla po liscie artystow w albumie
        for artist_info in item["track"]["album"]["artists"]:
            artist_id = artist_info["id"]
            artist_name = artist_info["name"]
            artist_url = artist_info["external_urls"]["spotify"]

            artist = {"artist_id": artist_id, "name": artist_name, "url": artist_url}

            artist_list.append(artist)
    return artist_list


def get_songs(recently_played):
    song_list = []
    for item in recently_played["items"]:
        song_id = item["track"]["id"]
        song_name = item["track"]["name"]
        song_popularity = item["track"]["popularity"]
        song_duration = item["track"]["duration_ms"]
        song_url = item["track"]["external_urls"]["spotify"]
        song_played_at = item["played_at"]
        album_id = item["track"]["album"]["id"]
        # artist_id = item["track"]["artists"]["id"]
        artist_id = item["track"]["album"]["artists"][0]["id"]

        song = {
            "song_id": song_id,
            "song_name": song_name,
            "popularity": song_popularity,
            "duration_ms": song_duration,
            "time_played": song_played_at,
            "url": song_url,
            "artist_id": artist_id,
            "album_id": album_id,
        }

        song_list.append(song)
    return song_list


def etl_spotify():

    # dane do api spotify
    SPOTIPY_CLIENT_ID = "390e4a89e62040d0b112eee8500018d0"
    SPOTIPY_CLIENT_SECRET = "72ef35acd4974b97a62f0b686682b432"
    SPOTIPY_REDIRECT_URL = "http://localhost:6060/callback"

    # scope po jakim szukamy
    scope = "user-read-recently-played"

    # polaczenie sie z api
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            scope=scope,
            client_id=SPOTIPY_CLIENT_ID,
            client_secret=SPOTIPY_CLIENT_SECRET,
            redirect_uri=SPOTIPY_REDIRECT_URL,
        )
    )

    recently_played = sp.current_user_recently_played(limit=50, after=None)

    # create albums list
    album_list = get_albums(recently_played)
    check_data.check_date(album_list)
    #print(album_list)
    # create artists list
    artist_list = get_artists(recently_played)

    # create song list
    song_list = get_songs(recently_played)

    return album_list, artist_list, song_list

    # Zapis do tych danych na razie do pliku zeby je zbierac
    # TODO: pozmieniac te etl pod pysparka
    # TODO: pobierac dane na plikow csv, zrobic pyspark df, zapisac do postgresa


etl_spotify()
