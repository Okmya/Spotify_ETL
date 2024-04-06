import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
import json
import pandas as pd

# TODO: zrobic to w pliku tekstowym
SPOTIPY_CLIENT_ID = "390e4a89e62040d0b112eee8500018d0"
SPOTIPY_CLIENT_SECRET = "72ef35acd4974b97a62f0b686682b432"
SPOTIPY_REDIRECT_URL = "http://localhost:6060/callback"

scope = "user-read-recently-played"

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        scope=scope,
        client_id=SPOTIPY_CLIENT_ID,
        client_secret=SPOTIPY_CLIENT_SECRET,
        redirect_uri=SPOTIPY_REDIRECT_URL,
    )
)

recently_played = sp.current_user_recently_played(limit=50, after=None)
# print(results)
# for idx, item in enumerate(recently_played["items"]):
#    track = item["track"]
#    print(idx, track["artists"][0]["name"], " – ", track["name"])
# for item in recently_played["items"]:
#    item["track"]["album"].pop("available_markets", None)
#    item["track"].pop("available_markets", None)

song_name = []
artist_name = []
played_at = []
timestamp = []


# extracting data
for item in recently_played["items"]:
    song_name.append(item["track"]["name"])
    artist_name.append(item["track"]["artists"][0]["name"])
    played_at.append(item["played_at"])
    timestamp.append(item["played_at"][0:10])


# dictionary
spotify_songs_dictionary = {
    "song_name": song_name,
    "artist_name": artist_name,
    "played_at": played_at,
    "timestamp": timestamp,
}

df = pd.DataFrame(
    spotify_songs_dictionary,
    columns=["song_name", "artist_name", "played_at", "timestamp"],
)

# write df to csv file
df.to_csv("spotify_recently_played_data.csv")


with open("spotify_recently_played_data.json", "w", encoding="utf-8") as file:
    json.dump(recently_played, file, ensure_ascii=False, indent=4)


# TODO: zaladowac dane do df z pyspark
