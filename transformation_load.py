import spotipy
from spotipy.oauth2 import SpotifyOAuth
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp, expr
import pandas as pd
import uuid
import data_extraction
from sqlalchemy import create_engine
from sqlalchemy import text

# pip install sqlalchemy
# pip install pyspark spotipy
# pip install pyspark psycopg2-binary sqlalchemy

# docker exec -it spark /bin/bash


def spark_transformation():
    # create spark session
    spark = (
        SparkSession.builder.appName("Spark Data Transformation")
        .config("spark.driver.extraClassPath", "postgresql-42.7.3.jar")
        .config("spark.executor.extraClassPath", "postgresql-42.7.3.jar")
        .getOrCreate()
    )

    # get data from data_extraction script
    album_list, artist_list, song_list = data_extraction.etl_spotify()

    # create spark df
    album_df = spark.createDataFrame(album_list).dropDuplicates(["album_id"])
    artist_df = spark.createDataFrame(artist_list).dropDuplicates(["artist_id"])
    song_df = spark.createDataFrame(song_list)

    # add timestamp and timezone
    song_df = song_df.withColumn("time_played", to_timestamp(col("time_played")))
    song_df = song_df.withColumn(
        "time_played", from_utc_timestamp(col("time_played"), "Europe/Warsaw")
    )

    # change format of time_played
    song_df = song_df.withColumn(
        "time_played", expr("date_format(time_played, 'yyyy-MM-dd HH:mm:ss')")
    )

    # convert to UNIX timestamp
    song_df = song_df.withColumn("UNIX_Time_Stamp", expr("unix_timestamp(time_played)"))

    # generate unique id
    song_df = song_df.withColumn("unique_id", expr("uuid()"))

    song_df = song_df.select(
        "unique_id",
        "song_id",
        "song_name",
        "duration_ms",
        "url",
        "popularity",
        "time_played",
        "album_id",
        "artist_id",
    )

    # song_df.show(10)
    # album_df.show(10)

    # configuration of jdbc connection with PostgreSQL
    jdbc_url = "jdbc:postgresql://db_postgres:5432/spotify_database"
    connection_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver",
        "truncate": "true",
    }

    # Connect to PostgreSQL using SQLAlchemy to execute SQL queries
    engine = create_engine(
        f"postgresql+psycopg2://admin:admin@db_postgres:5432/spotify_database"
    )

    connection = engine.connect()

    # delete table if exists
    connection.execute(text("DROP TABLE IF EXISTS tmp_song"))
    connection.execute(text("DROP TABLE IF EXISTS tmp_album"))
    connection.execute(text("DROP TABLE IF EXISTS tmp_artist"))
    connection.execute(
        text(
            """CREATE TABLE IF NOT EXISTS tmp_song (
            unique_id TEXT PRIMARY KEY,
            song_id TEXT,
            song_name TEXT,
            duration_ms INT,
            url TEXT,
            popularity INT,
            time_played TEXT,
            album_id TEXT,
            artist_id TEXT
        )
    """
        )
    )
    connection.execute(
        text(
            """CREATE TABLE IF NOT EXISTS tmp_album (
            album_id TEXT PRIMARY KEY,
            name TEXT,
            release_date TEXT,
            total_tracks INT,
            url TEXT
        )
    """
        )
    )
    connection.execute(
        text(
            """CREATE TABLE IF NOT EXISTS tmp_artist (
            artist_id TEXT PRIMARY KEY,
            name TEXT,
            url TEXT
        )
    """
        )
    )

    connection.commit()
    # write data to PostgreSQL
    song_df.write.jdbc(
        url=jdbc_url,
        table="tmp_song",
        mode="overwrite",
        properties=connection_properties,
    )
    album_df.write.jdbc(
        url=jdbc_url,
        table="tmp_album",
        mode="overwrite",
        properties=connection_properties,
    )
    artist_df.write.jdbc(
        url=jdbc_url,
        table="tmp_artist",
        mode="overwrite",
        properties=connection_properties,
    )

    # execute SQL queries to move data from temporary tables to target tables
    # Move data from tmp_song to song
    connection.execute(
        text(
            """INSERT INTO song (unique_id,song_id, song_name, duration_ms, url, popularity, time_played, album_id, artist_id)
        SELECT tmp.unique_id,tmp.song_id, tmp.song_name, tmp.duration_ms, tmp.url, tmp.popularity, tmp.time_played::timestamp, tmp.album_id, tmp.artist_id
        FROM tmp_song tmp
        LEFT JOIN song s ON tmp.song_id = s.song_id AND tmp.time_played::timestamp = s.time_played
        WHERE s.song_id IS NULL;
    """
        )
    )

    # Move data from tmp_album to album
    connection.execute(
        text(
            """INSERT INTO album (album_id, name, release_date, total_tracks, url)
        SELECT DISTINCT tmp.album_id, tmp.name, tmp.release_date::date, tmp.total_tracks, tmp.url
        FROM tmp_album tmp
        LEFT JOIN album a ON tmp.album_id = a.album_id
        WHERE a.album_id IS NULL;
    """
        )
    )

    # Move data from tmp_artist to artist
    connection.execute(
        text(
            """INSERT INTO artist (artist_id, name, url)
        SELECT DISTINCT tmp.artist_id, tmp.name, tmp.url
        FROM tmp_artist tmp
        LEFT JOIN artist a ON tmp.artist_id = a.artist_id
        WHERE a.artist_id IS NULL;
    """
        )
    )

    # Drop temporary tables
    connection.execute(text("DROP TABLE IF EXISTS tmp_song"))
    connection.execute(text("DROP TABLE IF EXISTS tmp_album"))
    connection.execute(text("DROP TABLE IF EXISTS tmp_artist"))

    connection.commit()

    print("Zakonczenie procesu ETL")

    # close connection
    spark.stop()


spark_transformation()
