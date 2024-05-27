--Song Table as fact table

CREATE TABLE IF NOT EXISTS song (
    unique_id TEXT PRIMARY KEY,
    song_id TEXT NOT NULL,
    song_name TEXT,
    duration_ms INT,
    url TEXT,
    popularity INT,
    time_played TIMESTAMP,
    album_id TEXT,
    artist_id TEXT,
    time_insterted TIMESTAMP DEFAULT NOW()
);

--Album table as dimension table for songs 

CREATE TABLE IF NOT EXISTS album(
    album_id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    release_date DATE,
    total_tracks INT,
    url TEXT
);


CREATE TABLE IF NOT EXISTS artist(
    artist_id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    url TEXT
);