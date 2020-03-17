import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
                        songplayID INT IDENTITY(0,1),
                        artist VARCHAR,
                        auth VARCHAR(12),
                        firstName VARCHAR(20),
                        gender VARCHAR(1),
                        ItemInSession INT2,
                        lastName VARCHAR(20),
                        length NUMERIC,
                        level VARCHAR(4),
                        location VARCHAR,
                        method VARCHAR(5),
                        page VARCHAR(20),
                        registration FLOAT,
                        sessionId INT PRIMARY KEY,
                        song VARCHAR,
                        status INT,
                        ts TIMESTAMP,
                        userAgent VARCHAR,
                        userId INT        
                        )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
                        num_songs INT,
                        artist_id VARCHAR(25),
                        artist_latitude FLOAT,
                        artist_longitude FLOAT,
                        artist_location VARCHAR,
                        artist_name VARCHAR,
                        song_id VARCHAR(50) PRIMARY KEY,
                        title VARCHAR,
                        duration NUMERIC,
                        year INT
                            )

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
                        user_id INT NOT NULL PRIMARY KEY,
                        first_name VARCHAR(20),
                        last_name VARCHAR(20),
                        gender VARCHAR(1),
                        level VARCHAR(4)
                      )
diststyle auto;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR(50) NOT NULL PRIMARY KEY sortkey,
                        title VARCHAR,
                        artist_id VARCHAR(25) distkey, 
                        year INT,
                        duration NUMERIC
                      );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
                        artist_id VARCHAR(25) NOT NULL PRIMARY KEY,
                        artist_name VARCHAR sortkey distkey,
                        location VARCHAR,
                        latitude FLOAT,
                        longitude FLOAT
                       );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
                        start_time TIMESTAMP NOT NULL sortkey,
                        hour INT,
                        day INT,
                        week INT,
                        month INT distkey,
                        year INT,
                        weekday INT
                       );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id INT NOT NULL PRIMARY KEY,
                        start_time TIMESTAMP NOT NULL sortkey,
                        user_id INT NOT NULL,
                        level VARCHAR(4),
                        song_id VARCHAR(50),
                        artist_id VARCHAR(25) distkey,
                        session_id INT NOT NULL,
                        location VARCHAR,
                        user_agent VARCHAR,
                        FOREIGN KEY (user_id) REFERENCES users(user_id),
                        FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
                        FOREIGN KEY (song_id) REFERENCES songs(song_id)                       );
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}'
     IAM_ROLE '{}'
     REGION 'us-west-2' 
     COMPUPDATE OFF STATUPDATE OFF
     FORMAT AS JSON '{}'
     TIMEFORMAT 'epochmillisecs'
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

# setting COMPUPDATE, STATUPDATE to speed up COPY

staging_songs_copy = ("""
copy staging_songs from '{}'
     IAM_ROLE '{}'
     REGION 'us-west-2' 
     COMPUPDATE OFF STATUPDATE OFF
     JSON 'auto'
""").format(config.get('S3','SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level,
                       song_id, artist_id, session_id, location, user_agent)
SELECT ste.songplayID AS songplay_id,
       ste.ts AS start_time ,
       ste.userId AS user_id,
       ste.level AS level,
       sts.song_id AS song_id,
       sts.artist_id AS artist_id,
       ste.sessionId AS session_id,
       ste.location AS location,
       ste.userAgent AS user_agent
FROM staging_events ste
INNER JOIN staging_songs sts ON (ste.artist = sts.artist_name)
AND (ste.song = sts.title)
WHERE ste.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT ste.userID AS user_id,
                ste.firstName AS first_name,
                ste.lastName AS last_name,
                ste.gender AS gender,
                ste.level AS level
FROM staging_events ste
WHERE ste.userID IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT sts.song_id AS song_id,
                sts.title AS title,
                sts.artist_id AS artist_id,
                sts.year AS year,
                sts.duration AS duration
FROM staging_songs sts
WHERE sts.song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, location, latitude, longitude)
SELECT sts.artist_id AS artist_id,
       sts.artist_name AS artist_name,
       sts.artist_location AS location,
       sts.artist_latitude AS latitude,
       sts.artist_longitude AS longitude
FROM staging_songs sts
WHERE sts.artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ste.ts AS start_time,
                EXTRACT(hour FROM ste.ts) AS hour,
                EXTRACT(day FROM ste.ts) AS dat,
                EXTRACT(week FROM ste.ts) AS week,
                EXTRACT(month FROM ste.ts) AS month,
                EXTRACT(year FROM ste.ts) AS year,
                EXTRACT(weekday FROM ste.ts) AS weekday
FROM staging_events ste
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, songplay_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
