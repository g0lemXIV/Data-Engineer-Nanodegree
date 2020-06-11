# coding=utf-8

"""
This is an query list for staging and inserting podcasts data.

Action is as follows:
- Drop tables if exists;
- Create new empty tables;
- Staging data from S3;
- Inserting data into start schema;
"""
import config
import os

# DROP TABLES
staging_episodes_table_drop = "DROP TABLE IF EXISTS staging_episodes"
staging_podcasts_table_drop = "DROP TABLE IF EXISTS staging_podcasts"
author_drop = "DROP TABLE IF EXISTS author CASCADE"
podcasts_drop = "DROP TABLE IF EXISTS podcast CASCADE"
rating_drop = "DROP TABLE IF EXISTS rating CASCADE"
podcastplay_drop = "DROP TABLE IF EXISTS podcastplay CASCADE"
episode_drop = "DROP TABLE IF EXISTS episode CASCADE"


# CREATE TABLES
staging_podcast_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_podcasts (
                        podcastID INT IDENTITY(0,1),
                        authorID INT,
                        Name VARCHAR,
                        Rating_Volume INT,
                        Rating REAL,
                        Genre VARCHAR,
                        Description VARCHAR(max),
                        Spotify_ranking REAL,
                        feed_url VARCHAR,
                        country VARCHAR(4),
                        collectionName VARCHAR,
                        trackId BIGINT,
                        primaryGenreName VARCHAR(30),
                        currency VARCHAR(4),
                        language VARCHAR,
                        owner_name VARCHAR,
                        owner_email VARCHAR,
                        podcast_count INT
                        )
""")

staging_episodes_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_episodes (
                        guid INT NOT NULL PRIMARY KEY,
                        comments_url VARCHAR,
                        description VARCHAR(max),
                        cc VARCHAR,
                        file_size FLOAT,
                        duration_itunes FLOAT,
                        published_date TIMESTAMP,
                        title VARCHAR(max),
                        feed_url VARCHAR
                        )
""")


author_table_create = ("""
CREATE TABLE IF NOT EXISTS author (
                        authorID INT NOT NULL PRIMARY KEY,
                        podcast_name VARCHAR,
                        owner_name VARCHAR,
                        owner_email VARCHAR,
                        country VARCHAR(4),
                        language VARCHAR distkey,
                        currency VARCHAR(4)
                        )
""")

podcast_table_create = ("""
CREATE TABLE IF NOT EXISTS podcast (
                        podcastID INT NOT NULL PRIMARY KEY,
                        podcast_name VARCHAR distkey,
                        description VARCHAR(max),
                        genre VARCHAR sortkey,
                        itunes_genre VARCHAR,
                        apple_trackID BIGINT,
                        feed_url VARCHAR,
                        FOREIGN KEY (podcastID) REFERENCES rating(podcastID)
                        )
""")

rating_table_create = ("""
CREATE TABLE IF NOT EXISTS rating (
                        podcastID INT NOT NULL PRIMARY KEY,
                        itunes_rating REAL,
                        spotify_rating REAL,
                        rating_volume INT sortkey
                        )
diststyle auto;
""")

episodes_table_create = ("""
CREATE TABLE IF NOT EXISTS episode (
                        episodeid INT NOT NULL PRIMARY KEY,
                        title VARCHAR(max),
                        description VARCHAR(max),
                        duration_itunes FLOAT,
                        file_size FLOAT,
                        comments_url VARCHAR,
                        licence VARCHAR,
                        relised_date TIMESTAMP sortkey
                        )
diststyle auto;
""")

podcastplay_table_create = ("""
CREATE TABLE IF NOT EXISTS podcastplay (
                        podcastplayID INT IDENTITY(0,1) PRIMARY KEY,
                        authorid INT,
                        podcastid INT,
                        episodeid INT,
                        itunesid BIGINT,
                        duration FLOAT,
                        file_size FLOAT,
                        relised_date TIMESTAMP sortkey,
                        FOREIGN KEY (authorid) REFERENCES author(authorID),
                        FOREIGN KEY (podcastid) REFERENCES podcast(podcastID),
                        FOREIGN KEY (episodeid) REFERENCES episode(episodeID)
                        )
diststyle auto;
""")

# STAGING TABLES

staging_podcast_copy = ("""
copy staging_podcasts from '{}'
     access_key_id '{}'
     secret_access_key '{}'
     COMPUPDATE OFF STATUPDATE OFF
     JSON 'auto'
""").format(os.getenv('S3_PODCASTS'),
            os.getenv('AWS_ACCESS_KEY_ID'),
            os.getenv('AWS_SECRET_ACCESS_KEY'))

# setting COMPUPDATE, STATUPDATE to speed up COPY

staging_episode_copy = ("""
copy staging_episodes from '{}'
     access_key_id '{}'
     secret_access_key '{}'
     COMPUPDATE OFF STATUPDATE OFF
     JSON 'auto'
     TIMEFORMAT 'auto'
""").format(os.getenv('S3_EPISODES'),
            os.getenv('AWS_ACCESS_KEY_ID'),
            os.getenv('AWS_SECRET_ACCESS_KEY'))

# FINAL TABLES

author_insert = ("""
INSERT INTO author (authorid, podcast_name, owner_name,
                    owner_email, country, language, currency)
SELECT stp.authorID as authorid,
       stp.collectionName as podcast_name,
       stp.owner_name as owner_name,
       stp.owner_email as owner_email,
       stp.country as country,
       stp.language as language,
       stp.currency as currency
FROM staging_podcasts stp
WHERE stp.authorID IS NOT NULL;
""")

podcast_insert = ("""
INSERT INTO podcast (podcastid, podcast_name, genre, itunes_genre,
                     apple_trackid, feed_url, description)
SELECT stp.podcastid as podcastid,
       stp.collectionName as podcast_name,
       stp.genre as genre,
       stp.primaryGenreName as itunes_genre,
       stp.trackid as apple_trackid,
       stp.feed_url as feed_url,
       stp.description as description
FROM staging_podcasts stp
WHERE stp.feed_url IS NOT NULL;
""")

rating_insert = ("""
INSERT INTO rating (podcastID, itunes_rating, spotify_rating,
                    rating_volume)
SELECT stp.podcastid as podcastid,
       stp.rating_volume as itunes_rating,
       stp.spotify_ranking as spotify_rating,
       stp.rating_volume as rating_volume
FROM staging_podcasts stp;
""")

episode_insert = ("""
INSERT INTO episode (episodeid, title, description, duration_itunes,
                    file_size, comments_url, licence, relised_date)
SELECT ste.guid as episodeid,
       ste.title as title,
       ste.description as description,
       ste.duration_itunes as duration_itunes,
       ste.file_size as file_size,
       ste.comments_url as comments_url,
       ste.cc as licence,
       ste.published_date as relised_date
FROM staging_episodes ste;
""")

podcastplay_insert = ("""
INSERT INTO podcastplay (authorid, podcastid, file_size, duration,
                         episodeid, itunesid, relised_date)
SELECT stp.authorID as authorid,
       stp.podcastid as podcastid,
       ste.file_size as file_size,
       ste.duration_itunes as duration,
       ste.guid as episodeid,
       stp.trackid as itunesid,
       ste.published_date as relised_date
FROM staging_podcasts stp
INNER JOIN staging_episodes ste ON (stp.feed_url = ste.feed_url)
""")


# QUERY LISTS
create_table_queries = [staging_podcast_table_create, staging_episodes_table_create,
                        author_table_create, rating_table_create, podcast_table_create,
                        episodes_table_create, podcastplay_table_create]
drop_table_queries = [staging_episodes_table_drop, staging_podcasts_table_drop,
                      author_drop, podcasts_drop, rating_drop, episode_drop,
                      podcastplay_drop]
copy_table_queries = [staging_podcast_copy, staging_episode_copy]
insert_table_queries = [author_insert, podcast_insert, rating_insert,
                        episode_insert, podcastplay_insert]
