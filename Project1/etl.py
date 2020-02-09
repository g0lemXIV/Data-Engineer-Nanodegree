import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
<<<<<<< HEAD
    """Function process songs files, first
    read json file check if json is not empty file
    and make request to database.
    Parameters
    ----------
    cur: psyconpg2.connect
        cursor of connection
    filepath: str
        path to file as json file
    """
    # name of songs and aritsts talbes as list 
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    artist_data = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']

    # open song file and check is not empty
    df = pd.read_json(filepath, lines=True)
    if not df.empty:
    # insert song record
        song_data = list(df[song_columns].values[0])
        cur.execute(song_table_insert, song_data)
        # insert artist record
        artist_data = list(df[artist_data].values[0])
        cur.execute(artist_table_insert, artist_data)
    else:
        None

def process_log_file(cur, filepath):
    """Function process log files, first
    read json file, query log where page == 'NextSong',
    convert nanoseconds to datetime format and finally
    insert data into tables.
    Parameters
    ----------
    cur: psyconpg2.connect
        cursor of connection
    filepath: str
        path to file as json file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms', exact=True).astype('datetime64[s]')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    # time columns
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
=======
    # open song file
    df = 

    # insert song record
    song_data = 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = 

    # filter by NextSong action
    df = 

    # convert timestamp column to datetime
    t = 
    
    # insert time data records
    time_data = 
    column_labels = 
    time_df = 
>>>>>>> master

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

<<<<<<< HEAD
    # user columns
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']

    # load user table
    user_df = df[user_columns].drop_duplicates(subset=['userId'], keep='first')
=======
    # load user table
    user_df = 
>>>>>>> master

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
<<<<<<< HEAD
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, t[index], row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
=======
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = 
>>>>>>> master
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
<<<<<<< HEAD
    """Function process data with specify function
    Parameters
    ----------
    cur: psyconpg2.connect
        cursor of connection
    conn: psyconpg2.connect.cursor
        connecition function
    filepath: str
        path to main directory of files
    func: function
        python function which process data, function must have input (cur, absolte_path_to_file)
    """
=======
>>>>>>> master
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
<<<<<<< HEAD
    """Connect to database, run all process function and close connection.
    """
=======
>>>>>>> master
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
<<<<<<< HEAD
    print('Successfully insert data into database')
=======

>>>>>>> master
    conn.close()


if __name__ == "__main__":
    main()