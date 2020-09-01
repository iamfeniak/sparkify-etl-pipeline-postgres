import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    The Function accepts files in JSON format, reads song details and artist details; 
    finally inserts records to songs and artists tables.

    Args:
        cur: Database cursor.
        filepath: JSON file's location.

    Returns:
        None

    """
    # open song file
    df = pd.read_json(filepath, typ="series")

    # insert song record
    song_data = df.filter(items=['song_id', 'title', 'artist_id', 'year', 'duration']).values
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.filter(items=['artist_id', 'artist_name', 'artist_location', 
                               'artist_latitude', 'artist_longitude']).values
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    The Function accepts files in JSON format, reads and filters logs; 
    finally it inserts records to times, users and songplays tables.

    Args:
        cur: Database cursor.
        filepath: JSON file's location.

    Returns:
        None

    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data=dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            print("Expected match found for Song %s and Artist %s" % (row.song, row.artist))
            songid, artistid = results
        else:
            # it could happen there will be no match for played song in song and artist table, leave columns empty then 
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    The Function accepts database connection details and path to directory with logs or songsl;
    afterwards it delegates processing to respective processing function by iterating over files.

    Args:
        cur: Database cursor.
        cur: Database connection.
        filepath: JSON files path.
        func: respective processing function.

    Returns:
        None

    """
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
    """ 
    Main function delegates processing to all other functions. 

    Parameters: 
        None

    Returns: 
        None

    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()