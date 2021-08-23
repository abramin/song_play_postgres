import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads json song file, creates song and artist dataframes and writes all records to the database
    """
    # open song file
    df = pd.read_json(filepath,typ='series')
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = artist_data.tolist()
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    song_data = song_data.tolist()
    cur.execute(song_table_insert, song_data)
    



def process_log_file(cur, filepath):
    """
    - Reads json log file, processses data and writes Time, User and SongPlay records to corresponding tables.
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t,t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, (t.dt.weekday.isin([0,1,2,3,4])))
    column_labels = ('Time','Hour', 'Day', 'WeekofYear', 'Month', 'Year', 'WeekDay')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    # Bulk load time records
    tmp_time_df = "./tmp_time_dataframe.csv"
    time_df.to_csv(tmp_time_df, index=False, header=False)
    file = open(tmp_time_df, 'r')
    cur.copy_from(file, "time", sep=",")
    os.remove(tmp_time_df)

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
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - High level function that takes filepath and calls processing function to extract, transform and load data into database
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
        try:
            func(cur, datafile)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as e:
            print("Database Error: %s" % e)
            conn.rollback()
            cur.close()
            return 1
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()