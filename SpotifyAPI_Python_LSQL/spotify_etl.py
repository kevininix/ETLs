import sqlalchemy 
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import os

DB_LOCATION = 'sqlite:////my_played_tracks.sqlite'
USER_ID = os.environ.get('USER_ID')
TOKEN = os.environ.get('TOKEN')

# Fucntion to validate data
def check_if_valid(df: pd.DataFrame) -> bool:
    # is it empty?
    if df.empty:
        print('No songs downloaded, finishing execution')
        return False
    
    # Check for primary key duplicates
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary Key is not unique')
    
    # Check for NULLS
    if df.isnull().values.any():
        raise Exception('Null value found')
    
    # Check only data from last 24h is being added
    yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
    yesterday = yesterday.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception('At least one of the songs comes from more than 24h ago')
    return True

if __name__ == '__main__':
    
    # Extract step
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {TOKEN}'
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = 1)
    # Convert to unix time
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Request libray
    request = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}", 
    headers = headers)
    
    data = request.json()

    # Relevant columns
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
   
    for song in data['items']:

        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
    # Create dataframe with the lists
    song_df= pd.DataFrame({
        'song_name': song_names, 
        'artist_name': artist_names,
        'played_at':played_at_list, 
        'timestamp':timestamps
        })
    
    # Validation
    if check_if_valid(song_df):
        print('Data validated, proceed to the Loading step')

    # Loading
    
    # Connect to sqlite
    engine = sqlalchemy.create_engine(DB_LOCATION)
    conn = sqlite3.connect('my_played_songs.sql')
    cursor = conn.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS my_played_songs(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    cursor.execute(sql)
    print('Database opened succesfully')
    
    # Load data into table
    try:
        song_df.to_sql('my_played_songs', engine, index = False, if_exists = 'append')
    except:
        print('Data already exists in table')
    
    # End connection
    conn.close()
    print('Database closed succesfully')