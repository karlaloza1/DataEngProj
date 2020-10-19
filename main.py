import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json 
from datetime import datetime
import datetime
import sqlite3 

# constants database location 
DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "	45d23vtnitp2horhy5suxpwyd" # your Spotify username 
#token we just generated 
TOKEN = "BQBYDdnZByqrKFBbuNsFMul2r_arPVb_x0xAliO9vnoaHE6ZaOZUAux8Ph8KJJfWHnXf3dTZa6voS--HJ4hlhbe4nNuEn6XApg1VidRv2Zlf4Ajdr_l3E_nLwteosHJ7guhesg56XQopupiCBEC8E1YlJiUS54HF1Guw"

#Before we even think about loading data, we need to validate data to make sure we have accurate 
#data on the database
#Validate data to be able to store it on the database
#we dont want messy data, vendors can send empty files and etc. 

#This function will do data validation to see that data enters the database correctly
def check_if_valid_data(df: pd.DataFrame) -> bool:

#check if the json file is empty
    if df.empty:
        print("No song downloaded. Finishing execution")
        return False

#first we need to undestand the primary key on the data we get
#we know that played_at is the primary key because there is only posible to listen something
#at a specific time. 
#Primary Key Check

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    #Check for nulls values on the file. Because we do not want null values on our table

    if df.isnull().values.any():
        raise Exception("Null values found")


    # Check that all timestamps are of yesterday's date
    
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
    #check that every entry is from yesterday and not same day 
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")
     
    return True



if __name__ == "__main__":
    
    # Extract part of the ETL process
    #API instructions
 
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    
    # Convert time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()
    #print(data)

    # values we actually want from the json file
    #appending the data to the list
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps =[]

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
    #create a dictionary to pass in a panda DataFrame 
    song_dict ={
        "song_name": song_names,
        "artist_name": artist_names, 
        "played_at": played_at_list,
        "timestamp": timestamps 
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    if check_if_valid_data(song_df):
            print("Data valid, proceed to Load stage")
    
    print(song_df)

#LOAD 

#Storing data on cloud or premise
#ORM - Object Relational Mappers
# writing code in python but want to get the data on the database using SQL. Library allows you
# to query your data qithout using SQL such as sqlalchemy
# Databases can be divided on relational or non relational (store data in json documents)

#We would use SQLite 
#Database has to be stored somewhere either on premise or in the cloud. 
#ORM - only for relationa databases - to retrieve data from database need to use SQL but writing the code on Python
# you are able to query your data on python code with SQLAlchemy 

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor() 

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
"""
cursor.execute(sql_query)
print("Opened database sucessfully")

try:
    song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
except:
    print("Data already in the database")

conn.close()
print("Close database sucessfully")


#1484811043508
