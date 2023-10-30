import pandas as pd
import pymongo
from pymongo import MongoClient
from googleapiclient.discovery import build
Api_Key = 'AIzaSyCq83WjG246N6aDaMQMb6GUeP4c3PZSwoA'
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey = Api_Key)
channel_id = 'UCi3o8sgPl4-Yt501ShuiEgA'

# Fetching the Channel Details using Channel ID:

def get_channel_data(youtube, channel_id):

            channel_response = youtube.channels().list(
            id=channel_id,
            part='snippet,statistics,contentDetails')
            channel_detls = channel_response.execute()

            c_data = dict ( Channel_name = channel_detls['items'][0]['snippet']['title'],
                            Channel_ID = channel_detls['items'][0]['id'],
                            Subscribers = channel_detls['items'][0]['statistics']['subscriberCount'],
                            Views = channel_detls['items'][0]['statistics']['viewCount'],
                            Description = channel_detls['items'][0]['snippet']['description'],
                            Total_Videos = channel_detls['items'][0]['statistics']['videoCount'],
                            Playlist_id = channel_detls['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                            )
            return c_data
get_channel_data(youtube, channel_id)

# Fetching the Video_ID Details from Playlist:
def get_videoID_data(youtube, playid):

            vid_details = []

            vid_request = youtube.playlistItems().list(
            playlistId=playid,
            maxResults=50,
            part='snippet,contentDetails' # snippet,statistics,contentDetails
            )
            response = vid_request.execute()

            for item in response['items']:
                vid_details.append(item['contentDetails']['videoId'])

            return vid_details

#Fetching video details of each video in the playlist:

def get_videodetails(youtube, videoID_data): # ----------- 3
            videoid_details =[]
            for video_id in videoID_data: # Fetch video details
                video_response = youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=video_id,
                    maxResults=50
                ).execute()

                if video_response['items']:
                    video_details = {
                        "Video_Id": video_id,
                        "Video_Name": video_response['items'][0]['snippet']['title'],
                        "Video_Description": video_response['items'][0]['snippet']['description'],
                        "Video_Statistics": video_response['items'][0]['statistics']['commentCount'],
                        "Comment_Count": video_response['items'][0]['statistics'].get('commentCount', 0),
                        "View_Count": video_response['items'][0]['statistics'].get('viewCount', 0),
                        "Like_Count": video_response['items'][0]['statistics'].get('likeCount', 0),
                        "Favorite_Count": video_response['items'][0]['statistics'].get('favoriteCount', 0),
                        "Published_At": video_response['items'][0]['snippet']['publishedAt'],
                        "Duration": video_response['items'][0]['contentDetails']['duration'],
                        "Thumbnail": video_response['items'][0]['snippet']['thumbnails']['default']['url'],
                        "Caption_Status": video_response['items'][0]['contentDetails'].get('caption'),
                    }

                videoid_details.append(video_details)

            return videoid_details

#Fetching the Commands of each videos

def get_comments_details(youtube,videoID_data): # ----------- 4
            v_c_data = []
            for i in videoID_data:
                comments_response = youtube.commentThreads().list(
                    part='snippet',
                    maxResults=2,  # only 2 comments per vios
                    videoId=i
                ).execute()

                comments = comments_response['items']

                for comment in comments:
                    comment_information = {
                        "Video_iD": i,
                        "Comment_Id": comment['snippet']['topLevelComment']['id'],
                        "Comment_Text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    v_c_data.append(comment_information)

            return v_c_data

#Declaring

getCH = get_channel_data(youtube, channel_id)
Channel_Name = (getCH['Channel_name'][:30]) # get channel name from the getch
playid = (getCH['Playlist_id'])
videoID_data = get_videoID_data(youtube, playid)
getVD = get_videodetails(youtube, videoID_data)
getCC = get_comments_details(youtube,videoID_data)

#Adding all datas to a single data frame.

Main_data_frame = {
            'Channel': getCH,
            'VideoID': videoID_data,
            'VideoDetail': getVD,
            'Comments': getCC
        }

#Create a MongoClient instance to connect to the MongoDB server

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://hariprakashpm:Kavyalaya@cluster0.rcugtle.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

from pymongo.mongo_client import MongoClient
client = MongoClient("mongodb+srv://hariprakashpm:Kavyalaya@cluster0.rcugtle.mongodb.net/?retryWrites=true&w=majority")
db = client["YouTube"]
collection = db[Channel_Name]
mongo_dump = collection.insert_one(Main_data_frame)

#SQL Connection 

import mysql.connector
mydb = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            # port = 3306,
            # database = 'YoutubeProject'
        )
print(mydb)
mycursor = mydb.cursor(buffered=True)


#Creating Database:

mycursor.execute('Create Database YouTubes')
mycursor.execute('Use YouTubes')

#Channel table Creation 01:

def get_channel_data():
            
            Channel_table = f"""CREATE TABLE {Channel_Name}_Channel
            (
            Channel_name VARCHAR(255),
            Channel_ID VARCHAR(255),
            Subscribers INT,
            Views INT,
            Description TEXT,
            Total_Videos INT,
            Playlist_id VARCHAR(255)
            )"""
            
            mycursor.execute(Channel_table) # Create the table
            
            mongo_data = collection.find() # Fetch data from MongoDB collection
            
            # Insert data into MySQL table
            for document in mongo_data:
                
                Channel_name = document["Channel"]["Channel_name"]
                Channel_ID = document["Channel"]["Channel_ID"]
                Subscribers = document["Channel"]["Subscribers"]
                Views = document["Channel"]["Views"]
                Description = document["Channel"]["Description"]
                Total_Videos = document["Channel"]["Total_Videos"]
                Playlist_id = document["Channel"]["Playlist_id"]

                mysql_query = f"""
                INSERT INTO {Channel_Name}_Channel(
                    Channel_name, Channel_ID, Subscribers, Views, Description, Total_Videos, Playlist_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
                """
                mysql_values = (
                    Channel_name, Channel_ID, Subscribers, Views, Description, Total_Videos, Playlist_id
                )
                mycursor.execute(mysql_query, mysql_values)

            mycursor.execute(f"""select * from youtubes.{Channel_Name}_Channel""")

            rows = mycursor.fetchall() # Fetch the results of the query

            # Print the rows
            for row in rows:
                print(row)
            
            return rows

# Video table Creation 02:

def get_video_data():

            Video_table = f"""CREATE TABLE {Channel_Name}_Video
            (
            Video_Id VARCHAR(255),
            Video_Name VARCHAR(255),
            Video_Description TEXT,
            Video_Statistics VARCHAR(255),
            Comment_Count INT,
            View_Count INT,
            Like_Count INT,
            Favorite_Count INT,
            Published_At VARCHAR(255),
            Duration VARCHAR(255),
            Thumbnail VARCHAR(255),
            Caption_Status VARCHAR(255),
            Playlist_id VARCHAR(255)
            )"""
            
            mycursor.execute(Video_table)
            
            mongo_data = collection.find() # Fetch data from MongoDB collection

            # Insert data into MySQL table
            for document in mongo_data:
                Playlist_id = document["Channel"]["Playlist_id"]
                for video_detail in document["VideoDetail"]:
                    
                    Video_Id = video_detail["Video_Id"]
                    Video_Name = video_detail["Video_Name"]
                    Video_Description = video_detail["Video_Description"]
                    Video_Statistics = video_detail["Video_Statistics"]
                    Comment_Count = video_detail["Comment_Count"]
                    View_Count = video_detail["View_Count"]
                    Like_Count = video_detail["Like_Count"]
                    Favorite_Count = video_detail["Favorite_Count"]
                    Published_At = video_detail["Published_At"]
                    Duration = video_detail["Duration"]
                    Thumbnail = video_detail["Thumbnail"]
                    Caption_Status = video_detail["Caption_Status"]
                    

                    mysql_query1 = f"""
                    INSERT INTO {Channel_Name}_Video (
                        Video_Id, Video_Name, Video_Description, Video_Statistics, Comment_Count, View_Count, Like_Count,Favorite_Count, Published_At, Duration, Thumbnail, Caption_Status, Playlist_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    mysql_values1 = (
                        Video_Id, Video_Name, Video_Description, Video_Statistics, Comment_Count, View_Count, Like_Count,Favorite_Count, Published_At, Duration, Thumbnail, Caption_Status, Playlist_id
                    )
                    mycursor.execute(mysql_query1, mysql_values1)

                mycursor.execute(f"""select * from youtubes.{Channel_Name}_Video""")

                rows1 = mycursor.fetchall() # Fetch the results of the query

                for row1 in rows1:
                    print(row1)

            return rows1

 # Comment table Creation 03:

def get_comments_data():
            
            Comment_table = f"""CREATE TABLE {Channel_Name}_Comments
            (
            Video_id VARCHAR(255),
            Comment_id VARCHAR(255),
            Comment_text TEXT,
            Comment_author VARCHAR(255),
            Comment_PublishedAt VARCHAR(255),
            Playlist_id VARCHAR(255)
            )"""

            mycursor.execute(Comment_table)

            mongo_data = collection.find()

            for document in mongo_data: # Insert data into MySQL table
                Playlist_id = document["Channel"]["Playlist_id"]
                for cc_detail in document["Comments"]:
                    Video_iD = cc_detail["Video_iD"]
                    Comment_Id = cc_detail["Comment_Id"]
                    Comment_Text = cc_detail["Comment_Text"]
                    Comment_Author = cc_detail["Comment_Author"]
                    Comment_PublishedAt = cc_detail["Comment_PublishedAt"]

                    mysql_query2 = f"""
                    INSERT INTO {Channel_Name}_Comments (
                        Video_iD, Comment_Id, Comment_Text, Comment_Author, Comment_PublishedAt, Playlist_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s)"""
                    mysql_values2 = (
                        Video_iD, Comment_Id, Comment_Text, Comment_Author, Comment_PublishedAt, Playlist_id
                    )
                    mycursor.execute(mysql_query2, mysql_values2)

                mycursor.execute(f"""select * from youtubes.{Channel_Name}_Comments""")
                
                rows2 = mycursor.fetchall()

                for row2 in rows2:
                    print(row2)

            return rows2

# Playlist table Creation 04:

def get_playlist_data():
            
            Playlist_table = f"""CREATE TABLE {Channel_Name}_Playlist 
            ( 
                Channel_ID VARCHAR(255), 
                Playlist_id VARCHAR(255), 
                VideoID VARCHAR(255)
                )"""

            mycursor.execute(Playlist_table)

            mongo_data = collection.find()

            # Insert data into the SQL table
            for document in mongo_data:
                
                Channel_ID = document["Channel"]["Channel_ID"]
                Playlist_id = document["Channel"]["Playlist_id"]
                video_pliD = document["VideoID"]
                mysql_query3 = f"""INSERT INTO {Channel_Name}_Playlist (Channel_ID, VideoID, Playlist_id) VALUES (%s, %s, %s)"""
                mysql_values3 = [(Channel_ID, video_id, Playlist_id) for video_id in video_pliD]
                mycursor.executemany(mysql_query3, mysql_values3)
            
            mycursor.execute(f"""select * from youtubes.{Channel_Name}_Playlist""")
            
            rows3 = mycursor.fetchall()

            for row3 in rows3:
                print(row3)
            
            return rows3


myscursor = mydb.cursor()

def sql_execution():

            sqlch = get_channel_data() # SQL table creation
            sqlvi = get_video_data()
            sqlcc = get_comments_data()
            sqlpl = get_playlist_data()

execution = sql_execution()

dataf0 = pd.read_sql(f"""SELECT * FROM youtubes.{Channel_Name}_Channel""", mydb) # Read data from the hello table
dataf1 = pd.read_sql(f"""SELECT * FROM youtubes.{Channel_Name}_Video""", mydb) # dataf1 = pd.read_sql_query(execution[3], mysql_conn)
dataf2 = pd.read_sql(f"""SELECT * FROM youtubes.{Channel_Name}_Comments""", mydb)
dataf3 = pd.read_sql(f"""SELECT * FROM youtubes.{Channel_Name}_Playlist""", mydb)

