from googleapiclient.discovery import build
import pymongo
import mysql.connector as con
import streamlit as st


# API key credentials

def api_connect():
    api_key = '********'

    api_service_name = "youtube"  # API service name
    api_version = "v3"  # API version
    youtube = build(api_service_name, api_version, developerKey=api_key)  # API object
    return youtube


youtube = api_connect()


def get_channel_id(channel_name):
    search_response = youtube.search().list(
        q=channel_name,
        part='id',
        type='channel'
    ).execute()

    # the first search result is the desired channel
    channel_id = search_response['items'][0]['id']['channelId']
    return channel_id


# Call the function with a channel name
channel_name = input("Enter channel name: ")
channel_id = get_channel_id(channel_name)
channel_id


def get_channel_details(Channel_id):
    channel_data = youtube.channels().list(
        id=Channel_id,
        part='snippet,statistics,contentDetails'
    ).execute()

    # Extract channel data
    channel_details = {}
    for item in channel_data['items']:
        channel_details = {
            "channel_name": item["snippet"]["title"],
            "channel_id": item["id"],
            "channel_description": item["snippet"]["description"],
            "subscription_count": item["statistics"]["subscriberCount"],
            "channel_views": item["statistics"]["viewCount"],
            "total_videos": item["statistics"]["videoCount"],
            "Playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"]
        }

    return channel_details


# Get channel details using the retrieved channel ID
Channel_details = get_channel_details(channel_id)


def get_video_ids(channel_Id):
    video_ids = []
    response = youtube.channels().list(id=channel_Id,
                                       part="contentDetails").execute()

    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids


video_ids = get_video_ids(channel_id)


def get_video_details(Video_Ids):
    video_datas = []

    for video_id in Video_Ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response['items']:
            data = {
                "channel_name": item["snippet"]["channelTitle"],
                "channel_id": item["snippet"]["channelId"],
                "video_id": item["id"],
                "video_title": item["snippet"]["title"],
                "tags": item.get("snippet", {}).get("tags"),
                "video_description": item["snippet"]["description"],
                "published_at": item["snippet"]["publishedAt"],
                "duration": item["contentDetails"]["duration"],
                "video_views": item["statistics"]["viewCount"],
                "comments_count": item.get("statistics", {}).get("commentCount"),
                "favorite_count": item["statistics"]["favoriteCount"],
                "like_count": item.get("statistics", {}).get("likeCount"),
                "dislike_count": item.get("statistics", {}).get("dislikeCount"),
                "definition": item["contentDetails"]["definition"],
                "caption_status": item["contentDetails"]["caption"]
            }
            video_datas.append(data)

    return video_datas


video_details = get_video_details(video_ids)


def get_comment_data(Video_Ids):
    video_comments = []
    # Fetch Comments for Each Video
    for video_id in Video_Ids:
        try:
            comments_response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            ).execute()
            # Extract Comments Information
            for comment_item in comments_response.get('items', []):
                comment_data = {
                    "Comment_id": comment_item['snippet']["topLevelComment"]["id"],
                    "video_id": comment_item['snippet']["topLevelComment"]["snippet"]["videoId"],
                    "Comment_text": comment_item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_published": comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
                }

                video_comments.append(comment_data)

        except Exception as e:
            print(f"Failed to fetch comments for video ID {video_id}: {str(e)}")

    return video_comments


comment_data = get_comment_data(video_ids)


def get_playlist_data(Channel_id):
    next_page_token = None
    playlist_data = []
    while True:
        playlist_response = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=Channel_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in playlist_response['items']:
            data = dict(playlist_id=i["id"],
                        channel_id=i['snippet']["channelId"],
                        playlist_name=i['snippet']['title'],
                        video_count=i['contentDetails']['itemCount'],
                        playlist_published=i['snippet']['publishedAt'])

            playlist_data.append(data)

        next_page_toekn = response.get("nextPageToken")
        if next_page_token is None:
            break

    return playlist_data


video_playlist = get_playlist_data(channel_id)

client = pymongo.MongoClient(
    "mongodb+srv://avinodhini1996:<*********>@cluster0.5eywzj2.mongodb.net/?retryWrites=true&w=majority")

db = client["youtube_harvesting"]


def youtube_channel_details(channel):
    # Fetch YouTube Channel ID
    yt_channel_id = get_channel_id(channel)

    # Fetch Channel Details
    yt_channel_details = get_channel_details(yt_channel_id)

    # Fetch Video IDs
    yt_video_ids = get_video_ids(yt_channel_id)

    # Fetch Video Details
    yt_video_details = get_video_details(yt_video_ids)

    # Fetch Comments for Videos
    yt_comments = get_comment_data(yt_video_ids)

    # Fetch Playlists for the Channel
    yt_playlists = get_playlist_data(yt_channel_id)

    # Store fetched data in MongoDB
    col = db["youtube_channels"]  # Accessing the collection in the database
    col.insert_one({
        "channel_data": yt_channel_details,  # Storing channel details
        "video_data": yt_video_details,  # Storing video details
        "comment_data": yt_comments,  # Storing comments data
        "playlist_data": yt_playlists  # Storing playlist data
    })

    return "Upload successful!"  # Returning success message


Channel_name = input("input youtube channel name: ")
Channel_details = youtube_channel_details(Channel_name)

import mysql.connector as con

# Replace placeholders with your MySQL connection details
host = 'localhost'
user = 'root'
password = '12345'
database = 'youtube_data'

# Establish the connection
mydb = con.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Check if the connection is successful
if connection.is_connected():
    print("Connected to MySQL!")
else:
    print("Connection failed.")

    # Cursor for MySQL
cursor = mydb.cursor()


def channels_table():
    # Replace placeholders with your MySQL connection details
    host = 'localhost'
    user = 'root'
    password = '12345'
    database = 'youtube_data'

    # Establish the connection

    mydb = con.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """
        CREATE TABLE IF NOT EXISTS Channel (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Subscribers BIGINT,
            Views BIGINT,
            Total_Videos INT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(100)
        )
        """

        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print("An error occurred:", e)

        channel_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "channel_data": 1}):
        channel_list.append(i["channel_data"])

    df = pd.DataFrame(channel_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO Channel (
            Channel_Name,
            Channel_Id,
            Subscribers,
            Views,
            Total_Videos,
            Channel_Description,
            Playlist_Id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        values = (
            row["channel_name"],
            row["channel_id"],
            row["subscription_count"],
            row["channel_views"],
            row["total_videos"],
            row["channel_description"],
            row["Playlist_id"]
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("An error occurred:", e)


def channels_table():
    # Replace placeholders with your MySQL connection details
    host = 'localhost'
    user = 'root'
    password = '12345'
    database = 'youtube_data'

    # Establish the connection

    mydb = con.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """
        CREATE TABLE IF NOT EXISTS Channel (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Subscribers BIGINT,
            Views BIGINT,
            Total_Videos INT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(100)
        )
        """

        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print("An error occurred:", e)

        channel_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "channel_data": 1}):
        channel_list.append(i["channel_data"])

    df = pd.DataFrame(channel_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO Channel (
            Channel_Name,
            Channel_Id,
            Subscribers,
            Views,
            Total_Videos,
            Channel_Description,
            Playlist_Id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        values = (
            row["channel_name"],
            row["channel_id"],
            row["subscription_count"],
            row["channel_views"],
            row["total_videos"],
            row["channel_description"],
            row["Playlist_id"]
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("An error occurred:", e)


def playlist_table():
    # Replace placeholders with your MySQL connection details
    host = 'localhost'
    user = 'root'
    password = '12345'
    database = 'youtube_data'

    # Establish the connection

    mydb = con.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """
        CREATE TABLE IF NOT EXISTS Playlist (
            Playlist_Id VARCHAR(100) Primary key,
            Channel_Id VARCHAR(100),
            Playlist_Name VARCHAR(100),
            PublishedAt timestamp,
            Video_Count INT

        )
        """

        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print("An error occurred:", e)

    Playlist_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "playlist_data": 1}):
        for j in range(len(i["playlist_data"])):
            Playlist_list.append(i["playlist_data"][j])

    Playlist_list

    df_playlist = pd.DataFrame(Playlist_list)

    from datetime import datetime

    # Function to convert the datetime string to the desired format
    def convert_datetime(datetime_str):
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
        formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_datetime

    # Apply the conversion function to the entire 'playlist_published' column
    df_playlist['playlist_published'] = df_playlist['playlist_published'].apply(lambda x: convert_datetime(x))

    for index, row in df_playlist.iterrows():
        insert_query = '''INSERT INTO Playlist (
            Playlist_Id,
            Channel_Id,
            Video_Count,
            PublishedAt,
            Playlist_Name

        ) VALUES (%s, %s, %s, %s, %s)'''

        values = (
            row["playlist_id"],
            row["channel_id"],
            row["video_count"],
            row["playlist_published"],
            row["playlist_name"]

        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("An error occurred:", e)


def comments_table():
    # Replace placeholders with your MySQL connection details
    host = 'localhost'
    user = 'root'
    password = '12345'
    database = 'youtube_data'

    # Establish the connection

    mydb = con.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    drop_query = "drop table if exists Comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """
        CREATE TABLE IF NOT EXISTS Comments (
                        Comment_Id varchar(100) primary key,
                        Video_Id varchar(100),
                        Comment_Text text,
                        Comment_Author varchar(100),
                        Comment_Publishedt timestamp

        )
        """

        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print("An error occurred:", e)

    Comments_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "comment_data": 1}):
        for j in range(len(i["comment_data"])):
            Comments_list.append(i["comment_data"][j])

    df_comments = pd.DataFrame(Comments_list)

    # Apply the conversion function to the entire 'published' column
    df_comments['Comment_published'] = df_comments['Comment_published'].apply(lambda x: convert_datetime(x))

    for index, row in df_comments.iterrows():
        insert_query = '''INSERT INTO Comments (
                        Comment_Id,
                        Video_Id,
                        Comment_Text,
                        Comment_Author,
                        Comment_publishedt


        ) VALUES (%s,%s,%s,%s,%s)'''

        values = (
            row["Comment_id"],
            row["video_id"],
            row["Comment_text"],
            row["Comment_Author"],
            row["Comment_published"]

        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("An error occurred:", e)


def videos_table():
    # Replace placeholders with your MySQL connection details
    host = 'localhost'
    user = 'root'
    password = '12345'
    database = 'youtube_data'

    # Establish the connection

    mydb = con.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    drop_query = "DROP TABLE IF EXISTS Videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """
        CREATE TABLE IF NOT EXISTS Videos (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(100),
            Video_Title VARCHAR(100),
            Tags TEXT,
            Video_Description TEXT,
            Published_Date TIMESTAMP,
            Duration varchar(100),
            Video_Views BIGINT,
            Comments_Count INT,
            Favorite_count INT,
            Like_Count BIGINT,
            Dislike_Count varchar(100),
            Definition VARCHAR(100),
            Caption_Status VARCHAR(100)
        )
        """

        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print("An error occurred:", e)

    Videos_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "video_data": 1}):
        for j in range(len(i["video_data"])):
            Videos_list.append(i["video_data"][j])

    df_videos = pd.DataFrame(Videos_list)

    # Apply the conversion function to the entire 'published_at' column
    df_videos['published_at'] = df_videos['published_at'].apply(lambda x: convert_datetime(x))

    for index, row in df_videos.iterrows():
        insert_query = '''INSERT INTO Videos (
            Channel_Name,
            Channel_Id,
            Video_Id,
            Video_Title,
            Tags,
            Video_Description,
            Published_Date,
            Duration,
            Video_Views,
            Comments_Count,
            Favorite_count,
            Like_Count,
            Dislike_Count,
            Definition,
            Caption_Status

        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (
            row["channel_name"],
            row["channel_id"],
            row["video_id"],
            row["video_title"],
            row["tags"],
            row["video_description"],
            row["published_at"],
            row["duration"],
            row["video_views"],
            row["comments_count"],
            row["favorite_count"],
            row["like_count"],
            row["dislike_count"],
            row["definition"],
            row["caption_status"]

        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("An error occurred:", e)


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "tables created sucessfully!"


def show_videos():
    Videos_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "video_data": 1}):
        for j in range(len(i["video_data"])):
            Videos_list.append(i["video_data"][j])
        df_videos = st.DataFrame(Videos_list)
    return df_videos


def show_comments():
    Comments_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "comment_data": 1}):
        for j in range(len(i["comment_data"])):
            Comments_list.append(i["comment_data"][j])
        df_comments = st.DataFrame(Comments_list)
    return df_comments


def show_playlist():
    Playlist_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "playlist_data": 1}):
        for j in range(len(i["playlist_data"])):
            Playlist_list.append(i["playlist_data"][j])
        df_playlist = st.DataFrame(Playlist_list)
    return df_playlist


def show_channel():
    channel_list = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "channel_data": 1}):
        channel_list.append(i["channel_data"])
    df_channel = st.DataFrame(Videos_list)
    return df_channel


with st.sidebar:
    st.title(":black[YOUTUBE DATA HARVESTING USINGSQL,MONGODB AND STREAMLIT]")

Channel_ID = st.text_input("Enter the YouTube channel ID")

if st.button("collect and store data"):
    channels_ids = []
    db = client["youtube_harvesting"]
    col = db["youtube_channels"]
    for i in col.find({}, {"_id": 0, "channel_data": 1}):
        channel_ids.append(i["channel_data"]["Channel_ID"])

    if Channel_ID in channel_ids:
        st.success("Channel Details already exists")
    else:
        insert = youtube_channel_details(Channel_ID)
        st.success(insert)

if st.button("Migrate data to MYSQL"):
    Table = tables()
    st.success(Table)

show_table = st.radio("view Table", ("Channels", "Playlists", "Videos", "Comments"))

if show_table == "Channels":
    show_channel()

elif show_table == "Playlists":
    show_playlist()

elif show_table == "Videos":
    show_videos()

elif show_table == "Comments":
    show_comments()

host = 'localhost'
user = 'root'
password = '12345'
database = 'youtube_data'

# Establish the connection
mydb = con.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = mydb.cursor()

question = st.selectbox("Select your question",
                        ("1. What are the names of all the videos and their corresponding channels?",
                         "2. Which channels have the most number of videos, and how many videos do they have?",
                         "3. What are the top 10 most viewed videos and their respective channels?",
                         "4. How many comments were made on each video, and what are their corresponding video names?",
                         "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                         "6. What is the total number of likes and favorites for each video, and what are their corresponding video names?",
                         "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                         "8. What are the names of all the channels that have published videos in the year 2022?",
                         "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                         "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question == "1. What are the names of all the videos and their corresponding channels? ":
    query1 = "select title as videos, channel,_name as channelname from videos"
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["video title", "Channel name"])
    st.write(df)

elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
    query2 = "select channel_name as channelname, total_cideos as no_videos from channels order by total_videos desc"
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)

elif question == "3. What are the top 10 most viewed videos and their respective channels?":
    query2 = '''select view as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
    st.write(df3)

elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comments as no_commnets,title as videotitle from videos where commnets id not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of commets", "videotitle"])
    st.write(df4)

elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select title as videotitle,channel_name as channelname likes as likecount
            from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["videotitle", "channel name", "likecount"])
    st.write(df5)

elif question == "6. What is the total number of likes and favorites for each video, and what are their corresponding video names?":
    query6 = '''select likes as likecount, title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select channel_name as channelname, views as totalviews from channels, title as videotitle from videos'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channel name", "total views"])
    st.write(df7)

elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select title as videotitle, published_date as videorelease, channel_name as channelname from videos 
            where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8)

elif question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

    T9 = []
    for index, row in df9.iterrows():
        channell_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duarion)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df1 = pd.DataFrame(T9)
    st.write(df1)

elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select title as videotitle,channel_name as channelname,comments as comments from videos
                where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title", "channel name", "comments"])
    st.write(df10)

