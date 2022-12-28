import googleapiclient.discovery 
import googleapiclient.errors
import sqlite3
import tweepy

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
YOUTUBE_API_KEY = "your key here" 
YOUTUBE_PLAYLIST = "your playlist id here"

DATABASE = "database.db"

TWITTER_BEARER_TOKEN = "your bearer token here"
TWITTER_ACCESS_TOKEN = "your access token here"
TWITTER_ACCESS_TOKEN_SECRET = "your access token secret here"
TWITTER_CONSUMER_KEY = "your consumer key here"
TWITTER_CONSUMER_SECRET = "your consumer secret here"

def get_yt_videos(api_service_name:str, api_version:str, api_key:str, playlist:str) -> list:
    """
    Returns the video ids from a youtube playlist.
    
        Parameters:
            api_service_name (str): The name of the API service from Google.
            api_version (str): The version of the API service.
            api_key (str): The API key.
            playlist (str): The youtube playlist id.
        
        Returns:
            video_ids (list): A list of youtube video ids.
    """

    youtube = googleapiclient.discovery.build(
        api_service_name, 
        api_version, 
        developerKey=api_key
        )

    video_ids = []
    page_token = ""
    while True:
        request = youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist,
                    maxResults=50,
                    pageToken=page_token
                )
        response = request.execute()
    
        for video in response['items']:
            vid_id = video['contentDetails']['videoId']
            video_ids.append(vid_id)
        
        if 'nextPageToken' in response:
            page_token = response['nextPageToken']
        else:
            break

    return video_ids


def get_url(video_ids:list) -> list:
    """
    Returns the urls for the youtube video ids.
    
        Parameters:
            video_ids (list): A list of youtube video ids.
        
        Returns:
            urls (list): A list of youtube video urls.
    """
    return ["https://www.youtube.com/watch?v=" + i for i in video_ids]


def zip_vids(video_ids:list, urls:list)-> list:
    """
    Returns a list of tuples of the video ids and urls (for sqlite ingestion).
    
        Parameters: 
            video_ids (list): A list of youtube video ids.
            urls (list): A list of youtube video urls.
                
        Returns:
            records (list): A list of tuples of the video ids and urls.
    """
    return list(zip(video_ids, urls))


def insert_videos(records:list, database:str):
    """
    Inserts the video ids and urls into the database.
    
        Parameters:
            records (list): A list of tuples of the video ids and urls.
            database (str): The name of the database.
        
        Returns:
            None
        
        Raises:
            sqlite3.IntegrityError: If the video id and url are already in the database.
    """
    conn = sqlite3.connect(database)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS videos (
                                            id TEXT,
                                            url TEXT,
                                            tweeted INTEGER,
                                            PRIMARY KEY (id, url)
                                            )''')
    conn.commit()
    
    try:
        c.executemany("INSERT INTO videos VALUES (?, ?, 0)", records)
        conn.commit()
    except sqlite3.IntegrityError:
        print("Already in database")


def get_video(database)-> tuple(str, str):
    """
    Returns a video id and url from the database to tweet which has not been tweeted yet.
    
        Parameters:
            database (str): The name of the database.
        
        Returns:
            video_id (str): A youtube video id.
            video_url (str): A youtube video url.
    """
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute("SELECT id, url FROM videos WHERE tweeted = 0")
    video_id = c.fetchone()[0]
    video_url = c.fetchone()[1]
    
    return video_id, video_url


def update_chosen_video(video_id:str, database:str):
    """
    Updates the database to show that the video has been tweeted.
    
        Parameters:
            video_id (str): A youtube video id.
            database (str): The name of the database.
        
        Returns:
            None
    """
    conn = sqlite3.connect(database)
    c = conn.cursor()
    print(video_id)
    c.execute("UPDATE videos SET tweeted = 1 WHERE id = ?", (video_id,))
    conn.commit()


def tweet_video(video_url:str, bearer_token:str, access_token:str, access_token_secret:str, consumer_key:str, consumer_secret:str):
    """
    Tweets the video url.
    
        Parameters:
            video_url (str): A youtube video url.
            bearer_token (str): The bearer token for the Twitter API.
            access_token (str): The access token for the Twitter API.
            access_token_secret (str): The access token secret for the Twitter API.
            consumer_key (str): The consumer key for the Twitter API.
            consumer_secret (str): The consumer secret for the Twitter API.
        
        Returns:
            None
    """
    
    # Authenticate to Twitter
    api = tweepy.Client(bearer_token=bearer_token,
                        access_token=access_token,
                        access_token_secret=access_token_secret,
                        consumer_key=consumer_key,
                        consumer_secret=consumer_secret)

    api.create_tweet(text=video_url)


def main():
    """
    Main function. 
    
    Parses the youtube playlist, inserts the video ids and urls into the database, gets a video id and url from the database to tweet, 
    updates the database to show that the video has been tweeted, and tweets the video url.
    
        Parameters:
            None
        
        Returns:
            None
    """
    video_ids = get_yt_videos(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, YOUTUBE_API_KEY, YOUTUBE_PLAYLIST)
    urls = get_url(video_ids)
    records = zip_vids(video_ids, urls)

    insert_videos(records, DATABASE)
    video_id, video_url = get_video(DATABASE)
    update_chosen_video(video_id, DATABASE)

    tweet_video(video_url, TWITTER_BEARER_TOKEN, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)


if __name__ == "__main__":
    main() 