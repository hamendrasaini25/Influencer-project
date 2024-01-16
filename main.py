import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import NoTranscriptFound
from concurrent.futures import ThreadPoolExecutor


def get_video_comments(youtube, video_id):
    video_comments = []
    try:
        comments_response = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            maxResults=50  # Adjust as needed
        ).execute()

        # Extract Comments for each Video
        for comment_item in comments_response.get('items', []):
            comment_text = comment_item['snippet']['topLevelComment']['snippet']['textDisplay']
            author_name = comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName']
            likes_count = comment_item['snippet']['topLevelComment']['snippet'].get('likeCount', 0)
            video_comments.append({'author': author_name, 'comment': comment_text, 'likes': likes_count})

            # Get replies to the comment
            replies = comment_item.get('replies', {}).get('comments', [])

            for reply in replies:
                reply_text = reply['snippet']['textDisplay']
                reply_author_name = reply['snippet']['authorDisplayName']
                reply_likes_count = reply['snippet'].get('likeCount', 0)
                video_comments.append({'author': reply_author_name, 'comment': f'Reply: {reply_text}', 'likes': reply_likes_count})

        # Sort comments by likes
        video_comments.sort(key=lambda x: x.get('likes', 0), reverse=True)

        # Capture all comments
        all_comments = video_comments

        # Capture the top most liked comments
        top_comments_count = min(len(video_comments), 5)
        top_most_liked_comments = video_comments[:top_comments_count]

    except HttpError as e:
        if e.resp.status == 403:
            # Comments are disabled for this video, skip comments retrieval
            pass
        all_comments = []
        top_most_liked_comments = []

    return all_comments, top_most_liked_comments

def get_video_captions(video_id):
    caption = []
    try:
        # Retrieve the available transcripts for the video
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        try:
            # Check if there is a manually created transcript in English
            manual_transcript_en = transcript_list.find_manually_created_transcript(['en'])

            if manual_transcript_en:
                # If a manual transcript in English is available, use it
                caption.append({'transcript_text': ' '.join(entry['text'] for entry in manual_transcript_en.fetch())})
        except NoTranscriptFound:
            pass  # Handle the case where no manually created transcript is found

        # If not, check if there is an automatically generated transcript in English
        generated_transcript_en = transcript_list.find_generated_transcript(['en'])
        if generated_transcript_en:
            # Extract transcript text without timestamps
            caption.append({'transcript_text': ' '.join(entry['text'] for entry in generated_transcript_en.fetch())})
        else:
            # If no English transcript is available, check if the transcript is translatable
            if transcript_list.is_translatable:
                # Translate the transcript to English
                translated_transcript = transcript_list.translate_to('en')
                # Extract translated transcript text without timestamps
                caption.append({'transcript_text': ' '.join(entry['text'] for entry in translated_transcript.fetch())})
            else:
                # If no English transcript and translation is available, return None
                caption.append(None)

    except Exception as e:
        caption.append(None)

    return caption

def get_video_statistics(youtube, video_id):
    try:
        video_statistics_response = youtube.videos().list(part='statistics,snippet', id=video_id).execute()
        statistics = video_statistics_response['items'][0]['statistics']
        snippet = video_statistics_response['items'][0]['snippet']
        views = statistics['viewCount']
        likes = statistics.get('likeCount', 0)
        dislikes = statistics.get('dislikeCount', 0)
        upload_date = snippet.get('publishedAt', '')
    except:
        views = likes = dislikes = upload_date = None

    return views, likes, dislikes, upload_date

def process_video(item):
    youtube = build("youtube", 'v3', developerKey=api_key)
    video_id = item['snippet']['resourceId']['videoId']
    video_title = item['snippet']['title']
    video_description = item['snippet']['description']
    views, likes, dislikes, upload_date = get_video_statistics(youtube, video_id)
    all_comments, top_most_liked_comments = get_video_comments(youtube, video_id)
    caption = get_video_captions(video_id)
    
    return {
        'video_id': video_id,
        'title': video_title,
        'description': video_description,
        'views': views,
        'likes': likes,
        'dislikes': dislikes,
        'upload_date': upload_date,
        'all_comments': all_comments,
        'top_most_liked_comments': top_most_liked_comments,
        'captions': caption
    }


def video_details(api_key, channel_id):
    youtube = build("youtube", 'v3', developerKey=api_key)

    # Step 1: Get Uploads Playlist ID
    channels_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    uploads_playlist_id = channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # Step 2: Get Videos from Uploads Playlist (All Videos)
    videos = []
    next_page_token = None

    with ThreadPoolExecutor() as executor:
        while True:
            playlist_items_response = youtube.playlistItems().list(playlistId=uploads_playlist_id, part='snippet',
                maxResults=50,  # Adjust as needed
                pageToken=next_page_token).execute()

            # Extract Video Details using ThreadPoolExecutor
            video_data = list(executor.map(process_video, playlist_items_response['items']))
            videos.extend(video_data)

            # Check for more pages
            next_page_token = playlist_items_response.get('nextPageToken')
            if not next_page_token:
                break

    return pd.DataFrame(videos)

