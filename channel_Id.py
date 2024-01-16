import pandas as pd
import requests
import re
from concurrent.futures import ThreadPoolExecutor

def get_channel_id(video_url, channel_name):
    response = requests.get(video_url)
    
    if response.status_code == 200:
        match = re.search(r'"channelId":"(.*?)"', response.text)

        if match:
            channel_id = match.group(1)
            return {'channel_name': channel_name, 'channel_id': channel_id}

    return None

def get_channel_ids_from_video_urls(df):
    all_channel_ids = []

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(get_channel_id, df['video_url'], df['channel_name']))

    for result in results:
        if result:
            all_channel_ids.append(result)

    return pd.DataFrame(all_channel_ids)

