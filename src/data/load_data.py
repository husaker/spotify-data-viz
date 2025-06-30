import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import os
import tempfile
import json
from .spotify_utils import enrich_tracks, enrich_artists
from .cache_utils import load_track_cache, save_track_cache, load_artist_cache, save_artist_cache

def load_spotify_data_from_sheets(sheet_url):
    """
    Load Spotify listening data from Google Sheets into a pandas DataFrame.
    Args:
        sheet_url (str): URL of the Google Sheet
    Returns:
        pd.DataFrame: DataFrame containing the Spotify listening data
    """
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not credentials_json:
        raise ValueError('GOOGLE_CREDENTIALS_JSON is not set in environment!')
    credentials_dict = json.loads(credentials_json)
    if "private_key" in credentials_dict:
        credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    credentials_json = json.dumps(credentials_dict)
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
        tmp.write(credentials_json)
        tmp.flush()
        credentials_path = tmp.name
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'])
    return df

def get_enriched_spotify_data(sheet_url, track_cache_path="data/cache/track_info.pkl", artist_cache_path="data/cache/artist_info.pkl"):
    raw_df = load_spotify_data_from_sheets(sheet_url).copy()
    print("raw_df rows:", len(raw_df))
    # 1. Track enrichment
    track_cache = load_track_cache(track_cache_path)
    unique_track_ids = set(raw_df['Spotify ID'].dropna())
    missing_track_ids = unique_track_ids - set(track_cache.keys())
    if missing_track_ids:
        new_track_info = enrich_tracks(list(missing_track_ids))
        track_cache.update(new_track_info)
        save_track_cache(track_cache, track_cache_path)
    # 2. Artist enrichment
    all_artist_ids = set()
    for tid in unique_track_ids:
        if tid in track_cache and track_cache[tid].get('artist_id'):
            all_artist_ids.add(track_cache[tid]['artist_id'])
    artist_cache = load_artist_cache(artist_cache_path)
    missing_artist_ids = all_artist_ids - set(artist_cache.keys())
    if missing_artist_ids:
        new_artist_info = enrich_artists(list(missing_artist_ids))
        artist_cache.update(new_artist_info)
        save_artist_cache(artist_cache, artist_cache_path)
    # 3. Merge enrichment into raw_df
    track_enrichment_df = (
        pd.DataFrame.from_dict(track_cache, orient='index')
        .reset_index().rename(columns={'index': 'Spotify ID'})
    )
    df = raw_df.merge(track_enrichment_df, on='Spotify ID', how='left')
    print("After track merge rows:", len(df))
    artist_enrichment_df = (
        pd.DataFrame.from_dict(artist_cache, orient='index')
        .reset_index().rename(columns={'index': 'artist_id'})
    )
    df = df.merge(artist_enrichment_df, on='artist_id', how='left')

    return df