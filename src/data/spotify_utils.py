import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_spotify_client():
    """
    Initialize and return a Spotify client using client credentials flow.
    """
    load_dotenv()
    
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        raise ValueError("Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in .env file")
    
    return spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    ))

def get_tracks_batch(track_ids, sp):
    """
    Get track information for a batch of tracks.
    
    Args:
        track_ids (list): List of Spotify track IDs
        sp (spotipy.Spotify): Spotify client instance
    
    Returns:
        dict: Dictionary mapping track IDs to their durations
    """
    try:
        tracks = sp.tracks(track_ids)
        return {track['id']: track['duration_ms'] for track in tracks['tracks'] if track is not None}
    except Exception as e:
        print(f"Error getting batch of tracks: {str(e)}")
        return {}

def process_batch(batch_info):
    """
    Process a batch of track IDs.
    
    Args:
        batch_info (tuple): Tuple containing (batch_ids, sp)
    
    Returns:
        dict: Dictionary mapping track IDs to their durations
    """
    batch_ids, sp = batch_info
    return get_tracks_batch(batch_ids, sp)

def add_track_lengths_to_df(df):
    """
    Add track lengths to the DataFrame using parallel batch processing.
    
    Args:
        df (pd.DataFrame): DataFrame containing Spotify track IDs
    
    Returns:
        pd.DataFrame: DataFrame with added 'duration_ms' column
    """
    # Create a copy to avoid modifying the original
    df_with_lengths = df.copy()
    
    # Get unique track IDs
    unique_track_ids = df_with_lengths['Spotify ID'].unique()
    
    print(f"\nFetching {len(unique_track_ids)} tracks from Spotify...")
    
    # Initialize Spotify client
    sp = get_spotify_client()
    
    # Process tracks in batches of 50 (Spotify API limit)
    BATCH_SIZE = 50
    batches = [(unique_track_ids[i:i + BATCH_SIZE], sp) 
              for i in range(0, len(unique_track_ids), BATCH_SIZE)]
    
    # Process batches in parallel
    track_durations = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_batch, batch) for batch in batches]
        
        # Process results as they complete
        for future in tqdm(as_completed(futures), total=len(batches), desc="Processing batches"):
            batch_durations = future.result()
            track_durations.update(batch_durations)
    
    # Map durations back to the DataFrame
    df_with_lengths['duration_ms'] = df_with_lengths['Spotify ID'].map(track_durations)
    
    # Convert milliseconds to minutes for better readability
    df_with_lengths['duration_min'] = df_with_lengths['duration_ms'].apply(
        lambda x: round(x/60000, 2) if x is not None else None
    )
    
    return df_with_lengths 

def get_tracks_and_artists_batch(track_ids, sp):
    """
    Получить обложку трека и artist_id для батча треков.
    Возвращает dict: track_id -> (cover_url, artist_id)
    """
    try:
        tracks = sp.tracks(track_ids)
        result = {}
        for track in tracks['tracks']:
            if track is not None:
                # Берём первую (самую крупную) картинку альбома
                cover_url = track['album']['images'][0]['url'] if track['album']['images'] else None
                artist_id = track['artists'][0]['id'] if track['artists'] else None
                result[track['id']] = (cover_url, artist_id)
        return result
    except Exception as e:
        print(f"Error getting batch of tracks for images: {str(e)}")
        return {}

def get_artists_images_batch(artist_ids, sp):
    """
    Получить картинки профиля артистов батчем через sp.artists (до 50 id за раз).
    Возвращает dict: artist_id -> artist_image_url
    """
    try:
        result = {}
        artist_ids = list(artist_ids)
        BATCH_SIZE = 50
        for i in range(0, len(artist_ids), BATCH_SIZE):
            batch_ids = artist_ids[i:i+BATCH_SIZE]
            try:
                artists = sp.artists(batch_ids)['artists']
                for artist in artists:
                    image_url = artist['images'][0]['url'] if artist['images'] else None
                    result[artist['id']] = image_url
            except Exception as e:
                print(f"Error getting artists batch {batch_ids}: {str(e)}")
                for artist_id in batch_ids:
                    result[artist_id] = None
        return result
    except Exception as e:
        print(f"Error in get_artists_images_batch: {str(e)}")
        return {}

def process_images_batch(batch_info):
    batch_ids, sp = batch_info
    return get_tracks_and_artists_batch(batch_ids, sp)

def add_images_to_df(df):
    """
    Добавляет в DataFrame колонки track_cover_url и artist_image_url, используя Spotify API.
    Работает параллельно батчами.
    """
    df_with_images = df.copy()
    unique_track_ids = df_with_images['Spotify ID'].unique()
    print(f"\nFetching cover images and artist ids for {len(unique_track_ids)} tracks from Spotify...")
    sp = get_spotify_client()
    BATCH_SIZE = 50
    batches = [(unique_track_ids[i:i + BATCH_SIZE], sp)
              for i in range(0, len(unique_track_ids), BATCH_SIZE)]
    track_covers = {}
    track_to_artist = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_images_batch, batch) for batch in batches]
        for future in tqdm(as_completed(futures), total=len(batches), desc="Processing image batches"):
            batch_result = future.result()
            for track_id, (cover_url, artist_id) in batch_result.items():
                track_covers[track_id] = cover_url
                track_to_artist[track_id] = artist_id
    df_with_images['track_cover_url'] = df_with_images['Spotify ID'].map(track_covers)
    # Получаем уникальные artist_id
    unique_artist_ids = set([aid for aid in track_to_artist.values() if aid])
    print(f"\nFetching artist images for {len(unique_artist_ids)} artists from Spotify...")
    artist_images = get_artists_images_batch(unique_artist_ids, sp)
    # Маппим artist_image_url по artist_id для каждой строки
    df_with_images['artist_image_url'] = df_with_images['Spotify ID'].map(lambda tid: artist_images.get(track_to_artist.get(tid)))
    return df_with_images 