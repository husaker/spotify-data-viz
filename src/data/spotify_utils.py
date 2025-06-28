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

def add_track_lengths_to_df(df, max_workers=5):
    """
    Add track lengths to the DataFrame using parallel batch processing.
    Args:
        df (pd.DataFrame): DataFrame containing Spotify track IDs
        max_workers (int): Number of threads for parallel processing.
    Returns:
        pd.DataFrame: DataFrame with added 'duration_ms' column
    """
    df_with_lengths = df.copy()
    unique_track_ids = df_with_lengths['Spotify ID'].unique()
    print(f"\nFetching {len(unique_track_ids)} tracks from Spotify...")
    sp = get_spotify_client()
    BATCH_SIZE = 50
    batches = [(unique_track_ids[i:i + BATCH_SIZE], sp) 
              for i in range(0, len(unique_track_ids), BATCH_SIZE)]
    track_durations = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_batch, batch) for batch in batches]
        for future in as_completed(futures):
            batch_durations = future.result()
            track_durations.update(batch_durations)
    df_with_lengths['duration_ms'] = df_with_lengths['Spotify ID'].map(track_durations)
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

def add_artist_info_to_df(df, max_workers=5):
    """
    Добавляет в DataFrame колонки track_cover_url, artist_image_url и genre, используя Spotify API.
    Работает параллельно батчами.
    max_workers (int): Number of threads for parallel processing.
    """
    df_with_info = df.copy()
    unique_track_ids = df_with_info['Spotify ID'].unique()
    print(f"\nFetching cover images and artist ids for {len(unique_track_ids)} tracks from Spotify...")
    sp = get_spotify_client()
    BATCH_SIZE = 50
    batches = [(unique_track_ids[i:i + BATCH_SIZE], sp)
              for i in range(0, len(unique_track_ids), BATCH_SIZE)]
    track_covers = {}
    track_to_artist = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_images_batch, batch) for batch in batches]
        for future in as_completed(futures):
            batch_result = future.result()
            for track_id, (cover_url, artist_id) in batch_result.items():
                track_covers[track_id] = cover_url
                track_to_artist[track_id] = artist_id
    df_with_info['track_cover_url'] = df_with_info['Spotify ID'].map(track_covers)
    unique_artist_ids = set([aid for aid in track_to_artist.values() if aid])
    print(f"\nFetching artist images and genres for {len(unique_artist_ids)} artists from Spotify...")
    artist_images = {}
    artist_genres = {}
    BATCH_SIZE = 50
    artist_ids_list = list(unique_artist_ids)
    for i in range(0, len(artist_ids_list), BATCH_SIZE):
        batch_ids = artist_ids_list[i:i+BATCH_SIZE]
        try:
            artists = sp.artists(batch_ids)['artists']
            for artist in artists:
                image_url = artist['images'][0]['url'] if artist['images'] else None
                genre = artist['genres'][0] if artist['genres'] else None
                artist_images[artist['id']] = image_url
                artist_genres[artist['id']] = genre
        except Exception as e:
            print(f"Error getting artists batch {batch_ids}: {str(e)}")
            for artist_id in batch_ids:
                artist_images[artist_id] = None
                artist_genres[artist_id] = None
    df_with_info['artist_image_url'] = df_with_info['Spotify ID'].map(lambda tid: artist_images.get(track_to_artist.get(tid)))
    df_with_info['genre'] = df_with_info['Spotify ID'].map(lambda tid: artist_genres.get(track_to_artist.get(tid)))
    return df_with_info

# Алиас для обратной совместимости
add_images_to_df = add_artist_info_to_df

def get_artists_genres_batch(artist_ids, sp):
    """
    Получить жанры артистов батчем через sp.artists (до 50 id за раз).
    Возвращает dict: artist_id -> genre (первый жанр или None)
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
                    genre = artist['genres'][0] if artist['genres'] else None
                    result[artist['id']] = genre
            except Exception as e:
                print(f"Error getting artists batch {batch_ids}: {str(e)}")
                for artist_id in batch_ids:
                    result[artist_id] = None
        return result
    except Exception as e:
        print(f"Error in get_artists_genres_batch: {str(e)}")
        return {}

def add_genres_to_df(df):
    """
    Добавляет в DataFrame колонку 'genre' по artist_id через Spotify API.
    Работает батчами и параллельно.
    """
    df_with_genres = df.copy()
    # Получаем artist_id для каждого трека (если нет - None)
    if 'Spotify ID' not in df_with_genres.columns:
        raise ValueError('DataFrame должен содержать колонку Spotify ID')
    if 'artist_image_url' not in df_with_genres.columns:
        raise ValueError('Сначала обогатите DataFrame функцией add_images_to_df')
    # Получаем artist_id через уже существующий маппинг
    from tqdm import tqdm
    from concurrent.futures import ThreadPoolExecutor, as_completed
    sp = get_spotify_client()
    # Получаем artist_id для каждого трека
    if 'artist_id' not in df_with_genres.columns:
        # Вытаскиваем через cover enrichment (track_to_artist)
        # Но проще повторно получить через API, если нет
        def get_artist_id(spotify_id):
            try:
                track = sp.track(spotify_id)
                return track['artists'][0]['id'] if track['artists'] else None
            except:
                return None
        df_with_genres['artist_id'] = df_with_genres['Spotify ID'].map(get_artist_id)
    unique_artist_ids = set(df_with_genres['artist_id'].dropna())
    print(f"\nFetching genres for {len(unique_artist_ids)} artists from Spotify...")
    artist_genres = get_artists_genres_batch(unique_artist_ids, sp)
    df_with_genres['genre'] = df_with_genres['artist_id'].map(artist_genres)
    return df_with_genres 