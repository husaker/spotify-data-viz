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

def enrich_tracks(track_ids):
    """
    Enrich track info for a list of Spotify IDs. Returns dict {Spotify ID: {duration_ms, duration_min, cover_url, artist_id}}
    """
    sp = get_spotify_client()
    result = {}
    BATCH_SIZE = 50
    for i in range(0, len(track_ids), BATCH_SIZE):
        batch = track_ids[i:i+BATCH_SIZE]
        tracks = sp.tracks(batch)['tracks']
        for track in tracks:
            if track is not None:
                duration_ms = track['duration_ms']
                duration_min = round(duration_ms/60000, 2) if duration_ms is not None else None
                cover_url = track['album']['images'][0]['url'] if track['album']['images'] else None
                artist_id = track['artists'][0]['id'] if track['artists'] else None
                result[track['id']] = {
                    'duration_ms': duration_ms,
                    'duration_min': duration_min,
                    'track_cover_url': cover_url,
                    'artist_id': artist_id
                }
    return result

def enrich_artists(artist_ids):
    """
    Enrich artist info for a list of artist IDs. Returns dict {artist_id: {artist_image_url, genre}}
    """
    sp = get_spotify_client()
    result = {}
    BATCH_SIZE = 50
    for i in range(0, len(artist_ids), BATCH_SIZE):
        batch = artist_ids[i:i+BATCH_SIZE]
        artists = sp.artists(batch)['artists']
        for artist in artists:
            if artist is not None:
                image_url = artist['images'][0]['url'] if artist['images'] else None
                genre = artist['genres'][0] if artist['genres'] else None
                result[artist['id']] = {
                    'artist_image_url': image_url,
                    'genre': genre
                }
    return result 