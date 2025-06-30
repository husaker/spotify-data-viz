"""
Cache management utilities for Spotify API data.
"""

import os
import pickle
from pathlib import Path
from .api_config import CACHE_CONFIG

def clear_cache():
    """Clear all cached data."""
    cache_dir = Path(CACHE_CONFIG['cache_dir'])
    if cache_dir.exists():
        for cache_file in cache_dir.glob("*.pkl"):
            try:
                cache_file.unlink()
                print(f"Deleted cache file: {cache_file}")
            except Exception as e:
                print(f"Error deleting {cache_file}: {e}")
        print("Cache cleared successfully")
    else:
        print("Cache directory does not exist")

def list_cache():
    """List all cached files with their sizes and timestamps."""
    cache_dir = Path(CACHE_CONFIG['cache_dir'])
    if not cache_dir.exists():
        print("Cache directory does not exist")
        return
    
    cache_files = list(cache_dir.glob("*.pkl"))
    if not cache_files:
        print("No cache files found")
        return
    
    print(f"Found {len(cache_files)} cache files:")
    print("-" * 80)
    
    for cache_file in sorted(cache_files):
        try:
            size = cache_file.stat().st_size
            size_mb = size / (1024 * 1024)
            
            with open(cache_file, 'rb') as f:
                cached_data = pickle.load(f)
                timestamp = cached_data.get('timestamp', 0)
                import time
                age_hours = (time.time() - timestamp) / 3600
            
            print(f"{cache_file.name:<40} {size_mb:.2f} MB  {age_hours:.1f} hours old")
        except Exception as e:
            print(f"{cache_file.name:<40} Error reading: {e}")

def get_cache_info():
    """Get information about cache usage."""
    cache_dir = Path(CACHE_CONFIG['cache_dir'])
    if not cache_dir.exists():
        return {"total_files": 0, "total_size_mb": 0, "cache_dir": str(cache_dir)}
    
    cache_files = list(cache_dir.glob("*.pkl"))
    total_size = sum(f.stat().st_size for f in cache_files)
    total_size_mb = total_size / (1024 * 1024)
    
    return {
        "total_files": len(cache_files),
        "total_size_mb": total_size_mb,
        "cache_dir": str(cache_dir),
        "cache_enabled": CACHE_CONFIG['enable_cache'],
        "expiry_hours": CACHE_CONFIG['cache_expiry_hours']
    }

def delete_expired_cache():
    """Delete cache files that have expired."""
    cache_dir = Path(CACHE_CONFIG['cache_dir'])
    if not cache_dir.exists():
        print("Cache directory does not exist")
        return
    
    import time
    expiry_seconds = CACHE_CONFIG['cache_expiry_hours'] * 3600
    current_time = time.time()
    deleted_count = 0
    
    for cache_file in cache_dir.glob("*.pkl"):
        try:
            with open(cache_file, 'rb') as f:
                cached_data = pickle.load(f)
                timestamp = cached_data.get('timestamp', 0)
                
                if current_time - timestamp > expiry_seconds:
                    cache_file.unlink()
                    print(f"Deleted expired cache: {cache_file.name}")
                    deleted_count += 1
        except Exception as e:
            print(f"Error processing {cache_file.name}: {e}")
    
    print(f"Deleted {deleted_count} expired cache files")

def save_enriched_df(df, cache_path):
    """Сохраняет обогащённый DataFrame в pickle-файл."""
    with open(cache_path, 'wb') as f:
        pickle.dump(df, f)

def load_enriched_df(cache_path):
    """Загружает обогащённый DataFrame из pickle-файла, если он существует."""
    if os.path.exists(cache_path):
        with open(cache_path, 'rb') as f:
            return pickle.load(f)
    return None

def save_track_cache(track_cache, path):
    with open(path, 'wb') as f:
        pickle.dump(track_cache, f)

def load_track_cache(path):
    if not os.path.exists(path):
        return {}
    with open(path, 'rb') as f:
        return pickle.load(f)

def save_artist_cache(artist_cache, path):
    with open(path, 'wb') as f:
        pickle.dump(artist_cache, f)

def load_artist_cache(path):
    if not os.path.exists(path):
        return {}
    with open(path, 'rb') as f:
        return pickle.load(f) 