"""
Configuration file for Spotify API settings and rate limiting parameters.
"""

# Rate limiting settings
RATE_LIMIT_CONFIG = {
    'max_retries': 3,
    'base_delay': 1,
    'default_retry_time': 60,  # seconds
    'batch_delay': 0.2,  # seconds between batches
    'request_delay': 0.1,  # seconds between individual requests
}

# Batch processing settings
BATCH_CONFIG = {
    'track_batch_size': 20,  # Reduced from 50
    'artist_batch_size': 20,  # Reduced from 50
    'max_workers': 2,  # Reduced from 5
}

# Cache settings
CACHE_CONFIG = {
    'cache_dir': 'data/cache',
    'cache_expiry_hours': 24,  # Cache expires after 24 hours
    'enable_cache': True,
}

# API endpoint limits (for reference)
SPOTIFY_LIMITS = {
    'tracks_per_request': 50,
    'artists_per_request': 50,
    'requests_per_second': 10,  # Conservative estimate
    'requests_per_minute': 600,
} 