# Spotify API Optimization

This module contains optimized functions for working with the Spotify API, which solve problems with exceeding request limits.

## Main improvements

### 1. Rate Limiting
- Automatic detection of exceeding limits
- Smart retry logic with exponential delay
- Extracting waiting time from API error messages

### 2. Caching
- Automatic caching of all API requests
- Cache is stored in `data/cache/` with a 24-hour lifespan
- Reusing data without accessing API

### 3. Request optimization
- Reduced batch size from 50 to 20 elements
- Reduced number of parallel streams from 5 to 2
- Added delays between requests

### 4. Configuration
All parameters are configured in the `api_config.py` file:

```python
# Rate limiting settings
RATE_LIMIT_CONFIG = {
    'max_retries': 3,
    'base_delay': 1,
    'default_retry_time': 60,
    'batch_delay': 0.2,
    'request_delay': 0.1,
}

# Batch processing settings
BATCH_CONFIG = {
    'track_batch_size': 20,
    'artist_batch_size': 20,
    'max_workers': 2,
}

# Cache settings
CACHE_CONFIG = {
    'cache_dir': 'data/cache',
    'cache_expiry_hours': 24,
    'enable_cache': True,
}
```

## Usage

### Main functions

```python
from src.data.spotify_utils import (
    add_track_lengths_to_df,
    add_artist_info_to_df,
    add_genres_to_df
)

# Adding track lengths
df = add_track_lengths_to_df(df)

# Adding track covers, artist images, and genres
df = add_artist_info_to_df(df)

# Adding only genres (if needed separately)
df = add_genres_to_df(df)
```

### Cache management

```python
from src.data.cache_utils import (
    clear_cache,
    list_cache,
    get_cache_info,
    delete_expired_cache
)

# Clear all cache
clear_cache()

# View cache information
cache_info = get_cache_info()
print(cache_info)

# Delete only expired cache files
delete_expired_cache()
```

## Recommendations

### For large datasets
1. Run processing in several stages
2. Use cache for repeated runs
3. Increase delays in `api_config.py` if necessary

### Monitoring
- Watch for exceeding limit messages
- Use `list_cache()` for cache monitoring
- Increase `batch_delay` and `request_delay` if frequent errors occur

### Debugging
- Disable cache: `CACHE_CONFIG['enable_cache'] = False`
- Increase logging in `rate_limited_request()`
- Use smaller batches for testing

## Spotify API limitations

- **Tracks endpoint**: up to 50 tracks per request
- **Artists endpoint**: up to 50 artists per request
- **Rate limits**: ~10 requests per second, ~600 per minute
- **Retry after**: usually 30-60 seconds after exceeding limit

## Troubleshooting

### Common rate limit errors
1. Increase `batch_delay` to 0.5-1.0 seconds
2. Decrease `max_workers` to 1
3. Decrease batch size to 10

### Slow operation
1. Increase `max_workers` to 3-4 (if allowed by API)
2. Decrease delays
3. Use cache for repeated runs

### Cache issues
1. Check access rights to the `data/cache/` folder
2. Clear cache: `clear_cache()`
3. Disable cache temporarily for debugging 