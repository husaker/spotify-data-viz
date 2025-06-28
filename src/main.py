import os
from dotenv import load_dotenv
from data.load_data import load_spotify_data_from_sheets
from data.spotify_utils import add_track_lengths_to_df, add_images_to_df
import pandas as pd

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Configuration
    SHEET_URL = os.getenv('GOOGLE_SHEET_URL')
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH')
    
    if not SHEET_URL or not CREDENTIALS_PATH:
        raise ValueError("Please set GOOGLE_SHEET_URL and GOOGLE_CREDENTIALS_PATH in .env file")
    
    try:
        # Load data from Google Sheets
        df = load_spotify_data_from_sheets(SHEET_URL, CREDENTIALS_PATH)
        
        # Add track lengths
        print("\nFetching track lengths from Spotify...")
        df = add_track_lengths_to_df(df)
        
        # Add track and artist images
        print("\nFetching track and artist images from Spotify...")
        df = add_images_to_df(df)
        
        # Basic data validation
        print("\nData Overview:")
        print(f"Total number of tracks: {len(df)}")
        print(f"Date range: from {df['Date'].min()} to {df['Date'].max()}")
        print(f"Number of unique artists: {df['Artist'].nunique()}")
        
        # Display first few rows
        print("\nFirst 5 rows of the data:")
        print(df.head())
        
        # Show example image URLs
        print("\nExample track cover URL:")
        print(df['track_cover_url'].dropna().iloc[0] if df['track_cover_url'].notna().any() else "No cover found")
        print("Example artist image URL:")
        print(df['artist_image_url'].dropna().iloc[0] if df['artist_image_url'].notna().any() else "No artist image found")
        
        # Basic statistics
        print("\nMost listened artists:")
        print(df['Artist'].value_counts().head())
        
        # Track length statistics
        print("\nTrack length statistics (minutes):")
        print(f"Average track length: {df['duration_min'].mean():.2f}")
        print(f"Shortest track: {df['duration_min'].min():.2f}")
        print(f"Longest track: {df['duration_min'].max():.2f}")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    main() 