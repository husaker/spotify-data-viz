import os
from dotenv import load_dotenv
from data.load_data import load_spotify_data_from_sheets
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
        
        # Basic data validation
        print("\nData Overview:")
        print(f"Total number of tracks: {len(df)}")
        print(f"Date range: from {df['Date'].min()} to {df['Date'].max()}")
        print(f"Number of unique artists: {df['Artist'].nunique()}")
        
        # Display first few rows
        print("\nFirst 5 rows of the data:")
        print(df.head())
        
        # Basic statistics
        print("\nMost listened artists:")
        print(df['Artist'].value_counts().head())
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    main() 