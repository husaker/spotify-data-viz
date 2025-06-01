import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

def load_spotify_data_from_sheets(sheet_url, credentials_path):
    """
    Load Spotify listening data from Google Sheets into a pandas DataFrame.
    
    Args:
        sheet_url (str): URL of the Google Sheet
        credentials_path (str): Path to the Google Sheets API credentials JSON file
    
    Returns:
        pd.DataFrame: DataFrame containing the Spotify listening data
    """
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    # Add credentials to the account
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
    
    # Open the sheet
    sheet = client.open_by_url(sheet_url).sheet1
    
    # Get all values
    data = sheet.get_all_records()
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Convert date column to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    return df 