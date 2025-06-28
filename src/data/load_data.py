import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import os
import tempfile
import json
from dotenv import load_dotenv

def load_spotify_data_from_sheets(sheet_url, credentials_path):
    """
    Load Spotify listening data from Google Sheets into a pandas DataFrame.
    
    Args:
        sheet_url (str): URL of the Google Sheet
        credentials_path (str): Path to the Google Sheets API credentials JSON file
    
    Returns:
        pd.DataFrame: DataFrame containing the Spotify listening data
    """
    load_dotenv()

    # Check for credentials in environment variable
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if credentials_json:
        credentials_dict = json.loads(credentials_json)
        if "private_key" in credentials_dict:
            credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
        credentials_json = json.dumps(credentials_dict)
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
            tmp.write(credentials_json)
            tmp.flush()
            credentials_path = tmp.name
    
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    # Add credentials to the account
    print("DEBUG: credentials_path =", credentials_path)
    with open(credentials_path, "r") as f:
        print("DEBUG: credentials file content:")
        print(f.read())
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