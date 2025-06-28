import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import os
import tempfile
import json

def load_spotify_data_from_sheets(sheet_url):
    """
    Load Spotify listening data from Google Sheets into a pandas DataFrame.
    Args:
        sheet_url (str): URL of the Google Sheet
    Returns:
        pd.DataFrame: DataFrame containing the Spotify listening data
    """
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not credentials_json:
        raise ValueError('GOOGLE_CREDENTIALS_JSON is not set in environment!')
    credentials_dict = json.loads(credentials_json)
    if "private_key" in credentials_dict:
        credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    credentials_json = json.dumps(credentials_dict)
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
        tmp.write(credentials_json)
        tmp.flush()
        credentials_path = tmp.name
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'])
    return df