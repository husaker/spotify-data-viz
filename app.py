import streamlit as st
import pandas as pd
from src.data.load_data import load_spotify_data_from_sheets
from src.data.spotify_utils import add_track_lengths_to_df, add_images_to_df
import os
from dotenv import load_dotenv

# Загрузка и обработка данных (кэшируем для ускорения)
@st.cache_data
def load_and_enrich_data():
    load_dotenv()
    SHEET_URL = os.getenv('GOOGLE_SHEET_URL')
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH')
    if not SHEET_URL or not CREDENTIALS_PATH:
        st.error('GOOGLE_SHEET_URL и GOOGLE_CREDENTIALS_PATH не заданы в .env')
        st.stop()
    df = load_spotify_data_from_sheets(SHEET_URL, CREDENTIALS_PATH)
    df = add_track_lengths_to_df(df)
    df = add_images_to_df(df)
    return df

def filter_by_date(df, date_from, date_to):
    return df[(df['Date'] >= date_from) & (df['Date'] <= date_to)]

def top_artists(df, n=5):
    grouped = df.groupby('Artist').agg({
        'Spotify ID': 'count',
        'duration_min': 'sum',
        'artist_image_url': 'first'
    }).rename(columns={'Spotify ID': 'listened_tracks', 'duration_min': 'listened_minutes'})
    return grouped.sort_values('listened_tracks', ascending=False).head(n).reset_index()

def top_tracks(df, n=5):
    grouped = df.groupby(['Track', 'Artist']).agg({
        'Spotify ID': 'count',
        'duration_min': 'sum',
        'track_cover_url': 'first'
    }).rename(columns={'Spotify ID': 'times_listened', 'duration_min': 'minutes_listened'})
    return grouped.sort_values('times_listened', ascending=False).head(n).reset_index()

def show_top_artists(df):
    st.subheader('Top 5 Artists')
    top = top_artists(df)
    for _, row in top.iterrows():
        cols = st.columns([1, 3])
        with cols[0]:
            if pd.notna(row['artist_image_url']):
                st.image(row['artist_image_url'], width=130)
            else:
                st.write('No image')
        with cols[1]:
            st.markdown(f"<span style='font-size:22px; font-weight:bold'>{row['Artist']}</span>", unsafe_allow_html=True)
            st.markdown(f"<span style='color:gray'>Listened tracks: {int(row['listened_tracks'])}</span>", unsafe_allow_html=True)
            st.markdown(f"<span style='color:gray'>Listened minutes: {int(row['listened_minutes'])}</span>", unsafe_allow_html=True)

def show_top_tracks(df):
    st.subheader('Top 5 Tracks')
    top = top_tracks(df)
    for _, row in top.iterrows():
        cols = st.columns([1, 3])
        with cols[0]:
            if pd.notna(row['track_cover_url']):
                st.image(row['track_cover_url'], width=150)
            else:
                st.write('No cover')
        with cols[1]:
            st.markdown(f"<span style='font-size:22px; font-weight:bold'>{row['Track']}</span>", unsafe_allow_html=True)
            st.markdown(
                f"<span style='color:#888; font-weight:bold; font-size:18px'>{row['Artist']}</span>",
                unsafe_allow_html=True
            )
            st.markdown(f"<span style='color:gray'>Times listened: {int(row['times_listened'])}</span>", unsafe_allow_html=True)
            st.markdown(f"<span style='color:gray'>Minutes listened: {int(row['minutes_listened'])}</span>", unsafe_allow_html=True)

def main():
    st.title('Spotify Listening Visualization')
    df = load_and_enrich_data()
    # Фильтр по дате
    min_date, max_date = df['Date'].min().date(), df['Date'].max().date()
    date_from, date_to = st.date_input('Filter: from/to', [min_date, max_date], min_value=min_date, max_value=max_date)
    filtered_df = filter_by_date(df, pd.to_datetime(date_from), pd.to_datetime(date_to))
    # Вкладки для переключения между графиками
    tab1, tab2 = st.tabs(['Top 5 Artists', 'Top 5 Tracks'])
    with tab1:
        show_top_artists(filtered_df)
    with tab2:
        show_top_tracks(filtered_df)

if __name__ == '__main__':
    main() 