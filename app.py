import streamlit as st
import pandas as pd
from src.data.load_data import get_enriched_spotify_data
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import datetime
import matplotlib.patheffects as pe

st.set_page_config(layout="centered")

# Custom style for headers and hiding anchor icon
st.markdown('''
    <style>
    .spotify-green-title {
        color: #1DB954 !important;
        font-weight: bold;
    }
    .stTabs [data-baseweb="tab"] > div {
        color: #1DB954 !important;
        font-weight: bold;
    }
    /* Hide anchor icon for all h1, h2, h3 headers */
    .stMarkdown h1 a, .stMarkdown h2 a, .stMarkdown h3 a {
        display: none !important;
    }
    </style>
''', unsafe_allow_html=True)

def load_and_enrich_data():
    load_dotenv()
    SHEET_URL = os.getenv('GOOGLE_SHEET_URL')
    if not SHEET_URL:
        st.error('GOOGLE_SHEET_URL is not set in .env')
        st.stop()
    return get_enriched_spotify_data(SHEET_URL)

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
    top = top_artists(df)
    for _, row in top.iterrows():
        cols = st.columns([1, 3])
        with cols[0]:
            if pd.notna(row['artist_image_url']):
                st.image(row['artist_image_url'], width=150)
            else:
                st.write('No image')
        with cols[1]:
            st.markdown(f"<span style='font-size:22px; font-weight:bold'>{row['Artist']}</span>", unsafe_allow_html=True)
            st.markdown(f"<span style='color:gray'>Listened tracks: {int(row['listened_tracks'])}</span>", unsafe_allow_html=True)
            st.markdown(f"<span style='color:gray'>Listened minutes: {int(row['listened_minutes'])}</span>", unsafe_allow_html=True)

def show_top_tracks(df):
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
            st.markdown(f"<span style='color:#888; font-weight:bold; font-size:18px'>{row['Artist']}</span>", unsafe_allow_html=True)
            st.markdown(f"<span style='color:gray'>Times listened: {int(row['times_listened'])}</span>", unsafe_allow_html=True)
            st.markdown(f"<span style='color:gray'>Minutes listened: {int(row['minutes_listened'])}</span>", unsafe_allow_html=True)

def plot_weekly_avg_charts(df):
    # --- Подготовка ---
    sns.set_theme(style="dark", rc={
        "axes.facecolor": "#191414",
        "figure.facecolor": "#191414",
        "axes.labelcolor": "#fff",
        "xtick.color": "#fff",
        "ytick.color": "#fff",
        "text.color": "#fff"
    })

    # Приводим Date к datetime и округляем до дня
    df['Date'] = pd.to_datetime(df['Date']).dt.floor('D')

    # Если нет колонки duration_min, можно рассчитать, если есть duration_ms или duration_sec
    if 'duration_min' not in df.columns:
        if 'duration_ms' in df.columns:
            df['duration_min'] = df['duration_ms'] / 1000 / 60
        elif 'duration_sec' in df.columns:
            df['duration_min'] = df['duration_sec'] / 60
        else:
            df['duration_min'] = 0  # fallback

    # --- Tracks ---
    daily_tracks = df.groupby('Date').size().rename('tracks').reset_index()
    daily_tracks['week'] = daily_tracks['Date'].dt.isocalendar().week
    daily_tracks['year'] = daily_tracks['Date'].dt.isocalendar().year

    weekly_tracks = (
        daily_tracks.groupby(['year', 'week'])
        .agg(
            total_tracks=('tracks', 'sum'),
            active_days=('Date', 'nunique')
        )
        .reset_index()
    )
    weekly_tracks['avg_daily_tracks'] = weekly_tracks['total_tracks'] / weekly_tracks['active_days']
    weekly_tracks['week_start'] = pd.to_datetime(
        weekly_tracks['year'].astype(str) + weekly_tracks['week'].astype(str) + '1',
        format='%G%V%u'
    )

    fig, ax = plt.subplots(figsize=(8, 4), dpi=200)
    line, = ax.plot(weekly_tracks['week_start'], weekly_tracks['avg_daily_tracks'], color='#1DB954', linewidth=3)
    line.set_path_effects([pe.Stroke(linewidth=8, foreground='#1DB954', alpha=0.18), pe.Normal()])
    ax.set_facecolor('#191414')
    fig.patch.set_facecolor('#191414')
    ax.grid(axis='y', color='#1DB954', alpha=0.08)
    ax.grid(axis='x', visible=False)
    ax.set_title('Average Daily Tracks listened per Week', color='#1DB954', fontsize=18, fontweight='bold')
    ax.set_ylabel('Tracks (avg/day)', color='w', fontsize=12)
    ax.set_xlabel('')
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', color='w')
    plt.setp(ax.get_yticklabels(), color='w')
    ax.plot(
        weekly_tracks['week_start'].iloc[-1],
        weekly_tracks['avg_daily_tracks'].iloc[-1],
        'o', color='white', markersize=8, markeredgewidth=2, markeredgecolor='#1DB954'
    )
    st.pyplot(fig)

    # --- Minutes ---
    daily_minutes = df.groupby('Date')['duration_min'].sum().reset_index()
    daily_minutes['week'] = daily_minutes['Date'].dt.isocalendar().week
    daily_minutes['year'] = daily_minutes['Date'].dt.isocalendar().year

    weekly_minutes = (
        daily_minutes.groupby(['year', 'week'])
        .agg(
            total_minutes=('duration_min', 'sum'),
            active_days=('Date', 'nunique')
        )
        .reset_index()
    )
    weekly_minutes['avg_daily_minutes'] = weekly_minutes['total_minutes'] / weekly_minutes['active_days']
    weekly_minutes['week_start'] = pd.to_datetime(
        weekly_minutes['year'].astype(str) + weekly_minutes['week'].astype(str) + '1',
        format='%G%V%u'
    )

    fig, ax = plt.subplots(figsize=(8, 4), dpi=200)
    line, = ax.plot(weekly_minutes['week_start'], weekly_minutes['avg_daily_minutes'], color='#1DB954', linewidth=3)
    line.set_path_effects([pe.Stroke(linewidth=8, foreground='#1DB954', alpha=0.18), pe.Normal()])
    ax.set_facecolor('#191414')
    fig.patch.set_facecolor('#191414')
    ax.grid(axis='y', color='#1DB954', alpha=0.08)
    ax.grid(axis='x', visible=False)
    ax.set_title('Average Daily Minutes listened per Week', color='#1DB954', fontsize=18, fontweight='bold')
    ax.set_ylabel('Minutes (avg/day)', color='w', fontsize=12)
    ax.set_xlabel('')
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', color='w')
    plt.setp(ax.get_yticklabels(), color='w')
    ax.plot(
        weekly_minutes['week_start'].iloc[-1],
        weekly_minutes['avg_daily_minutes'].iloc[-1],
        'o', color='white', markersize=8, markeredgewidth=2, markeredgecolor='#1DB954'
    )
    st.pyplot(fig)

def show_statistics(df):
    col1, col2 = st.columns(2)
    with col1:
        st.metric('Unique Artists listened', df['Artist'].nunique())
        st.metric('Unique Tracks played', df['Track'].nunique())
        st.metric('Total Tracks Played', len(df))
    with col2:
        st.metric('Total Minutes listened', int(df['duration_min'].sum()))
        st.metric('Active Days', df['Date'].dt.date.nunique())
        if 'genre' in df.columns and df['genre'].notna().any():
            top_genre = df['genre'].value_counts().idxmax()
            st.metric('Favorite Genre', f'{top_genre}')
        else:
            st.metric('Favorite Genre', 'N/A')

def plot_top_genres(df):
    sns.set_theme(style="dark", rc={"axes.facecolor": "#191414", "figure.facecolor": "#191414", "axes.labelcolor": "#fff", "xtick.color": "#fff", "ytick.color": "#fff", "text.color": "#fff"})
    genre_counts = df['genre'].value_counts().dropna().head(5)
    colors = ["#1DB954"] + ["#1ed760"]*4  # First column brighter
    genre_labels = genre_counts.index.to_list()
    fig, ax = plt.subplots(figsize=(8, 4), dpi=400)
    bars = sns.barplot(
        x=genre_counts.values,
        y=genre_labels,
        hue=genre_labels,
        palette=colors,
        ax=ax,
        legend=False
    )
    # Annotate each bar with the number of tracks inside the bar, near the right end
    for i, (value, label) in enumerate(zip(genre_counts.values, genre_labels)):
        ax.text(value * 0.90, i, str(value), va='center', ha='center', color='#191414', fontsize=12, fontweight='bold')
    ax.set_title('Top 5 Genres by Tracks Played', color='#1DB954', fontsize=16, fontweight='bold')
    ax.set_xlabel('Tracks', color='w', fontsize=12)
    ax.set_ylabel('Genre', color='w', fontsize=12)
    plt.setp(ax.get_xticklabels(), color='w')
    plt.setp(ax.get_yticklabels(), color='w')
    fig.patch.set_facecolor('#191414')
    st.pyplot(fig)

def main():
    st.markdown('<h1 class="spotify-green-title">Spotify Listening Visualization</h1>', unsafe_allow_html=True)
    df = load_and_enrich_data()

    min_date = df['Date'].min().date()
    max_date = df['Date'].max().date()

    # Session state for date filter
    if 'date_from' not in st.session_state:
        st.session_state['date_from'] = min_date
    if 'date_to' not in st.session_state:
        st.session_state['date_to'] = max_date


    col_reset, col_link = st.columns([1, 1])
    # Reset button
    with col_reset:
        if st.button('Reset date filter'):
            st.session_state['date_from'] = min_date
            st.session_state['date_to'] = max_date

    with col_link:
        st.link_button('Raw data', 'https://docs.google.com/spreadsheets/d/1-KX5LX6IBY8t5KV-9GN3RcuySFYIZu8F_XN2elH2B_c/edit?usp=sharing')

    col1, col2 = st.columns(2)
    with col1:
        st.date_input('From', min_value=min_date, max_value=max_date, key='date_from')
    with col2:
        st.date_input('To', min_value=min_date, max_value=max_date, key='date_to')

    date_from = st.session_state['date_from']
    date_to = st.session_state['date_to']

    if date_from > date_to:
        st.error('Start date cannot be later than end date!')
        return

    # Filter: include all times on the 'To' date
    start_dt = pd.to_datetime(date_from)
    end_dt = pd.to_datetime(date_to) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    filtered_df = df[(df['Date'] >= start_dt) & (df['Date'] <= end_dt)]

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        'Top 5 Artists',
        'Top 5 Tracks',
        'Average Daily Charts',
        'Top 5 Genres',
        'Statistics',
    ])
    with tab1:
        st.markdown('<h2 class="spotify-green-title">Top 5 Artists</h2>', unsafe_allow_html=True)
        show_top_artists(filtered_df)
    with tab2:
        st.markdown('<h2 class="spotify-green-title">Top 5 Tracks</h2>', unsafe_allow_html=True)
        show_top_tracks(filtered_df)
    with tab3:
        st.markdown('<h2 class="spotify-green-title">Average Daily Charts</h2>', unsafe_allow_html=True)
        plot_weekly_avg_charts(filtered_df)
    with tab4:
        st.markdown('<h2 class="spotify-green-title">Top 5 Genres</h2>', unsafe_allow_html=True)
        plot_top_genres(filtered_df)
    with tab5:
        st.markdown('<h2 class="spotify-green-title">Statistics</h2>', unsafe_allow_html=True)
        show_statistics(filtered_df)

if __name__ == '__main__':
    main() 