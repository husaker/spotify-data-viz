import streamlit as st
import pandas as pd
from src.data.load_data import load_spotify_data_from_sheets
from src.data.spotify_utils import add_track_lengths_to_df, add_images_to_df, add_artist_info_to_df
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import datetime
import matplotlib.patheffects as pe
from src.data.cache_utils import save_enriched_df, load_enriched_df

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

# Cache loading and enrichment of data
@st.cache_data(show_spinner=True)
def load_raw_data():
    load_dotenv()
    SHEET_URL = os.getenv('GOOGLE_SHEET_URL')
    if not SHEET_URL:
        st.error('GOOGLE_SHEET_URL is not set in .env')
        st.stop()
    df = load_spotify_data_from_sheets(SHEET_URL)
    return df

@st.cache_data(show_spinner=True)
def enrich_track_lengths(df):
    return add_track_lengths_to_df(df, max_workers=2)

@st.cache_data(show_spinner=True)
def enrich_artist_info(df):
    return add_artist_info_to_df(df, max_workers=2)

def load_and_enrich_data():
    cache_path = "data/cache/enriched_data.pkl"
    raw_df = load_raw_data()
    enriched_df = load_enriched_df(cache_path)
    if enriched_df is not None:
        # Find new tracks that are not in the cache
        new_tracks = raw_df[~raw_df['Spotify ID'].isin(enriched_df['Spotify ID'])]
        if not new_tracks.empty:
            new_enriched = enrich_track_lengths(new_tracks)
            new_enriched = enrich_artist_info(new_enriched)
            # Combine old cache and new enriched data
            enriched_df = pd.concat([enriched_df, new_enriched], ignore_index=True)
            save_enriched_df(enriched_df, cache_path)
        # Return the enriched DataFrame (no deduplication)
        return enriched_df
    else:
        # No cache — enrich all data
        enriched_df = enrich_track_lengths(raw_df)
        enriched_df = enrich_artist_info(enriched_df)
        save_enriched_df(enriched_df, cache_path)
        return enriched_df

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

def plot_cumulative_charts(df):
    # --- Tracks ---
    sns.set_theme(style="dark", rc={"axes.facecolor": "#191414", "figure.facecolor": "#191414", "axes.labelcolor": "#fff", "xtick.color": "#fff", "ytick.color": "#fff", "text.color": "#fff"})
    daily = df.groupby('Date').size().rename('tracks').reset_index()
    daily = daily.sort_values('Date')
    daily['cumulative'] = daily['tracks'].cumsum()
    fig, ax = plt.subplots(figsize=(8, 4), dpi=200)
    line, = ax.plot(daily['Date'], daily['cumulative'], color='#1DB954', linewidth=3)
    # Glow-effect
    line.set_path_effects([pe.Stroke(linewidth=8, foreground='#1DB954', alpha=0.18), pe.Normal()])
    ax.set_facecolor('#191414')
    fig.patch.set_facecolor('#191414')
    ax.grid(axis='y', color='#1DB954', alpha=0.08)
    ax.grid(axis='x', visible=False)
    ax.set_title('Cumulative Tracks Played', color='#1DB954', fontsize=18, fontweight='bold')
    ax.set_ylabel('Tracks', color='w', fontsize=12)
    ax.set_xlabel('')
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', color='w')
    plt.setp(ax.get_yticklabels(), color='w')
    # Marker on the last point
    ax.plot(daily['Date'].iloc[-1], daily['cumulative'].iloc[-1], 'o', color='white', markersize=8, markeredgewidth=2, markeredgecolor='#1DB954')
    st.pyplot(fig)

    # --- Minutes ---
    daily = df.groupby('Date')['duration_min'].sum().reset_index()
    daily = daily.sort_values('Date')
    daily['cumulative'] = daily['duration_min'].cumsum()
    fig, ax = plt.subplots(figsize=(8, 4), dpi=200)
    line, = ax.plot(daily['Date'], daily['cumulative'], color='#1DB954', linewidth=3)
    line.set_path_effects([pe.Stroke(linewidth=8, foreground='#1DB954', alpha=0.18), pe.Normal()])
    ax.set_facecolor('#191414')
    fig.patch.set_facecolor('#191414')
    ax.grid(axis='y', color='#1DB954', alpha=0.08)
    ax.grid(axis='x', visible=False)
    ax.set_title('Cumulative Minutes Listened', color='#1DB954', fontsize=18, fontweight='bold')
    ax.set_ylabel('Minutes', color='w', fontsize=12)
    ax.set_xlabel('')
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', color='w')
    plt.setp(ax.get_yticklabels(), color='w')
    ax.plot(daily['Date'].iloc[-1], daily['cumulative'].iloc[-1], 'o', color='white', markersize=8, markeredgewidth=2, markeredgecolor='#1DB954')
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
    min_date, max_date = df['Date'].min().date(), df['Date'].max().date()
    # Initialize session_state for dates
    if 'date_from' not in st.session_state:
        st.session_state['date_from'] = min_date
    if 'date_to' not in st.session_state:
        st.session_state['date_to'] = max_date
    # Reset button
    if st.button('Reset date filter'):
        st.session_state['date_from'] = min_date
        st.session_state['date_to'] = max_date
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
    filtered_df = filter_by_date(df, pd.to_datetime(date_from), pd.to_datetime(date_to))
    # Tabs for switching between graphs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        'Top 5 Artists',
        'Top 5 Tracks',
        'Cumulative Charts',
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
        st.markdown('<h2 class="spotify-green-title">Cumulative Charts</h2>', unsafe_allow_html=True)
        plot_cumulative_charts(filtered_df)
    with tab4:
        st.markdown('<h2 class="spotify-green-title">Top 5 Genres</h2>', unsafe_allow_html=True)
        plot_top_genres(filtered_df)
    with tab5:
        st.markdown('<h2 class="spotify-green-title">Statistics</h2>', unsafe_allow_html=True)
        show_statistics(filtered_df)

if __name__ == '__main__':
    main() 