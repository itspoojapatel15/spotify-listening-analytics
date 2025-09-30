"""Streamlit dashboard for Spotify listening analytics."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import snowflake.connector
import os

st.set_page_config(page_title="Spotify Analytics", page_icon="🎵", layout="wide")


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "SPOTIFY_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "SPOTIFY_DB"),
        schema="MARTS",
    )


@st.cache_data(ttl=3600)
def query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn)


st.title("🎵 Spotify Listening Analytics")

# === DAILY LISTENING ===
st.header("Listening Timeline")
daily = query("SELECT * FROM fct_daily_listening ORDER BY play_date DESC LIMIT 90")
if not daily.empty:
    fig = px.bar(daily, x="PLAY_DATE", y="TOTAL_MINUTES", color="DOMINANT_MOOD",
                 title="Daily Listening (Minutes)")
    st.plotly_chart(fig, use_container_width=True)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Daily Minutes", f"{daily['TOTAL_MINUTES'].mean():.0f}")
    col2.metric("Avg Tracks/Day", f"{daily['TRACKS_PLAYED'].mean():.0f}")
    col3.metric("Avg Artists/Day", f"{daily['UNIQUE_ARTISTS'].mean():.0f}")
    col4.metric("Total Days Tracked", len(daily))

# === TOP ARTISTS ===
st.header("Top Artists")
artists = query("SELECT * FROM dim_artists ORDER BY total_plays DESC LIMIT 20")
if not artists.empty:
    fig = px.bar(artists, x="ARTIST_NAME", y="TOTAL_PLAYS", color="AVG_ENERGY",
                 title="Top 20 Artists by Play Count")
    st.plotly_chart(fig, use_container_width=True)

# === TOP TRACKS ===
st.header("Top Tracks")
tracks = query("SELECT * FROM dim_tracks ORDER BY play_count DESC LIMIT 20")
if not tracks.empty:
    st.dataframe(
        tracks[["TRACK_NAME", "ARTIST_NAME", "PLAY_COUNT", "MOOD", "ENERGY", "VALENCE"]].head(20),
        use_container_width=True,
    )

# === MOOD HEATMAP ===
st.header("Mood Heatmap (Energy vs Valence)")
track_moods = query("""
    SELECT track_name, artist_name, energy, valence, danceability, play_count, mood
    FROM dim_tracks WHERE energy IS NOT NULL LIMIT 500
""")
if not track_moods.empty:
    fig = px.scatter(
        track_moods, x="VALENCE", y="ENERGY", color="MOOD",
        size="PLAY_COUNT", hover_data=["TRACK_NAME", "ARTIST_NAME"],
        title="Track Mood Map",
        labels={"VALENCE": "Valence (Happy →)", "ENERGY": "Energy (Intense →)"},
    )
    fig.add_hline(y=0.5, line_dash="dash", opacity=0.3)
    fig.add_vline(x=0.5, line_dash="dash", opacity=0.3)
    st.plotly_chart(fig, use_container_width=True)

# === AUDIO FEATURE RADAR ===
st.header("Audio Profile by Top Artists")
if not artists.empty:
    selected = st.selectbox("Select Artist", artists["ARTIST_NAME"].tolist())
    artist_row = artists[artists["ARTIST_NAME"] == selected].iloc[0]

    categories = ["Danceability", "Energy", "Valence", "Tempo (norm)"]
    values = [
        artist_row["AVG_DANCEABILITY"],
        artist_row["AVG_ENERGY"],
        artist_row["AVG_VALENCE"],
        min(artist_row["AVG_TEMPO"] / 200, 1.0),
    ]

    fig = go.Figure(data=go.Scatterpolar(r=values, theta=categories, fill="toself", name=selected))
    fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])), title=f"Audio Profile: {selected}")
    st.plotly_chart(fig, use_container_width=True)

# === SESSIONS ===
st.header("Recent Listening Sessions")
sessions = query("SELECT * FROM fct_listening_sessions ORDER BY session_start DESC LIMIT 20")
if not sessions.empty:
    st.dataframe(
        sessions[["SESSION_START", "TRACKS_PLAYED", "TOTAL_MINUTES", "DOMINANT_MOOD", "ARTISTS"]].head(20),
        use_container_width=True,
    )
