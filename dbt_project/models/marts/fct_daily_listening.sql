{{ config(materialized='table') }}

with enriched as (
    select * from {{ ref('int_listening_enriched') }}
),
daily as (
    select
        play_date,
        count(*) as tracks_played,
        count(distinct track_id) as unique_tracks,
        count(distinct artist_id) as unique_artists,
        sum(duration_ms) / 60000.0 as total_minutes,
        avg(energy) as avg_energy,
        avg(valence) as avg_valence,
        avg(danceability) as avg_danceability,
        avg(tempo) as avg_tempo,
        mode(mood) as dominant_mood,
        max_by(artist_name, count(*)) as top_artist
    from enriched
    group by 1
)
select * from daily
