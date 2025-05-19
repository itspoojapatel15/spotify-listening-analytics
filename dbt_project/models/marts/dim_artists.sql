{{ config(materialized='table') }}

with listens as (
    select * from {{ ref('int_listening_enriched') }}
),
artist_stats as (
    select
        artist_id,
        artist_name,
        count(*) as total_plays,
        count(distinct track_id) as unique_tracks,
        count(distinct play_date) as days_listened,
        sum(duration_ms) / 60000.0 as total_minutes,
        avg(danceability) as avg_danceability,
        avg(energy) as avg_energy,
        avg(valence) as avg_valence,
        avg(tempo) as avg_tempo,
        avg(popularity) as avg_track_popularity,
        min(played_at) as first_listened_at,
        max(played_at) as last_listened_at
    from listens
    where artist_id is not null
    group by 1, 2
)
select * from artist_stats
