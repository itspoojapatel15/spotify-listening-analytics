{{ config(materialized='table') }}

with listens as (
    select * from {{ ref('int_listening_enriched') }}
),
track_stats as (
    select
        track_id,
        track_name,
        artist_name,
        album_name,
        duration_ms,
        is_explicit,
        danceability,
        energy,
        valence,
        tempo,
        acousticness,
        instrumentalness,
        mood,
        count(*) as play_count,
        min(played_at) as first_played_at,
        max(played_at) as last_played_at
    from listens
    where track_id is not null
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
select * from track_stats
