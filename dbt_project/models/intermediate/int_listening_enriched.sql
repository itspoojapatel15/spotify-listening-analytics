{{ config(materialized='ephemeral') }}

with listens as (
    select * from {{ ref('stg_listening_history') }}
),
features as (
    select * from {{ ref('stg_audio_features') }}
),
enriched as (
    select
        l.played_at,
        l.track_id,
        l.track_name,
        l.duration_ms,
        l.is_explicit,
        l.popularity,
        l.album_id,
        l.album_name,
        l.artist_id,
        l.artist_name,
        f.danceability,
        f.energy,
        f.valence,
        f.tempo,
        f.acousticness,
        f.instrumentalness,
        f.speechiness,
        f.liveness,
        f.loudness,
        -- Mood classification
        case
            when f.valence >= 0.6 and f.energy >= 0.6 then 'happy'
            when f.valence >= 0.6 and f.energy < 0.6 then 'peaceful'
            when f.valence < 0.4 and f.energy >= 0.6 then 'angry'
            when f.valence < 0.4 and f.energy < 0.4 then 'sad'
            else 'neutral'
        end as mood,
        -- Time dimensions
        date_trunc('day', l.played_at) as play_date,
        hour(l.played_at) as play_hour,
        dayofweek(l.played_at) as play_day_of_week
    from listens l
    left join features f on l.track_id = f.track_id
)
select * from enriched
