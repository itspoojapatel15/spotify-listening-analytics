{{ config(materialized='table') }}

with enriched as (
    select *,
        lag(played_at) over (order by played_at) as prev_played_at,
        case
            when datediff('minute', lag(played_at) over (order by played_at), played_at) > 30
                 or lag(played_at) over (order by played_at) is null
            then 1 else 0
        end as is_new_session
    from {{ ref('int_listening_enriched') }}
),
sessions as (
    select *,
        sum(is_new_session) over (order by played_at rows unbounded preceding) as session_id
    from enriched
),
session_summary as (
    select
        session_id,
        min(played_at) as session_start,
        max(played_at) as session_end,
        count(*) as tracks_played,
        sum(duration_ms) / 60000.0 as total_minutes,
        avg(energy) as avg_energy,
        avg(valence) as avg_valence,
        avg(danceability) as avg_danceability,
        avg(tempo) as avg_tempo,
        mode(mood) as dominant_mood,
        listagg(distinct artist_name, ', ') within group (order by played_at) as artists,
        min(play_date) as session_date,
        min(play_hour) as session_start_hour
    from sessions
    group by session_id
)
select * from session_summary
