with source as (
    select raw_data, loaded_at from {{ source('raw_spotify', 'AUDIO_FEATURES') }}
),
parsed as (
    select
        raw_data:track_id::varchar as track_id,
        raw_data:danceability::float as danceability,
        raw_data:energy::float as energy,
        raw_data:key::integer as musical_key,
        raw_data:loudness::float as loudness,
        raw_data:mode::integer as mode,
        raw_data:speechiness::float as speechiness,
        raw_data:acousticness::float as acousticness,
        raw_data:instrumentalness::float as instrumentalness,
        raw_data:liveness::float as liveness,
        raw_data:valence::float as valence,
        raw_data:tempo::float as tempo,
        raw_data:time_signature::integer as time_signature,
        loaded_at,
        row_number() over (partition by raw_data:track_id::varchar order by loaded_at desc) as _rn
    from source
)
select * from parsed where _rn = 1
