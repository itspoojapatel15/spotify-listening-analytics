with source as (
    select raw_data, loaded_at from {{ source('raw_spotify', 'LISTENING_HISTORY') }}
),
parsed as (
    select
        raw_data:played_at::timestamp_tz as played_at,
        raw_data:track_id::varchar as track_id,
        raw_data:track_name::varchar as track_name,
        raw_data:duration_ms::integer as duration_ms,
        raw_data:explicit::boolean as is_explicit,
        raw_data:popularity::integer as popularity,
        raw_data:album_id::varchar as album_id,
        raw_data:album_name::varchar as album_name,
        raw_data:artist_id::varchar as artist_id,
        raw_data:artist_name::varchar as artist_name,
        loaded_at,
        row_number() over (partition by raw_data:played_at::varchar, raw_data:track_id::varchar order by loaded_at desc) as _rn
    from source
)
select * from parsed where _rn = 1
