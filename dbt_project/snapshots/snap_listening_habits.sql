{% snapshot snap_listening_habits %}
{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='artist_id',
        strategy='check',
        check_cols=['total_plays', 'unique_tracks', 'total_minutes'],
    )
}}
select * from {{ ref('dim_artists') }}
{% endsnapshot %}
