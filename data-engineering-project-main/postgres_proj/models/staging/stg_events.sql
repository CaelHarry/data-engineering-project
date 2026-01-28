{{ config(materialized='table') }}
with source as (

    select
        event_id,
        user_id,
        event_type,
        event_timestamp,
        session_id,
        event_properties,
        is_late,
        is_duplicate,
        source_file,
        ingestion_timestamp
    from {{ source('raw', 'events') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            order by ingestion_timestamp desc
        ) as row_num
    from source

)

select
    -- business keys
    event_id,
    user_id,

    -- event info
    event_type,
    event_timestamp,
    session_id,

    -- semi-structured data
    event_properties,

    -- metadata (kept for debugging)
    source_file,
    ingestion_timestamp

from deduplicated
where row_num = 1
