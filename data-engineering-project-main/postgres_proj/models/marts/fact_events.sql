{{ config(
    materialized = 'incremental',
    unique_key = 'event_id'
) }}

with events as (

    select
        event_id,
        user_id,
        event_type,
        event_timestamp,
        session_id,
        event_properties,
        ingestion_timestamp
    from {{ ref('stg_events') }}

    {% if is_incremental() %}
        -- Only process events ingested since last run
        where ingestion_timestamp > (
            select max(ingestion_timestamp)
            from {{ this }}
        )
    {% endif %}

)

select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    session_id,
    event_properties,
    ingestion_timestamp
from events
