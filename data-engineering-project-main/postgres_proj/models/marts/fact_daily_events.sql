{{ config(
    materialized = 'incremental',
    unique_key = 'event_date'
) }}

with events as (

    select
        event_timestamp::date as event_date,
        event_type,
        ingestion_timestamp
    from {{ ref('fact_events') }}

    {% if is_incremental() %}
        -- Reprocess recent days to handle late-arriving events
        where event_timestamp::date >= (
            select max(event_date) - interval '2 days'
            from {{ this }}
        )
    {% endif %}

),

daily_aggregates as (

    select
        event_date,

        count(*) as total_events,

        count(*) filter (where event_type = 'signup') as signup_events,
        count(*) filter (where event_type = 'login') as login_events,
        count(*) filter (where event_type = 'view_dashboard') as view_dashboard_events,
        count(*) filter (where event_type = 'create_project') as create_project_events,
        count(*) filter (where event_type = 'upgrade_plan') as upgrade_plan_events,
        count(*) filter (where event_type = 'cancel_subscription') as cancel_subscription_events

    from events
    group by event_date
)

select *
from daily_aggregates
