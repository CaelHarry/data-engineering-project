with events as (

    select
        user_id,
        event_type,
        event_timestamp
    from {{ ref('fact_events') }}

),

-- get first time a user did each event
first_events as (

    select
        user_id,
        min(case when event_type = 'signup' then event_timestamp end) as signup_ts,
        min(case when event_type = 'login' then event_timestamp end) as login_ts,
        min(case when event_type = 'view_dashboard' then event_timestamp end) as view_dashboard_ts,
        min(case when event_type = 'create_project' then event_timestamp end) as create_project_ts,
        min(case when event_type = 'upgrade_plan' then event_timestamp end) as upgrade_plan_ts
    from events
    group by user_id

),

funnel as (

    select
        user_id,

        signup_ts is not null as did_signup,

        login_ts is not null
            and login_ts >= signup_ts as did_login,

        view_dashboard_ts is not null
            and view_dashboard_ts >= login_ts as did_view_dashboard,

        create_project_ts is not null
            and create_project_ts >= view_dashboard_ts as did_create_project,

        upgrade_plan_ts is not null
            and upgrade_plan_ts >= create_project_ts as did_upgrade_plan

    from first_events

)

select
    user_id,
    did_signup,
    did_login,
    did_view_dashboard,
    did_create_project,
    did_upgrade_plan
from funnel
