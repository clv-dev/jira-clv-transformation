WITH jira_ticket_generate AS (
  SELECT
    *
  FROM `looker-team-management-386803.jira_clv_staging.historical_jira_ticket`
)

, jira_ticket__recast AS (
    SELECT
        ticket_key
        , CAST(sprint AS STRING) AS sprint
        , CAST(ticket_status AS STRING) AS ticket_status
        , CAST(parent_ticket_key AS STRING) AS parent_ticket_key
        , CAST(ticket_name AS STRING) AS ticket_name
        , CAST(update_date AS TIMESTAMP) AS update_date
        , CAST(assignee AS STRING) AS assignee
        , CAST(end_date AS DATE) AS end_date
        , CAST(ticket_type AS STRING) AS ticket_type
        , CAST(start_date AS DATE) AS start_date
        , CAST(story_points AS NUMERIC) AS story_points
    FROM jira_ticket_generate
)

, jira_ticket_add_update_iteration AS (
  SELECT
    ticket_key
    , sprint
    , ticket_status
    , parent_ticket_key
    , ticket_name
    , update_date
    , assignee
    , end_date
    , ticket_type
    , start_date
    , story_points
    , CASE
        ----PI8----
        WHEN update_date BETWEEN '2023-02-15' AND '2023-02-28' THEN 'PI8 Iteration 1'
        WHEN update_date BETWEEN '2023-03-01' AND '2023-03-14' THEN 'PI8 Iteration 2'
        WHEN update_date BETWEEN '2023-03-15' AND '2023-03-28' THEN 'PI8 Iteration 3'
        WHEN update_date BETWEEN '2023-03-29' AND '2023-04-11' THEN 'PI8 Iteration 4'
        WHEN update_date BETWEEN '2023-04-12' AND '2023-05-07' THEN 'PI8 Iteration 5'
        ----PI9-----
        WHEN update_date BETWEEN '2023-05-08' AND '2023-05-14' THEN 'PI9 Planning Week'
        WHEN update_date BETWEEN '2023-05-17' AND '2023-05-31 10:45:00' THEN 'PI9 Iteration 1'
        WHEN update_date BETWEEN '2023-05-31 10:45:01' AND '2023-06-14 10:30:00' THEN 'PI9 Iteration 2'
        WHEN update_date BETWEEN '2023-06-14 10:30:01' AND '2023-06-28 10:30:00' THEN 'PI9 Iteration 3'
        WHEN update_date BETWEEN '2023-06-28 10:30:01' AND '2023-07-12 10:30:00' THEN 'PI9 Iteration 4'
        WHEN update_date BETWEEN '2023-07-12 10:30:01' AND '2023-07-26 10:30:00' THEN 'PI9 Iteration 5'
        ----PI10-----
        WHEN update_date BETWEEN '2023-08-09 10:30:01' AND '2023-08-23 10:30:00' THEN 'PI10 Iteration 1' 
        WHEN update_date BETWEEN '2023-08-23 10:30:01' AND '2023-09-06 10:30:00' THEN 'PI10 Iteration 2' 
        WHEN update_date BETWEEN '2023-09-06 10:30:01' AND '2023-09-20 10:30:00' THEN 'PI10 Iteration 3'
        WHEN update_date BETWEEN '2023-09-20 10:30:01' AND '2023-10-04 10:30:00' THEN 'PI10 Iteration 4'
        WHEN update_date BETWEEN '2023-10-04 10:30:01' AND '2023-10-25 10:30:00' THEN 'PI10 Iteration 5'
        ----PI11-----
        WHEN update_date BETWEEN '2023-11-08 10:30:01' AND '2023-11-22 10:30:00' THEN 'PI11 Iteration 1' 
        WHEN update_date BETWEEN '2023-11-22 10:30:01' AND '2023-12-06 10:30:00' THEN 'PI11 Iteration 2'
        WHEN update_date BETWEEN '2023-12-06 10:30:01' AND '2023-12-20 10:30:00' THEN 'PI11 Iteration 3'
        WHEN update_date BETWEEN '2023-12-20 10:30:01' AND '2024-01-03 10:30:00' THEN 'PI11 Iteration 4'
        WHEN update_date BETWEEN '2024-01-03 10:30:01' AND '2024-01-24 10:30:00' THEN 'PI11 Iteration 5'

        ----PI12----
        WHEN update_date BETWEEN '2024-02-07 10:30:01' AND '2024-02-21 10:30:00' THEN 'PI12 Iteration 1'
        WHEN update_date BETWEEN '2024-02-21 10:30:01' AND '2024-03-06 10:30:00' THEN 'PI12 Iteration 2'
        WHEN update_date BETWEEN '2024-03-06 10:30:01' AND '2024-03-20 10:30:00' THEN 'PI12 Iteration 3'
        WHEN update_date BETWEEN '2024-03-20 10:30:01' AND '2024-04-03 10:30:00' THEN 'PI12 Iteration 4'
        WHEN update_date BETWEEN '2024-04-03 10:30:01' AND '2024-05-14 10:30:00' THEN 'PI12 Iteration 5'
        ELSE 'Undefined' END
    AS update_iteration
  FROM jira_ticket__recast
)

, jira_ticket_define_flags AS (
    SELECT
        ticket_key
        , sprint
        , ticket_status
        , jira_ticket.parent_ticket_key
        , ticket_name
        , update_date
        , assignee
        , end_date
        , ticket_type
        , start_date
        , story_points
        , update_iteration
        , CASE
            WHEN jira_parent.parent_ticket_key IS NOT NULL THEN TRUE
            ELSE FALSE END
        AS is_parent
        , CASE
            WHEN update_date = MAX(update_date) OVER(PARTITION BY update_iteration, ticket_key) THEN TRUE
            ELSE FALSE END
        AS is_current_row
        , CASE
            WHEN LENGTH(sprint) > 20 THEN TRUE
            ELSE FALSE END
        AS is_delayed_task
    FROM jira_ticket_add_update_iteration AS jira_ticket
    LEFT JOIN (
      SELECT DISTINCT
        parent_ticket_key
      FROM jira_ticket__recast) AS jira_parent
    ON jira_ticket.ticket_key = jira_parent.parent_ticket_key
)

, final_jira_analytics AS (
SELECT 
  ticket_key
  , sprint
  , ticket_status AS actual_ticket_status
  , CASE 
        WHEN ticket_status NOT IN ('Backlog', 'Done', 'TO DO', 'Cancelled') THEN 'In Progress'
        ELSE ticket_status
    END AS ticket_status  , parent_ticket_key
  , is_parent
  , is_delayed_task
  , ticket_name
  , update_date
  , assignee
  , end_date
  , ticket_type
  , start_date
  , story_points
  , update_iteration
  , is_current_row
FROM jira_ticket_define_flags, UNNEST(SPLIT(sprint, ';')) AS sprint

UNION ALL
SELECT
  ticket_key
  , sprint
  , ticket_status AS actual_ticket_status
  , CASE 
        WHEN ticket_status NOT IN ('Backlog', 'Done', 'TO DO', 'Cancelled') THEN 'In Progress'
        ELSE ticket_status
    END AS ticket_status
  , parent_ticket_key
  , is_parent
  , is_delayed_task
  , ticket_name
  , update_date
  , assignee
  , end_date
  , ticket_type
  , start_date
  , story_points
  , update_iteration
  , is_current_row
FROM jira_ticket_define_flags
WHERE sprint IS NULL

)

SELECT
    ticket_key
  , sprint
  , actual_ticket_status
  , ticket_status
  , parent_ticket_key
  , is_parent
  , is_delayed_task
  , ticket_name
  , update_date
  , assignee
  , end_date
  , ticket_type
  , start_date
  , story_points
  , update_iteration
  , is_current_row

FROM final_jira_analytics

ORDER BY update_date DESC
--Filter set for Looker Studio reporting: update_iteration within reporting iteration & is_current_row = True