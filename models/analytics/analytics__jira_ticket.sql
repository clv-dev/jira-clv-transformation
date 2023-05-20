WITH jira_ticket_generate AS (
  SELECT
    *
  FROM `looker-team-management-386803.jira_clv_staging.historic_jira_ticket`
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

, jira_ticket_add_boolean_columns AS (
  SELECT
    jira_ticket.ticket_key
    , jira_ticket.sprint
    , jira_ticket.ticket_status
    , jira_ticket.parent_ticket_key
    , jira_ticket.ticket_name
    , jira_ticket.update_date
    , jira_ticket.assignee
    , jira_ticket.end_date
    , jira_ticket.ticket_type
    , jira_ticket.start_date
    , jira_ticket.story_points
    , jira_date.dde_iteration AS update_iteration
    , CASE
        WHEN jira_parent.parent_ticket_key IS NOT NULL THEN TRUE
        ELSE FALSE END
      AS is_parent
    , CASE
        WHEN update_date = MAX(update_date) OVER(PARTITION BY dde_iteration, ticket_key) THEN TRUE
        ELSE FALSE END
      AS is_current_row
    , CASE
        WHEN LENGTH(sprint) > 20 THEN TRUE
        ELSE FALSE END
      AS is_delayed_task
  FROM jira_ticket__recast AS jira_ticket
  LEFT JOIN (
      SELECT DISTINCT parent_ticket_key
      FROM jira_ticket__recast) AS jira_parent
    ON jira_ticket.ticket_key = jira_parent.parent_ticket_key
  LEFT JOIN {{ ref('jira_date') }} AS jira_date
    ON CAST(jira_ticket.update_date AS DATE) = jira_date.date
)

SELECT 
  ticket_key
  , sprint
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
FROM jira_ticket_add_boolean_columns, UNNEST(SPLIT(sprint, ';')) AS sprint

UNION ALL
SELECT 
  ticket_key
  , sprint
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
FROM jira_ticket_add_boolean_columns
WHERE sprint IS NULL