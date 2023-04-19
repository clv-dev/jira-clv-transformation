WITH jira_ticket_generate AS (
  SELECT
    *
  FROM jira_analytics.base_jira_ticket
)

, jira_ticket_add_boolean_columns AS (
  SELECT
    a1.ticket_key
    , a1. sprint
    --, UNNEST(STRING_TO_ARRAY(sprint, ';')) AS sprint
    , a1. ticket_status
    , a1. parent_ticket_key
    , CASE
        WHEN a2.parent_ticket_key IS NOT NULL THEN TRUE 
        ELSE FALSE END
    AS is_parent
    , a1. ticket_name
    , a1. update_date
    , a1. assignee
    , a1. end_date
    , a1. ticket_type
    , a1. start_date
    , a1. story_points
  FROM jira_ticket_generate AS a1
  LEFT JOIN (
      SELECT DISTINCT parent_ticket_key
      FROM jira_ticket_generate) AS a2
    ON a1.ticket_key = a2.parent_ticket_key
)

SELECT
  ticket_key
  , UNNEST(STRING_TO_ARRAY(sprint, ';')) AS sprint
  , ticket_status
  , parent_ticket_key
  , is_parent
  , ticket_name
  , update_date
  , assignee
  , end_date
  , ticket_type
  , start_date
  , story_points
FROM jira_ticket_add_boolean_columns