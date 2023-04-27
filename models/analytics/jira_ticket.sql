WITH jira_ticket_generate AS (
  SELECT
    *
  FROM jira_intermediate.historic_jira_ticket
)

, jira_ticket_add_boolean_columns AS (
  SELECT
    a1.ticket_key
    , a1. sprint
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

, jira_ticket__add_boolean AS (
  SELECT
    jira_ticket.*
    , jira_date.dde_iteration AS update_iteration
    , CASE
        WHEN update_date = MAX(update_date) OVER(PARTITION BY dde_iteration, ticket_key) THEN 'Current Row'
        ELSE 'Not Current Row' END
      AS is_current_row
  FROM jira_ticket_add_boolean_columns AS jira_ticket
  LEFT JOIN {{ ref("jira_date") }} AS jira_date
    ON jira_ticket.update_date :: date = jira_date.date
)

SELECT 
  *
  , UNNEST(STRING_TO_ARRAY(sprint, ';')) AS sprint
FROM jira_ticket__add_boolean