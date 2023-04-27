WITH jira_ticket_generate AS (
  SELECT
    *
  FROM jira_intermediate.historic_jira_ticket
)

, jira_ticket_add_boolean_columns AS (
  SELECT
    jira_ticket.ticket_key
    , jira_ticket. sprint
    , jira_ticket. ticket_status
    , jira_ticket. parent_ticket_key
    , CASE
        WHEN jira_parent.parent_ticket_key IS NOT NULL THEN TRUE 
        ELSE FALSE END
    AS is_parent
    , jira_ticket. ticket_name
    , jira_ticket. update_date
    , jira_ticket. assignee
    , jira_ticket. end_date
    , jira_ticket. ticket_type
    , jira_ticket. start_date
    , jira_ticket. story_points
    , jira_date.dde_iteration AS update_iteration
    , CASE
        WHEN update_date = MAX(update_date) OVER(PARTITION BY dde_iteration, ticket_key) THEN 'Current Row'
        ELSE 'Not Current Row' END
      AS is_current_row
  FROM jira_ticket_generate AS jira_ticket
  LEFT JOIN (
      SELECT DISTINCT parent_ticket_key
      FROM jira_ticket_generate) AS jira_parent
    ON jira_ticket.ticket_key = jira_parent.parent_ticket_key
  LEFT JOIN jira_analytics.jira_date AS jira_date
    ON jira_ticket.update_date :: date = jira_date.date
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
  , update_iteration
  , is_current_row
FROM jira_ticket_add_boolean_columns