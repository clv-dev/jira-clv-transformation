WITH jira_ticket_generate AS (
  SELECT
    *
  FROM jira_analytics.base_jira_ticket
)

, jira_ticket__current_row_flag AS (
  SELECT
    ticket_key
    , UNNEST(STRING_TO_ARRAY(sprint, ';')) AS sprint
    , ticket_status
    , parent_ticket_key
    , ticket_name
    , update_date
    , assignee
    , end_date
    , ticket_type
    , start_date
    , story_points
    AS is_current_row
  FROM jira_ticket_generate
)
SELECT
  *
FROM jira_ticket__current_row_flag