WITH jira_ticket_generate AS (
  SELECT
    *
  FROM jira_analytics.base_jira_ticket
)

, jira_ticket__current_row_flag AS (
  SELECT
    *
    , CASE
        WHEN update_date = MAX(update_date) OVER(PARTITION BY ticket_key) THEN 'Current Row'
        ELSE 'Not Current Row' END
    AS is_current_row
  FROM jira_ticket_generate
)
SELECT
  *
FROM jira_ticket__current_row_flag