WITH jira_sprint_issues AS (
  SELECT
    *
  FROM {{ source('looker_management', 'jira_raw_sprint_issues') }}
)

SELECT 
    key AS ticket_id
    ,fields
FROM jira_sprint_issues