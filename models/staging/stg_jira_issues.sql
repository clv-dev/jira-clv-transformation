WITH jira_issue AS (
  SELECT
    *
  FROM {{ source('looker_management', 'jira_raw_issues') }}
)

SELECT 
    key AS ticket_id
    ,created
    ,changelog
    ,transitions
    ,renderedFields
    ,fields
    ,updated
FROM jira_issue