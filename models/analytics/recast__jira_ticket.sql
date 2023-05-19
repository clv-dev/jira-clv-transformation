WITH jira_ticket__source AS (
  SELECT
    *
  FROM `looker-team-management-386803.jira_clv_staging.staging__jira_ticket`
)

, jira_ticket__rename_recast AS (
  SELECT
    CAST(`Key` AS STRING) AS ticket_key
    , CAST(sprint AS STRING) AS sprint
    , CAST(status AS STRING) AS ticket_status
    , CAST(parent AS STRING) AS parent_ticket_key
    , CAST(summary AS STRING) AS ticket_name
    , PARSE_DATETIME('%m/%d/%Y %H:%M:%S', updated) AS update_date
    , CAST(assignee AS STRING) AS assignee
    , CAST(PARSE_DATETIME('%m/%d/%Y %H:%M:%S', End_Date) AS DATE) AS end_date
    , CAST(Issue_Type AS STRING) AS ticket_type
    , PARSE_DATE('%m/%d/%Y', Start_date) AS start_date
    , CAST(Est__Story_Points AS NUMERIC) AS story_points
  FROM jira_ticket__source
)

SELECT
  ticket_key -- natural key
  , sprint
  , ticket_type
  , ticket_status
  , parent_ticket_key
  , ticket_name
  , update_date
  , assignee
  , end_date
  , start_date
  , story_points
FROM jira_ticket__rename_recast