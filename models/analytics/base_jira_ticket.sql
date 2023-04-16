WITH jira_ticket__source AS (
  SELECT
    *
  FROM jira_intermediate.ticket
)

, jira_ticket__rename_recast AS (
  SELECT
    "Key" :: VARCHAR(10) AS ticket_key
    , sprint :: VARCHAR(512) AS sprint
    , status :: VARCHAR(25) AS ticket_status
    , parent :: VARCHAR(10) AS parent_ticket_key
    , summary :: VARCHAR(512) AS ticket_name
    , TO_TIMESTAMP(updated, 'MM/DD/YYYY HH24:MI:SS') AS update_date
    , assignee :: VARCHAR(100) AS assignee
    , TO_DATE("End Date", 'MM/DD/YYYY') AS end_date
    , "Issue Type" :: VARCHAR(10) AS ticket_type
    , TO_DATE("Start date", 'MM/DD/YYYY') AS start_date
    , "Est. Story Points" :: NUMERIC AS story_points
  FROM jira_ticket__source
)

SELECT
  ticket_key -- natural key
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
FROM jira_ticket__rename_recast