SELECT 
  DISTINCT Sprint_name AS sprint_name
  , CAST(Sprint_startDate AS DATE) AS iteration_start_date
  , CAST(Sprint_endDate AS DATE) AS iteration_end_date
FROM `looker-team-management-386803.jira_clv_staging.iteration_date_daniel_test`
WHERE Sprint_name IS NOT NULL
  AND Sprint_startDate IS NOT NULL
  AND Sprint_endDate IS NOT NULL
  AND Sprint_state != 'future'