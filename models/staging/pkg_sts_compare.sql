WITH iteration_date AS (
  SELECT 
    DISTINCT Sprint_name AS sprint_name
    , CAST(Sprint_startDate AS DATE) AS iteration_start_date
    , CAST(Sprint_endDate AS DATE) AS iteration_end_date
  FROM `looker-team-management-386803.jira_clv_staging.iteration_date_daniel_test`
)

, n1st_join_table AS (
  SELECT 
    a.spoke_name
    , a.package_name
    , a.sprint
    , a.pic_name
    , i.iteration_start_date
    , i.iteration_end_date
    , a.actual_status
    , p.planning_status
  FROM `looker-team-management-386803.jira_clv_test.pkg_sts_actual_tracking` a
  LEFT JOIN `looker-team-management-386803.jira_clv_test.pkg_sts_projection_tracking` p
    ON a.index = p.index
    AND a.sprint = p.sprint

  -- Only include already-started sprints
  INNER JOIN (
    SELECT *
    FROM iteration_date
    WHERE iteration_start_date <= CURRENT_DATE()
    ) i
    ON a.sprint = i.sprint_name
)

, join_table_with_window AS (
  SELECT 
    *
    , LAG(actual_status) OVER(PARTITION BY package_name ORDER BY iteration_start_date) AS previous_status
  FROM n1st_join_table
)

-- Creating dummy components --
, dummy AS (
  SELECT 
    *
    , MAX(iteration_start_date) OVER() AS latest_iteration
  FROM n1st_join_table
)

, dummy_current_status AS (
  SELECT 
    package_name
    , actual_status AS current_status
  FROM dummy
  WHERE iteration_start_date = latest_iteration
)
--------------------------------

, n2nd_join_table AS (
  SELECT
    j.spoke_name
    , j.package_name
    , j.sprint
    , j.pic_name
    , j.iteration_start_date
    , j.iteration_end_date
    , j.actual_status
    , j.planning_status
    , j.previous_status
    , d.current_status
    , CASE 
        WHEN current_status != previous_status THEN TRUE 
        ELSE FALSE END AS is_status_updated
    , CASE 
        WHEN actual_status = planning_status THEN TRUE 
        ELSE FALSE END AS reach_goal_flg
    , CASE 
        WHEN DATE_DIFF(iteration_end_date, CURRENT_DATE, DAY) > -5 AND DATE_DIFF(iteration_end_date, CURRENT_DATE, DAY) <= 8 THEN 'Current Iteration'
        WHEN DATE_DIFF(iteration_end_date, CURRENT_DATE, DAY) > -20 AND DATE_DIFF(iteration_end_date, CURRENT_DATE, DAY) <= -5 THEN 'Previous Iteration'
        END AS sprint_status
    , CASE 
        WHEN Actual_Status ='To Do' THEN 1
        WHEN Actual_Status ='In Progress' THEN 2
        WHEN Actual_Status ='Testing' THEN 3
        WHEN Actual_Status ='Bug Raised' THEN 4  
        WHEN Actual_Status ='Dev Done' THEN 5
        WHEN Actual_Status ='Staging' THEN 6
        ELSE 7 END AS act_decode
    , CASE 
        WHEN Planning_Status ='To Do' THEN 1
        WHEN Planning_Status ='In Progress' THEN 2
        WHEN Planning_Status ='Testing' THEN 3
        WHEN Planning_Status ='Bug Raised' THEN 4  
        WHEN Planning_Status ='Dev Done' THEN 5
        WHEN Planning_Status ='Staging' THEN 6
        ELSE 7 END AS pln_decode
  FROM join_table_with_window AS j
  LEFT JOIN dummy_current_status AS d
    ON j.package_name = d.package_name
)

, pkg_sts_compare AS (
  SELECT 
    spoke_name
    , package_name
    , sprint
    , pic_name
    , iteration_start_date
    , iteration_end_date
    , actual_status
    , planning_status
    , previous_status
    , current_status
    , is_status_updated
    , reach_goal_flg
    , sprint_status
  , CASE
      WHEN act_decode = pln_decode THEN 'On Schedule'
      WHEN act_decode < pln_decode THEN 'Behind Schedule'
      ELSE 'Ahead of Schedule' END AS progress
  FROM n2nd_join_table
)

SELECT *
FROM pkg_sts_compare