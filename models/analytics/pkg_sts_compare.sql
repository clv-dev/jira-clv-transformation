WITH n1st_join_table AS (
  SELECT 
    a.spoke_name
    , a.package_name
    , a.is_cognos
    , a.sprint
    , a.pic_name
    , i.iteration_start_date
    , i.iteration_end_date
    , a.actual_status
    , p.planning_status
  FROM {{ ref('pkg_sts_actual_tracking') }} AS a
  LEFT JOIN {{ ref('pkg_sts_projection_tracking') }} AS p
    ON a.package_name = p.package_name
    AND a.sprint = p.sprint

  -- Only include already-started sprints
  INNER JOIN (
    SELECT *
    FROM {{ ref('iteration_date') }}
    WHERE iteration_start_date < CURRENT_DATE()
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
    package_name
    , actual_status
    , iteration_start_date
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
    , j.is_cognos
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
        ELSE 'Past Iteration' END AS sprint_status

    {%- set status_list = ['actual_status', 'planning_status'] -%}

    {%- for status in status_list %}
    , CASE 
        WHEN {{ status }} = 'Bug Raised' THEN 0
        WHEN {{ status }} = 'To Do' THEN 1
        WHEN {{ status }} = 'In Progress' THEN 2
        WHEN {{ status }} = 'Testing' THEN 3
        WHEN {{ status }} = 'Dev Done' THEN 4
        WHEN {{ status }} = 'Staging' THEN 5
        WHEN {{ status }} = 'Enhancement' THEN 6
        ELSE 7 END AS {{ status }}_encode
    {%- endfor %}

  FROM join_table_with_window AS j
  LEFT JOIN dummy_current_status AS d
    ON j.package_name = d.package_name
)

, pkg_sts_compare AS (
  SELECT 
    spoke_name
    , package_name
    , is_cognos
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
      WHEN actual_status_encode = planning_status_encode THEN 'On Schedule'
      WHEN actual_status_encode < planning_status_encode THEN 'Behind Schedule'
      ELSE 'Ahead of Schedule' END AS progress
  FROM n2nd_join_table
)

SELECT *
FROM pkg_sts_compare