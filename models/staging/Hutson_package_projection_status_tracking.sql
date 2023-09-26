WITH SprintNames AS (
  SELECT sprint_name
  FROM UNNEST([
    'PI8_Iteration_1','PI8_Iteration_2', 'PI8_Iteration_3',
    'PI8_Iteration_4','PI8_Iteration_5', 'PI9_Iteration_1',
    'PI9_Iteration_2','PI9_Iteration_3', 'PI9_Iteration_4', 
    'PI9_Iteration_5','PI10_Iteration_1', 'PI10_Iteration_2',
    'PI10_Iteration_3','PI10_Iteration_4', 'PI10_Iteration_5',
    'PI11_Iteration_1', 'PI11_Iteration_2', 'PI11_Iteration_3',
    'PI11_Iteration_4', 'PI11_Iteration_5', 'PI12_Iteration_1',
    'PI12_Iteration_2', 'PI12_Iteration_3', 'PI12_Iteration_4',
    'PI12_Iteration_5', 'PI13_Iteration_1', 'PI13_Iteration_2',
    'PI13_Iteration_3', 'PI13_Iteration_4', 'PI13_Iteration_5',
    'PI14_Iteration_1', 'PI14_Iteration_2', 'PI14_Iteration_3',
    'PI14_Iteration_4', 'PI14_Iteration_5'
  ]) AS sprint_name
)
SELECT
  p.Spoke AS spoke_name,
  p.Package AS package_name,
  s.sprint_name AS sprint,
  CASE s.sprint_name
    WHEN 'PI8_Iteration_1' THEN p.PI8_Iteration_1
    WHEN 'PI8_Iteration_2' THEN p.PI8_Iteration_2
    WHEN 'PI8_Iteration_3' THEN p.PI8_Iteration_3
    WHEN 'PI8_Iteration_4' THEN p.PI8_Iteration_4
    WHEN 'PI8_Iteration_5' THEN p.PI8_Iteration_5
    WHEN 'PI9_Iteration_1' THEN p.PI9_Iteration_1
    WHEN 'PI9_Iteration_2' THEN p.PI9_Iteration_2
    WHEN 'PI9_Iteration_3' THEN p.PI9_Iteration_3
    WHEN 'PI9_Iteration_4' THEN p.PI9_Iteration_4
    WHEN 'PI9_Iteration_5' THEN p.PI9_Iteration_5
    WHEN 'PI10_Iteration_1' THEN p.PI10_Iteration_1
    WHEN 'PI10_Iteration_2' THEN p.PI10_Iteration_2
    WHEN 'PI10_Iteration_3' THEN p.PI10_Iteration_3
    WHEN 'PI10_Iteration_4' THEN p.PI10_Iteration_4
    WHEN 'PI10_Iteration_5' THEN p.PI10_Iteration_5
    WHEN 'PI11_Iteration_1' THEN p.PI11_Iteration_1
    WHEN 'PI11_Iteration_2' THEN p.PI11_Iteration_2
    WHEN 'PI11_Iteration_3' THEN p.PI11_Iteration_3
    WHEN 'PI11_Iteration_4' THEN p.PI11_Iteration_4
    WHEN 'PI11_Iteration_5' THEN p.PI11_Iteration_5
    WHEN 'PI12_Iteration_1' THEN p.PI12_Iteration_1
    WHEN 'PI12_Iteration_2' THEN p.PI12_Iteration_2
    WHEN 'PI12_Iteration_3' THEN p.PI12_Iteration_3
    WHEN 'PI12_Iteration_4' THEN p.PI12_Iteration_4
    WHEN 'PI12_Iteration_5' THEN p.PI12_Iteration_5
    WHEN 'PI13_Iteration_1' THEN p.PI13_Iteration_1
    WHEN 'PI13_Iteration_2' THEN p.PI13_Iteration_2
    WHEN 'PI13_Iteration_3' THEN p.PI13_Iteration_3
    WHEN 'PI13_Iteration_4' THEN p.PI13_Iteration_4
    WHEN 'PI13_Iteration_5' THEN p.PI13_Iteration_5
    WHEN 'PI14_Iteration_1' THEN p.PI14_Iteration_1
    WHEN 'PI14_Iteration_2' THEN p.PI14_Iteration_2
    WHEN 'PI14_Iteration_3' THEN p.PI14_Iteration_3
    WHEN 'PI14_Iteration_4' THEN p.PI14_Iteration_4
    WHEN 'PI14_Iteration_5' THEN p.PI14_Iteration_5
  END AS planning_status
FROM `looker-team-management-386803.jira_clv_staging.Package_Status__Projection_` p
CROSS JOIN SprintNames s