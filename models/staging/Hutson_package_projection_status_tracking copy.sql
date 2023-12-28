{%- set Iterations = [
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
] -%}

WITH SprintNames AS (
  SELECT sprint_name
  FROM UNNEST({{Iterations}}) AS sprint_name
)
SELECT
  p.Spoke AS spoke_name,
  p.Package AS package_name,
  s.sprint_name AS sprint,
  CASE s.sprint_name
    {% for Iteration in Iterations %}
        WHEN '{{Iteration}}' THEN p.{{Iteration}}
    {%- endfor %}

  END AS planning_status
FROM `looker-team-management-386803.jira_clv_staging.Package_Status__Projection_` p
CROSS JOIN SprintNames s