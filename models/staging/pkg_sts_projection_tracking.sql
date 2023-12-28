WITH pkg_sts_projection_raw AS (
  SELECT *
  FROM `looker-team-management-386803.jira_clv_staging.Package_Status__Projection_`
)

-- Unpivot status values under each sprint
, unpivot_pkg_sts_projection_raw AS (
  SELECT *
  FROM pkg_sts_projection_raw 
  UNPIVOT (
    Planning_Status FOR Sprint IN (

      {%- set pi_range = range(8,15) -%}
      {%- set iter_range = range(1,6) -%}

      {% for pi in pi_range %}
        {% set outerloop = loop %}
          {%- for iter in iter_range %}
            PI{{ pi }}_Iteration_{{ iter }}
              {%- if not (outerloop.last and loop.last) -%}
                ,
              {%- endif -%}
          {% endfor -%}
      {% endfor %}
      
    )
  )
)

-- Select necessary records and transform
, unpivot_pkg_sts_projection_transform AS (
  SELECT
    CAST(Index AS integer) AS Index
    , Spoke
    , Package
    , CONCAT('DDE ', REPLACE(Sprint, '_', ' ')) AS Sprint
    , Planning_Status
  FROM unpivot_pkg_sts_projection_raw
  WHERE Planning_Status IS NOT NULL
    AND Planning_Status != '-'
)

-- Create pkg_sts_projection_tracking table
SELECT
  Index AS index
  , Spoke AS spoke_name
  , Package AS package_name
  , Sprint AS sprint
  , Planning_Status AS planning_status
FROM unpivot_pkg_sts_projection_transform