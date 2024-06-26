WITH pkg_sts_actual_raw AS (
  SELECT *
  FROM `looker-team-management-386803.jira_clv_staging.Package_Status_Actual`
)

-- Unpivot status values under each sprint
, unpivot_pkg_sts_actual_raw AS (
  SELECT *
  FROM pkg_sts_actual_raw 
  UNPIVOT (
    Actual_Status FOR Sprint IN (

      {%- set pi_range = range(8,21) -%}
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
, unpivot_pkg_sts_actual_transform AS (
  SELECT
    CAST(Index AS integer) AS Index
    , Spoke
    , Package
    , IS_COGNOS__Y_N_
    , PIC
    , CONCAT('DDE ', REPLACE(Sprint, '_', ' ')) AS Sprint
    , Actual_Status
  FROM unpivot_pkg_sts_actual_raw
  WHERE Actual_Status IS NOT NULL
    AND Actual_Status != '-'
)

-- Create pkg_sts_actual_tracking table
SELECT
  Index AS index
  , Spoke AS spoke_name
  , Package AS package_name
  , IS_COGNOS__Y_N_ AS is_cognos
  , PIC AS pic_name
  , Sprint AS sprint
  , Actual_Status AS actual_status
FROM unpivot_pkg_sts_actual_transform