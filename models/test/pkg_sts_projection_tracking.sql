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
      PI8_Iteration_1,
      PI8_Iteration_2,
      PI8_Iteration_3,
      PI8_Iteration_4,
      PI8_Iteration_5,

      PI9_Iteration_1,
      PI9_Iteration_2,
      PI9_Iteration_3,
      PI9_Iteration_4,
      PI9_Iteration_5,

      PI10_Iteration_1,
      PI10_Iteration_2,
      PI10_Iteration_3,
      PI10_Iteration_4,
      PI10_Iteration_5,

      PI11_Iteration_1,
      PI11_Iteration_2,
      PI11_Iteration_3,
      PI11_Iteration_4,
      PI11_Iteration_5,

      PI12_Iteration_1,
      PI12_Iteration_2,
      PI12_Iteration_3,
      PI12_Iteration_4,
      PI12_Iteration_5,

      PI13_Iteration_1,
      PI13_Iteration_2,
      PI13_Iteration_3,
      PI13_Iteration_4,
      PI13_Iteration_5,

      PI14_Iteration_1,
      PI14_Iteration_2,
      PI14_Iteration_3,
      PI14_Iteration_4,
      PI14_Iteration_5
    )
  )
)

-- Select necessary records and transform
, unpivot_pkg_sts_projection_transform AS (
  SELECT
    s AS Index
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