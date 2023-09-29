WITH pkg_sts_actual_raw AS (
  SELECT *
  FROM `looker-team-management-386803.jira_clv_staging.Package_Status__Actual_`
)

-- Unpivot status values under each sprint
, unpivot_pkg_sts_actual_raw AS (
  SELECT *
  FROM pkg_sts_actual_raw 
  UNPIVOT (
    Actual_Status FOR Sprint IN (
      PI8___Iteration_1,
      PI8___Iteration_2,
      PI8___Iteration_3,
      PI8___Iteration_4,
      PI8___Iteration_5,

      PI9___Iteration_1,
      PI9___Iteration_2,
      PI9___Iteration_3,
      PI9___Iteration_4,
      PI9___Iteration_5,

      PI10___Iteration_1,
      PI10___Iteration_2,
      PI10___Iteration_3,
      PI10___Iteration_4,
      PI10___Iteration_5,

      PI11___Iteration_1,
      PI11___Iteration_2,
      PI11___Iteration_3,
      PI11___Iteration_4,
      PI11___Iteration_5,

      PI12___Iteration_1,
      PI12___Iteration_2,
      PI12___Iteration_3,
      PI12___Iteration_4,
      PI12___Iteration_5,

      PI13___Iteration_1,
      PI13___Iteration_2,
      PI13___Iteration_3,
      PI13___Iteration_4,
      PI13___Iteration_5,

      PI14___Iteration_1,
      PI14___Iteration_2,
      PI14___Iteration_3,
      PI14___Iteration_4,
      PI14___Iteration_5
    )
  )
)

-- Select necessary records and transform
, unpivot_pkg_sts_actual_transform AS (
  SELECT
    Index
    , Spoke
    , Package
    , PIC
    , CONCAT('DDE ', REPLACE(REPLACE(Sprint, '_', ' '), '  ', '')) AS Sprint
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
  , PIC AS pic_name
  , Sprint AS sprint
  , Actual_Status AS actual_status
FROM unpivot_pkg_sts_actual_transform