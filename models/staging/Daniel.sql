WITH package_status_raw AS (
  SELECT *
  FROM `looker-team-management-386803.jira_clv_staging.Package_Status__Actual_`
)

,unpivot_package_status AS (
  SELECT *
  FROM package_status_raw UNPIVOT (
      Status FOR Sprint IN (
        PI9___Iteration_5,
        PI9___Iteration_4,
        PI9___Iteration_3,
        PI9___Iteration_2,
        PI9___Iteration_1,
        PI8___Iteration_5,
        PI8___Iteration_4,
        PI8___Iteration_3,
        PI8___Iteration_2,
        PI8___Iteration_1,
        PI14___Iteration_5,
        PI14___Iteration_4,
        PI14___Iteration_3,
        PI14___Iteration_2,
        PI14___Iteration_1,
        PI13___Iteration_5,
        PI13___Iteration_4,
        PI13___Iteration_3,
        PI13___Iteration_2,
        PI13___Iteration_1,
        PI12___Iteration_5,
        PI12___Iteration_4,
        PI12___Iteration_3,
        PI12___Iteration_2,
        PI12___Iteration_1,
        PI11___Iteration_5,
        PI11___Iteration_4,
        PI11___Iteration_3,
        PI11___Iteration_2,
        PI11___Iteration_1,
        PI10___Iteration_5,
        PI10___Iteration_4,
        PI10___Iteration_3,
        PI10___Iteration_2,
        PI10___Iteration_1
      )
    )
)

,transform_package_satus AS (
  SELECT 
    CAST(Spoke AS STRING) AS Spoke,
    CAST(Package AS STRING) AS Package,
    CAST(PIC AS STRING) AS PIC,
    CAST(Current_status AS STRING) AS Current_status,
    CAST(Previous_Staus AS STRING) AS Previous_Staus,
    CAST(Status AS STRING) AS Status,
    CAST(Sprint AS STRING) AS Sprint
  FROM 
      unpivot_package_status
)

,Clean_package_satus AS (
  SELECT Spoke,
    Package,
    PIC,
    Current_status,
    Previous_Staus,
    Status,
    TRIM(REPLACE(Sprint, '_', ' ')) AS Sprint
  FROM transform_package_satus
  WHERE Status IS NOT NULL
    AND Status != '-'
    AND Spoke IS NOT NULL
)
SELECT Spoke,
  Package,
  PIC,
  Sprint,
  Status,
  Current_status,
  Previous_Staus
FROM Clean_package_satus