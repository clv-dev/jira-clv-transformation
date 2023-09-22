WITH Package_Status_raw AS (
  SELECT
        *
  FROM 
      `looker-team-management-386803.jira_clv_staging.Package_Status_clone`
)
,Package_Status_transform AS (
  SELECT
        Spoke,
        Package,
        PIC,
        Current__Status,
        Previous_Status,
        CONCAT(
          PI10___Iteration_1,';',
          PI10___Iteration_2,';',
          PI10___Iteration_3,';',
          PI10___Iteration_4
          ) AS Sprint_raw
  FROM
      Package_Status_raw, UNNEST(SPLIT(Sprint_raw,';')) AS Sprint
)
