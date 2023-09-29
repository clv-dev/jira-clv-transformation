                                                  --- PROCESSING PACKAGE STATUS ACTUAL DATA---

--- Unpivot,Clean, and Transform Actual Data
WITH package_status_actual_raw AS (
  SELECT *
  FROM `looker-team-management-386803.jira_clv_staging.Package_Status__Actual_`
)

---unpivot package_status_data
,unpivot_package_status_actual AS (
  SELECT *
  FROM 
      package_status_actual_raw UNPIVOT (
        Actual_Status FOR Sprint IN (
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

--Replace Special character '___' for Sprint, Filter out Status, Spoke NULL and Actual_Status have special character '-' and remove whitesapce all columns
,clean_package_status_actual AS (
  SELECT 
    Index                       AS Primary_key,
    TRIM(Spoke)                 AS Spoke,
    REPLACE(Package,'  ',' ')   AS Package, -- Use REPLACE function to remove whitespace, dont know why TRIM Function do not working in this column
    TRIM(PIC)                   AS PIC,
    TRIM(Actual_Status)         AS Actual_Status,
    TRIM(CONCAT('DDE ',REPLACE(Sprint, '___', ' '))) AS Sprint
  FROM 
      unpivot_package_status_actual
  WHERE 
        Actual_Status IS NOT NULL
        AND Actual_Status != '-'
)

--Recast all Column in Package status data with specific type for each column
,transform_package_status_actual AS (
  SELECT 
    CAST(Primary_key   AS STRING)   AS Primary_key,
    CAST(Spoke         AS STRING)   AS Spoke,
    CAST(Package       AS STRING)   AS Package,
    CAST(PIC           AS STRING)   AS PIC,
    CAST(Actual_Status AS STRING)   AS Actual_Status,
    CAST(
      REPLACE(
      Sprint,'_',' ')  AS STRING)   AS Sprint -- Use REPLACE function to remove special character left '_' after processing Replace '___' in Sprint column EX: 'PI10 Iteration_1' 
  FROM 
      clean_package_status_actual
)

                                                  --- PROCESSING PACKAGE STATUS PROJECTION DATA---

--- Unpivot,Clean, and Transform Projection Data
,package_status_projection_raw AS (
  SELECT *
  FROM `looker-team-management-386803.jira_clv_staging.package_status_projection_daniel_test`
)

-- unpivot Package Status Projection data
,unpivot_package_status_projection AS (
  SELECT *
  FROM 
    package_status_projection_raw UNPIVOT (
      Planning_Status FOR Sprint IN (
          PI8_Iteration_1
          ,PI8_Iteration_2
          ,PI8_Iteration_3
          ,PI8_Iteration_4
          ,PI8_Iteration_5
          ,PI9_Iteration_1
          ,PI9_Iteration_2
          ,PI9_Iteration_3
          ,PI9_Iteration_4
          ,PI9_Iteration_5
          ,PI10_Iteration_1
          ,PI10_Iteration_2
          ,PI10_Iteration_3
          ,PI10_Iteration_4
          ,PI10_Iteration_5
          ,PI11_Iteration_1
          ,PI11_Iteration_2
          ,PI11_Iteration_3
          ,PI11_Iteration_4
          ,PI11_Iteration_5
          ,PI12_Iteration_1
          ,PI12_Iteration_2
          ,PI12_Iteration_3
          ,PI12_Iteration_4
          ,PI12_Iteration_5
          ,PI13_Iteration_1
          ,PI13_Iteration_2
          ,PI13_Iteration_3
          ,PI13_Iteration_4
          ,PI13_Iteration_5
          ,PI14_Iteration_1
          ,PI14_Iteration_2
          ,PI14_Iteration_3
          ,PI14_Iteration_4
          ,PI14_Iteration_5
      )
    )
)

---remove whitespace for all columns and filter out NULL value in Planning Status column
,clean_package_status_projection AS (
  SELECT
        Index                 AS Primary_key,
        TRIM(Spoke)           AS Spoke,
        TRIM(Package)         AS Package,
        TRIM(Planning_Status) AS Planning_Status,
        TRIM(CONCAT('DDE ',REPLACE(Sprint,'_',' ')))  AS Sprint
  FROM
      unpivot_package_status_projection
  WHERE
      Planning_Status IS NOT NULL 
      AND Planning_Status !='-'
)

--Recast all Column in Package status data with specific type for each column
,transform_package_status_projection AS (
  SELECT
        CAST(Primary_key     AS STRING)   AS Primary_key,
        CAST(Spoke           AS STRING)   AS Spoke,
        CAST(Package         AS STRING)   AS Package,
        CAST(Planning_Status AS STRING)   AS Planning_Status,
        CAST(Sprint          AS STRING)   AS Sprint
  FROM 
      clean_package_status_projection
)

                                                  --- PROCESSING ITERATION DATE DATA---

,iteration_date_raw AS (
  SELECT *
  FROM
      `looker-team-management-386803.jira_clv_staging.iteration_date_daniel_test` 
)

--Recast all Column in iteration date data with specific type for each column
,recast_iteration_date AS(
  SELECT DISTINCT
        CAST(Sprint_name AS STRING)       AS Sprint,
        CAST(Sprint_startDate AS DATE)    AS Start_date,
        CAST(Sprint_endDate	 AS DATE)     AS End_date
  FROM 
      iteration_date_raw
)

                                                      ---JOINING TABLE---

--Join Package Status Actual with Package Status Projection by Primary_key, and Sprint Columns
, combine_actual_and_projection_package_status AS (
SELECT     
      PSA.Spoke       AS Spoke,
      PSA.Package     AS Package,
      PSA.Sprint      AS Sprint,
      PIC             AS PIC,
      Actual_Status   AS Actual_Status,
      Planning_Status AS Planning_Status
FROM
    transform_package_status_actual PSA 
    LEFT JOIN transform_package_status_projection PSP
    ON  PSA.Primary_key  = PSP.Primary_key
    AND PSA.Sprint =PSP.Sprint
)

--Join Package Status with Iteration Date by Sprint Column
,combine_ps_id AS (
SELECT 
      PS.Sprint AS Sprint,
      Spoke,
      PIC,
      Package,
      Actual_Status,
      Planning_Status,
      Start_date,
      End_date
FROM 
    combine_actual_and_projection_package_status PS 
    LEFT JOIN recast_iteration_date ID
    ON PS.Sprint =ID.Sprint
)

                                                    ---ADDING LOGIC---

-- Adding Current Sprint Logic base on Current_Date, start_date, and end_date
,current_sprint_logic AS(
SELECT  
      Sprint,
      Spoke,
      PIC,
      Package,
      Actual_Status,
      Planning_Status,
      Start_date,
      End_Date,
      CASE 
          WHEN Date_diff(End_Date,CURRENT_DATE, DAY) > -5  AND  Date_diff(End_Date,CURRENT_DATE, DAY) <= 8 THEN 'Current Iteration'
           -- add more 5days (Wed,Thur,Fri,Sat,Sun) to help Reporter preparing report, and a new sprint status will be changed on the first Monday of the next Sprint
          WHEN Date_diff(End_Date,CURRENT_DATE, DAY) > -20 AND  Date_diff(End_Date,CURRENT_DATE, DAY) <=-5 THEN 'Previous Iteration' 
          ELSE 'Over Iteration' 
      END AS Sprint_Status
FROM
    combine_ps_id
)
                                        ----TRANSFORM TABLE---
--- Concat Package name and Pivot Table
SELECT
*
FROM(
      SELECT DISTINCT
            Spoke,
            Actual_Status AS Status,
            STRING_AGG (Package,',') OVER (PARTITION BY Spoke,Actual_Status) AS Package
            ---Concat Package name base on Spoke and Actual Status
      FROM 
            current_sprint_logic 
      WHERE
            Sprint_Status ='Current Iteration'
    ) PIVOT( MAX(Package)  FOR Status IN ('To Do','In Progress','Testing','Dev Done','Staging','Production','Bug Raised')) ---Pivot from 3 column in CTE into 7 column base on Status, and the value is Package name that concated in CTE above 
    -- ss
