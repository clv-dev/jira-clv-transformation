WITH spoke_package_status_tuple AS (
  SELECT
    spoke_name
    , package_name
    , current_status
  FROM {{ ref('pkg_sts_compare') }} 
  GROUP BY spoke_name, package_name, current_status
)

, distinct_package_list AS (
  SELECT
    spoke_name
    , current_status
    , STRING_AGG(package_name, ', ') OVER (PARTITION BY spoke_name, current_status) AS package_list
  FROM spoke_package_status_tuple
)

SELECT *
FROM distinct_package_list
GROUP BY spoke_name, current_status, package_list