WITH actual_package AS (
  SELECT DISTINCT
    package_name
  FROM {{ ref('pkg_sts_actual_tracking') }}
)

, projection_package AS (
  SELECT DISTINCT
    package_name
  FROM {{ ref('pkg_sts_projection_tracking') }}
)

SELECT
  actual.package_name AS actual_package_name
  , projection.package_name AS projection_package_name
FROM actual_package AS actual
FULL OUTER JOIN projection_package AS projection
  USING(package_name)
WHERE actual.package_name IS NULL
  OR projection.package_name IS NULL