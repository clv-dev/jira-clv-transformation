SELECT *
FROM {{ ref('pkg_sts_agg') }}
PIVOT (
  ANY_VALUE(package_list) FOR current_status IN ('To Do','In Progress','Testing','Dev Done','Staging','Production','Bug Raised')
)