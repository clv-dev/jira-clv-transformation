WITH RECURSIVE dim_org_map_bridge AS (
  SELECT
    OFC_CD AS PARENT_OFFICE
    , OFC_CD AS CHILD_OFFICE
    , 0 AS DEPTH_FROM_PARENT
  FROM {{ ref('organization_raw') }}

  UNION ALL
  SELECT
    parent.PRNT_OFC_CD AS PARENT_OFFICE
    , child.CHILD_OFFICE AS CHILD_OFFICE
    , child.DEPTH_FROM_PARENT + 1 AS DEPTH_FROM_PARENT
  FROM dim_org_map_bridge AS child
  JOIN {{ ref('organization_raw') }} AS parent
  ON child.PARENT_OFFICE = parent.OFC_CD
)

SELECT
  *
FROM dim_org_map_bridge
WHERE PARENT_OFFICE IS NOT NULL
ORDER BY CHILD_OFFICE, DEPTH_FROM_PARENT