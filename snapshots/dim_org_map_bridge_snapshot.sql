{% snapshot dim_org_map_bridge_snapshot %}

  {{
    config(
      target_schema='jira_clv_test'
      ,unique_key='CHILD_OFFICE || PARENT_OFFICE || DEPTH_FROM_PARENT'
      ,strategy='check'
      ,check_cols=['CHILD_OFFICE', 'PARENT_OFFICE', 'DEPTH_FROM_PARENT']
      ,invalidate_hard_deletes=True
    )
  }}

  SELECT * FROM {{ ref('dim_org_map_bridge') }}

{% endsnapshot %}
