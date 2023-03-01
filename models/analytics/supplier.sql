SELECT
  supplier_id
  , UPPER(supplier_name) AS supplier_name
  , supplier_category_id
FROM `atomic-oven-374400.learn_dbt.purchasing_suppliers` 