SELECT
  a1.stock_item_id AS product_id,
  a1.stock_item_name AS product_name,
  a1.supplier_id,
  a2.supplier_name,
  COALESCE(brand,"Undefined") AS brand_name
FROM `atomic-oven-374400.learn_dbt.warehouse_stock_items` AS a1
LEFT JOIN {{ ref('supplier') }} AS a2
  ON a1.supplier_id = a2.supplier_id