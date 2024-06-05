WITH container_fact__select_source AS (
  SELECT *
  FROM {{ ref ('container_quantity') }}
)

SELECT
  IFNULL(CAST(dmc_office_inv_cr_ofc_cd AS STRING), 'UNDEFINED') AS INVOICE_OFFICE 
  , IFNULL(CAST(dml_trsp_cost_total_cntr_qty AS INTEGER), 0) AS CONTAINER_QUANTITY
FROM container_fact__select_source