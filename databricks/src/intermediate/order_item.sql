CREATE OR REFRESH LIVE TABLE order_item
COMMENT "Intermediate order item table enriched with gross and net amounts"
AS
SELECT
  oi.order_id,
  oi.product_id,
  oi.quantity,
  (p.price * oi.quantity) AS gross_amount,
  (p.price * oi.quantity) * (1 - (COALESCE(o.discount_percentage, 0) / 100)) AS net_amount
FROM ecomm.staging.order_item oi
LEFT JOIN ecomm.staging.product p 
  ON oi.product_id = p.id
LEFT JOIN ecomm.staging.order o 
  ON oi.order_id = o.order_id
