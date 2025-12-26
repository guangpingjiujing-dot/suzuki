WITH base_orders AS (
  SELECT
    order_id,
    user_id,
    status,
    created_at AS order_created_at,
    shipped_at,
    delivered_at,
    returned_at,
    num_of_item
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
),

order_items_with_products AS (
  SELECT
    oi.order_id,
    oi.product_id,
    oi.sale_price,
    p.cost,
    p.category,
    p.department,
    p.brand,
    p.distribution_center_id,
    (oi.sale_price - p.cost) AS item_profit
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.products` p
    ON oi.product_id = p.id
),

orders_enriched AS (
  SELECT
    bo.order_id,
    bo.status,
    bo.order_created_at,
    bo.shipped_at,
    bo.delivered_at,
    bo.returned_at,
    bo.num_of_item,
    oip.distribution_center_id,
    SUM(oip.sale_price) AS order_revenue,
    SUM(oip.item_profit) AS order_profit,
    COUNT(DISTINCT oip.product_id) AS product_count
  FROM base_orders bo
  JOIN order_items_with_products oip
    ON bo.order_id = oip.order_id
  GROUP BY
    bo.order_id,
    bo.status,
    bo.order_created_at,
    bo.shipped_at,
    bo.delivered_at,
    bo.returned_at,
    bo.num_of_item,
    oip.distribution_center_id
),

delivery_metrics AS (
  SELECT
    *,
    TIMESTAMP_DIFF(shipped_at, order_created_at, HOUR) AS hours_to_ship,
    TIMESTAMP_DIFF(delivered_at, shipped_at, HOUR) AS hours_to_deliver
  FROM orders_enriched
  WHERE shipped_at IS NOT NULL
),

dc_level_metrics AS (
  SELECT
    distribution_center_id,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(order_revenue) AS total_revenue,
    SUM(order_profit) AS total_profit,
    AVG(order_revenue) AS avg_order_revenue,
    AVG(product_count) AS avg_products_per_order,
    AVG(hours_to_ship) AS avg_hours_to_ship,
    AVG(hours_to_deliver) AS avg_hours_to_deliver,
    COUNTIF(returned_at IS NOT NULL) AS returned_orders,
    SAFE_DIVIDE(
      COUNTIF(returned_at IS NOT NULL),
      COUNT(DISTINCT order_id)
    ) AS return_rate
  FROM delivery_metrics
  GROUP BY distribution_center_id
),

dc_with_geo AS (
  SELECT
    dc.id AS distribution_center_id,
    dc.name AS distribution_center_name,
    dc.latitude,
    dc.longitude,
    dcm.*
  FROM `bigquery-public-data.thelook_ecommerce.distribution_centers` dc
  JOIN dc_level_metrics dcm
    ON dc.id = dcm.distribution_center_id
),

ranked_dc AS (
  SELECT
    * EXCEPT(latitude, longitude),
    RANK() OVER (
      ORDER BY total_revenue DESC
    ) AS revenue_rank,
    RANK() OVER (
      ORDER BY avg_hours_to_deliver ASC
    ) AS delivery_speed_rank,
    NTILE(4) OVER (
      ORDER BY total_profit DESC
    ) AS profit_quartile
  FROM dc_with_geo
)

SELECT
  *
FROM ranked_dc
WHERE
  revenue_rank <= 10
  AND total_orders >= 500
ORDER BY
  total_profit DESC;
