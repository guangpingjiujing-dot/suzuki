WITH
-- =====================================================
-- 1. 注文 × ユーザー × 日付（Completeのみ）
-- =====================================================
orders_base AS (
  SELECT
    o.order_id,
    o.user_id,
    DATE(o.created_at) AS order_date
  FROM `bigquery-public-data.thelook_ecommerce.orders` o
  WHERE o.status = 'Complete'
),

-- =====================================================
-- 2. 注文単位の売上
-- =====================================================
order_revenue AS (
  SELECT
    oi.order_id,
    SUM(oi.sale_price) AS order_revenue,
    COUNT(*) AS item_qty
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  GROUP BY oi.order_id
),

-- =====================================================
-- 3. ユーザー別 注文履歴（売上JOIN）
-- =====================================================
user_orders AS (
  SELECT
    ob.user_id,
    ob.order_id,
    ob.order_date,
    r.order_revenue,
    r.item_qty
  FROM orders_base ob
  JOIN order_revenue r
    ON ob.order_id = r.order_id
),

-- =====================================================
-- 4. ユーザー別 注文順序・前回購入日
-- =====================================================
user_orders_with_lag AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY order_date
    ) AS order_seq,

    LAG(order_date) OVER (
      PARTITION BY user_id
      ORDER BY order_date
    ) AS prev_order_date
  FROM user_orders
),

-- =====================================================
-- 5. 購入間隔（日数）
-- =====================================================
user_orders_interval AS (
  SELECT
    *,
    DATE_DIFF(order_date, prev_order_date, DAY) AS days_since_prev_order
  FROM user_orders_with_lag
),

-- =====================================================
-- 6. ユーザー別 集計KPI
-- =====================================================
user_kpi AS (
  SELECT
    user_id,

    COUNT(*) AS total_orders,
    SUM(order_revenue) AS ltv,
    AVG(order_revenue) AS avg_order_value,
    AVG(item_qty) AS avg_items_per_order,

    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    AVG(days_since_prev_order) AS avg_days_between_orders
  FROM user_orders_interval
  GROUP BY user_id
),

-- =====================================================
-- 7. 全体平均との差分（Window）
-- =====================================================
user_kpi_with_benchmark AS (
  SELECT
    *,
    AVG(ltv) OVER () AS avg_ltv_all_users,
    AVG(total_orders) OVER () AS avg_orders_all_users
  FROM user_kpi
),

-- =====================================================
-- 8. 優良顧客フラグ
-- =====================================================
final AS (
  SELECT
    u.* EXCEPT (
      email,
      street_address,
      postal_code,
      latitude,
      longitude,
      user_geom,
      created_at
    ),

    k.total_orders,
    k.ltv,
    k.avg_order_value,
    k.avg_items_per_order,
    k.avg_days_between_orders,

    CASE
      WHEN k.ltv >= avg_ltv_all_users
       AND k.total_orders >= avg_orders_all_users
      THEN 'VIP'
      WHEN k.total_orders >= 2
      THEN 'REPEAT'
      ELSE 'ONE_TIME'
    END AS customer_segment,

    ROW_NUMBER() OVER (
      PARTITION BY
        CASE
          WHEN k.ltv >= avg_ltv_all_users
           AND k.total_orders >= avg_orders_all_users
          THEN 'VIP'
          WHEN k.total_orders >= 2
          THEN 'REPEAT'
          ELSE 'ONE_TIME'
        END
      ORDER BY k.ltv DESC
    ) AS rank_in_segment
  FROM user_kpi_with_benchmark k
  JOIN `bigquery-public-data.thelook_ecommerce.users` u
    ON k.user_id = u.id
  QUALIFY rank_in_segment <= 100
)

-- =====================================================
-- 9. 出力（セグメント上位）
-- =====================================================
SELECT
  *
FROM final
ORDER BY
  customer_segment desc,
  ltv DESC;
