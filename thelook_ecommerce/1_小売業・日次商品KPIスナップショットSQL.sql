WITH
-- =====================================================
-- 1. 日次売上（商品 × 日）
-- =====================================================
daily_sales AS (
  SELECT
    DATE(o.created_at) AS sales_date,
    oi.product_id,
    COUNT(*) AS sold_qty,
    SUM(oi.sale_price) AS revenue
  FROM `bigquery-public-data.thelook_ecommerce.orders` o
  JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
    ON o.order_id = oi.order_id
  WHERE o.status = 'Complete'
  GROUP BY sales_date, oi.product_id
),

-- =====================================================
-- 2. 累積売上・前日比（Window関数）
-- =====================================================
daily_sales_with_trend AS (
  SELECT
    *,
    SUM(revenue) OVER (
      PARTITION BY product_id
      ORDER BY sales_date
    ) AS cumulative_revenue,

    revenue
      - LAG(revenue) OVER (
          PARTITION BY product_id
          ORDER BY sales_date
        ) AS diff_from_prev_day
  FROM daily_sales
),

-- =====================================================
-- 3. 現在在庫（商品別）
-- =====================================================
current_stock AS (
  SELECT
    product_id,
    COUNT(*) AS stock_qty
  FROM `bigquery-public-data.thelook_ecommerce.inventory_items`
  WHERE sold_at IS NULL
  GROUP BY product_id
),

-- =====================================================
-- 4. 過去30日平均販売数（需要）
-- =====================================================
avg_daily_demand AS (
  SELECT
    product_id,
    AVG(sold_qty) AS avg_daily_sold_qty
  FROM daily_sales
  WHERE sales_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY product_id
),

-- =====================================================
-- 5. 商品マスタ
-- =====================================================
products AS (
  SELECT
    * EXCEPT(sku)
  FROM `bigquery-public-data.thelook_ecommerce.products`
),

-- =====================================================
-- 6. KPI統合
-- =====================================================
kpi AS (
  SELECT
    s.sales_date,
    s.product_id,
    p.name AS product_name,
    p.category,
    p.brand,

    s.sold_qty,
    s.revenue,
    s.cumulative_revenue,
    s.diff_from_prev_day,

    cs.stock_qty,
    ad.avg_daily_sold_qty,

    -- 在庫回転率（簡易）
    SAFE_DIVIDE(s.sold_qty, cs.stock_qty) AS daily_turnover,

    -- 在庫残日数（欠品リスク）
    SAFE_DIVIDE(cs.stock_qty, ad.avg_daily_sold_qty) AS days_of_stock
  FROM daily_sales_with_trend s
  LEFT JOIN current_stock cs
    ON s.product_id = cs.product_id
  LEFT JOIN avg_daily_demand ad
    ON s.product_id = ad.product_id
  LEFT JOIN products p
    ON s.product_id = p.id
),

-- =====================================================
-- 7. 欠品リスクフラグ・ランキング
-- =====================================================
final AS (
  SELECT
    *,
    CASE
      WHEN days_of_stock < 7 THEN 'HIGH'
      WHEN days_of_stock < 14 THEN 'MID'
      ELSE 'LOW'
    END AS stock_risk_level,

    RANK() OVER (
      PARTITION BY sales_date
      ORDER BY revenue DESC
    ) AS revenue_rank_in_day
  FROM kpi
)

-- =====================================================
-- 8. 出力（分析向け）
-- =====================================================
SELECT
  *
FROM final
WHERE
  revenue_rank_in_day <= 20
  AND stock_risk_level IN ('HIGH', 'MID')
ORDER BY
  sales_date DESC,
  revenue DESC;
