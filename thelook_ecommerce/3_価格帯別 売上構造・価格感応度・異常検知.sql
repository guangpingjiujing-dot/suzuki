WITH
-- =====================================================
-- 1. 完了注文の注文明細
-- =====================================================
order_items_base AS (
  SELECT
    oi.order_id,
    oi.product_id,
    oi.sale_price,
    DATE(o.created_at) AS order_date
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.orders` o
    ON oi.order_id = o.order_id
  WHERE o.status = 'Complete'
),

-- =====================================================
-- 2. 商品マスタ（分析不要カラム除外）
-- =====================================================
products AS (
  SELECT
    * EXCEPT (sku)
  FROM `bigquery-public-data.thelook_ecommerce.products`
),

-- =====================================================
-- 3. 商品 × 日次売上
-- =====================================================
daily_product_sales AS (
  SELECT
    order_date,
    product_id,
    COUNT(*) AS sold_qty,
    SUM(sale_price) AS revenue,
    AVG(sale_price) AS avg_sale_price
  FROM order_items_base
  GROUP BY order_date, product_id
),

-- =====================================================
-- 4. 商品 × 集計（期間全体）
-- =====================================================
product_summary AS (
  SELECT
    product_id,
    SUM(sold_qty) AS total_sold_qty,
    SUM(revenue) AS total_revenue,
    AVG(avg_sale_price) AS avg_price_overall
  FROM daily_product_sales
  GROUP BY product_id
),

-- =====================================================
-- 5. カテゴリ × 価格統計（ベンチマーク）
-- =====================================================
category_price_stats AS (
  SELECT
    p.category,
    AVG(ps.avg_price_overall) AS avg_category_price,
    STDDEV(ps.avg_price_overall) AS std_category_price
  FROM product_summary ps
  JOIN products p
    ON ps.product_id = p.id
  GROUP BY p.category
),

-- =====================================================
-- 6. 商品 × カテゴリ × 価格偏差
-- =====================================================
product_price_deviation AS (
  SELECT
    p.id AS product_id,
    p.name AS product_name,
    p.category,
    ps.total_sold_qty,
    ps.total_revenue,
    ps.avg_price_overall,

    cps.avg_category_price,
    cps.std_category_price,

    SAFE_DIVIDE(
      ps.avg_price_overall - cps.avg_category_price,
      cps.std_category_price
    ) AS price_z_score
  FROM product_summary ps
  JOIN products p
    ON ps.product_id = p.id
  JOIN category_price_stats cps
    ON p.category = cps.category
),

-- =====================================================
-- 7. カテゴリ内ランキング
-- =====================================================
ranked_products AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY category
      ORDER BY total_revenue DESC
    ) AS revenue_rank_in_category
  FROM product_price_deviation
),

-- =====================================================
-- 8. 異常フラグ・価格感応度分類
-- =====================================================
final AS (
  SELECT
    *,

    CASE
      WHEN price_z_score >= 2 THEN 'VERY_EXPENSIVE'
      WHEN price_z_score >= 1 THEN 'EXPENSIVE'
      WHEN price_z_score <= -2 THEN 'VERY_CHEAP'
      WHEN price_z_score <= -1 THEN 'CHEAP'
      ELSE 'NORMAL'
    END AS price_position,

    CASE
      WHEN price_z_score > 0 AND total_sold_qty > 100 THEN 'HIGH_PRICE_STRONG_SALES'
      WHEN price_z_score < 0 AND total_sold_qty < 20 THEN 'LOW_PRICE_WEAK_SALES'
      ELSE 'NORMAL_BEHAVIOR'
    END AS price_sensitivity_flag
  FROM ranked_products
)

-- =====================================================
-- 9. 出力（注目商品抽出）
-- =====================================================
SELECT
  *
FROM final
WHERE
  revenue_rank_in_category <= 10
  AND price_position IN ('VERY_EXPENSIVE', 'VERY_CHEAP')
ORDER BY
  category,
  revenue_rank_in_category;
