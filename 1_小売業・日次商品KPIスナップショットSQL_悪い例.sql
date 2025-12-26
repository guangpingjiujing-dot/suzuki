-- =====================================================
-- 悪い例：WITH句をサブクエリ化、Window関数をGROUP BYで冗長化
-- =====================================================
SELECT
  ranked.sales_date,
  ranked.product_id,
  ranked.product_name,
  ranked.category,
  ranked.brand,
  ranked.sold_qty,
  ranked.revenue,
  ranked.cumulative_revenue,
  ranked.diff_from_prev_day,
  ranked.stock_qty,
  ranked.avg_daily_sold_qty,
  ranked.daily_turnover,
  ranked.days_of_stock,
  ranked.stock_risk_level,
  ranked.revenue_rank_in_day
FROM (
  SELECT
    kpi_with_risk.sales_date,
    kpi_with_risk.product_id,
    kpi_with_risk.product_name,
    kpi_with_risk.category,
    kpi_with_risk.brand,
    kpi_with_risk.sold_qty,
    kpi_with_risk.revenue,
    kpi_with_risk.cumulative_revenue,
    kpi_with_risk.diff_from_prev_day,
    kpi_with_risk.stock_qty,
    kpi_with_risk.avg_daily_sold_qty,
    kpi_with_risk.daily_turnover,
    kpi_with_risk.days_of_stock,
    CASE
      WHEN kpi_with_risk.days_of_stock < 7 THEN 'HIGH'
      WHEN kpi_with_risk.days_of_stock < 14 THEN 'MID'
      ELSE 'LOW'
    END AS stock_risk_level,
    rank_data.revenue_rank_in_day
  FROM (
    SELECT
      joined.sales_date,
      joined.product_id,
      joined.product_name,
      joined.category,
      joined.brand,
      joined.sold_qty,
      joined.revenue,
      cum.cumulative_revenue,
      diff.diff_from_prev_day,
      joined.stock_qty,
      joined.avg_daily_sold_qty,
      joined.daily_turnover,
      joined.days_of_stock
    FROM (
      SELECT
        trend.sales_date,
        trend.product_id,
        p.name AS product_name,
        p.category,
        p.brand,
        trend.sold_qty,
        trend.revenue,
        trend.cumulative_revenue,
        trend.diff_from_prev_day,
        cs.stock_qty,
        ad.avg_daily_sold_qty,
        SAFE_DIVIDE(trend.sold_qty, cs.stock_qty) AS daily_turnover,
        SAFE_DIVIDE(cs.stock_qty, ad.avg_daily_sold_qty) AS days_of_stock
      FROM (
        SELECT
          ds.sales_date,
          ds.product_id,
          ds.sold_qty,
          ds.revenue,
          cum_sum.cumulative_revenue,
          prev_diff.diff_from_prev_day
        FROM (
          SELECT
            DATE(o.created_at) AS sales_date,
            oi.product_id,
            COUNT(*) AS sold_qty,
            SUM(oi.sale_price) AS revenue
          FROM `bigquery-public-data.thelook_ecommerce.orders` o
          JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
            ON o.order_id = oi.order_id
          WHERE o.status = 'Complete'
          GROUP BY DATE(o.created_at), oi.product_id
        ) ds
        LEFT JOIN (
          SELECT
            ds2.product_id,
            ds2.sales_date,
            SUM(ds3.revenue) AS cumulative_revenue
          FROM (
            SELECT
              DATE(o.created_at) AS sales_date,
              oi.product_id,
              SUM(oi.sale_price) AS revenue
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
              ON o.order_id = oi.order_id
            WHERE o.status = 'Complete'
            GROUP BY DATE(o.created_at), oi.product_id
          ) ds2
          JOIN (
            SELECT
              DATE(o.created_at) AS sales_date,
              oi.product_id,
              SUM(oi.sale_price) AS revenue
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
              ON o.order_id = oi.order_id
            WHERE o.status = 'Complete'
            GROUP BY DATE(o.created_at), oi.product_id
          ) ds3
            ON ds2.product_id = ds3.product_id
            AND ds3.sales_date <= ds2.sales_date
          GROUP BY ds2.product_id, ds2.sales_date
        ) cum_sum
          ON ds.product_id = cum_sum.product_id
          AND ds.sales_date = cum_sum.sales_date
        LEFT JOIN (
          SELECT
            ds4.product_id,
            ds4.sales_date,
            ds4.revenue - ds5.revenue AS diff_from_prev_day
          FROM (
            SELECT
              DATE(o.created_at) AS sales_date,
              oi.product_id,
              SUM(oi.sale_price) AS revenue
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
              ON o.order_id = oi.order_id
            WHERE o.status = 'Complete'
            GROUP BY DATE(o.created_at), oi.product_id
          ) ds4
          LEFT JOIN (
            SELECT
              DATE(o.created_at) AS sales_date,
              oi.product_id,
              SUM(oi.sale_price) AS revenue
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
              ON o.order_id = oi.order_id
            WHERE o.status = 'Complete'
            GROUP BY DATE(o.created_at), oi.product_id
          ) ds5
            ON ds4.product_id = ds5.product_id
            AND ds5.sales_date = DATE_SUB(ds4.sales_date, INTERVAL 1 DAY)
        ) prev_diff
          ON ds.product_id = prev_diff.product_id
          AND ds.sales_date = prev_diff.sales_date
      ) trend
      LEFT JOIN (
        SELECT
          product_id,
          COUNT(*) AS stock_qty
        FROM `bigquery-public-data.thelook_ecommerce.inventory_items`
        WHERE sold_at IS NULL
        GROUP BY product_id
      ) cs
        ON trend.product_id = cs.product_id
      LEFT JOIN (
        SELECT
          product_id,
          AVG(sold_qty) AS avg_daily_sold_qty
        FROM (
          SELECT
            DATE(o.created_at) AS sales_date,
            oi.product_id,
            COUNT(*) AS sold_qty
          FROM `bigquery-public-data.thelook_ecommerce.orders` o
          JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
            ON o.order_id = oi.order_id
          WHERE o.status = 'Complete'
            AND DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          GROUP BY DATE(o.created_at), oi.product_id
        )
        GROUP BY product_id
      ) ad
        ON trend.product_id = ad.product_id
      LEFT JOIN (
        SELECT
          * EXCEPT(sku)
        FROM `bigquery-public-data.thelook_ecommerce.products`
      ) p
        ON trend.product_id = p.id
    ) joined
    LEFT JOIN (
      SELECT
        ds6.product_id,
        ds6.sales_date,
        SUM(ds7.revenue) AS cumulative_revenue
      FROM (
        SELECT
          DATE(o.created_at) AS sales_date,
          oi.product_id,
          SUM(oi.sale_price) AS revenue
        FROM `bigquery-public-data.thelook_ecommerce.orders` o
        JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
          ON o.order_id = oi.order_id
        WHERE o.status = 'Complete'
        GROUP BY DATE(o.created_at), oi.product_id
      ) ds6
      JOIN (
        SELECT
          DATE(o.created_at) AS sales_date,
          oi.product_id,
          SUM(oi.sale_price) AS revenue
        FROM `bigquery-public-data.thelook_ecommerce.orders` o
        JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
          ON o.order_id = oi.order_id
        WHERE o.status = 'Complete'
        GROUP BY DATE(o.created_at), oi.product_id
      ) ds7
        ON ds6.product_id = ds7.product_id
        AND ds7.sales_date <= ds6.sales_date
      GROUP BY ds6.product_id, ds6.sales_date
    ) cum
      ON joined.product_id = cum.product_id
      AND joined.sales_date = cum.sales_date
    LEFT JOIN (
      SELECT
        ds8.product_id,
        ds8.sales_date,
        ds8.revenue - ds9.revenue AS diff_from_prev_day
      FROM (
        SELECT
          DATE(o.created_at) AS sales_date,
          oi.product_id,
          SUM(oi.sale_price) AS revenue
        FROM `bigquery-public-data.thelook_ecommerce.orders` o
        JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
          ON o.order_id = oi.order_id
        WHERE o.status = 'Complete'
        GROUP BY DATE(o.created_at), oi.product_id
      ) ds8
      LEFT JOIN (
        SELECT
          DATE(o.created_at) AS sales_date,
          oi.product_id,
          SUM(oi.sale_price) AS revenue
        FROM `bigquery-public-data.thelook_ecommerce.orders` o
        JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
          ON o.order_id = oi.order_id
        WHERE o.status = 'Complete'
        GROUP BY DATE(o.created_at), oi.product_id
      ) ds9
        ON ds8.product_id = ds9.product_id
        AND ds9.sales_date = DATE_SUB(ds8.sales_date, INTERVAL 1 DAY)
    ) diff
      ON joined.product_id = diff.product_id
      AND joined.sales_date = diff.sales_date
  ) kpi_with_risk
  LEFT JOIN (
    SELECT
      r1.sales_date,
      r1.product_id,
      COUNT(*) + 1 AS revenue_rank_in_day
    FROM (
      SELECT
        DATE(o.created_at) AS sales_date,
        oi.product_id,
        SUM(oi.sale_price) AS revenue
      FROM `bigquery-public-data.thelook_ecommerce.orders` o
      JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
        ON o.order_id = oi.order_id
      WHERE o.status = 'Complete'
      GROUP BY DATE(o.created_at), oi.product_id
    ) r1
    LEFT JOIN (
      SELECT
        DATE(o.created_at) AS sales_date,
        oi.product_id,
        SUM(oi.sale_price) AS revenue
      FROM `bigquery-public-data.thelook_ecommerce.orders` o
      JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
        ON o.order_id = oi.order_id
      WHERE o.status = 'Complete'
      GROUP BY DATE(o.created_at), oi.product_id
    ) r2
      ON r1.sales_date = r2.sales_date
      AND r2.revenue > r1.revenue
    GROUP BY r1.sales_date, r1.product_id
  ) rank_data
    ON kpi_with_risk.product_id = rank_data.product_id
    AND kpi_with_risk.sales_date = rank_data.sales_date
  WHERE
    CASE
      WHEN kpi_with_risk.days_of_stock < 7 THEN 'HIGH'
      WHEN kpi_with_risk.days_of_stock < 14 THEN 'MID'
      ELSE 'LOW'
    END IN ('HIGH', 'MID')
) ranked
WHERE ranked.revenue_rank_in_day <= 20
ORDER BY
  ranked.sales_date DESC,
  ranked.revenue DESC;

