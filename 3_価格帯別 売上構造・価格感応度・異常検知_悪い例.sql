-- =====================================================
-- 悪い例：WITH句をサブクエリ化、Window関数をGROUP BYで冗長化
-- =====================================================
SELECT
  final_output.product_id,
  final_output.product_name,
  final_output.category,
  final_output.total_sold_qty,
  final_output.total_revenue,
  final_output.avg_price_overall,
  final_output.avg_category_price,
  final_output.std_category_price,
  final_output.price_z_score,
  final_output.revenue_rank_in_category,
  final_output.price_position,
  final_output.price_sensitivity_flag
FROM (
  SELECT
    ranked.product_id,
    ranked.product_name,
    ranked.category,
    ranked.total_sold_qty,
    ranked.total_revenue,
    ranked.avg_price_overall,
    ranked.avg_category_price,
    ranked.std_category_price,
    ranked.price_z_score,
    ranked.revenue_rank_in_category,
    CASE
      WHEN ranked.price_z_score >= 2 THEN 'VERY_EXPENSIVE'
      WHEN ranked.price_z_score >= 1 THEN 'EXPENSIVE'
      WHEN ranked.price_z_score <= -2 THEN 'VERY_CHEAP'
      WHEN ranked.price_z_score <= -1 THEN 'CHEAP'
      ELSE 'NORMAL'
    END AS price_position,
    CASE
      WHEN ranked.price_z_score > 0 AND ranked.total_sold_qty > 100 THEN 'HIGH_PRICE_STRONG_SALES'
      WHEN ranked.price_z_score < 0 AND ranked.total_sold_qty < 20 THEN 'LOW_PRICE_WEAK_SALES'
      ELSE 'NORMAL_BEHAVIOR'
    END AS price_sensitivity_flag
  FROM (
    SELECT
      dev.product_id,
      dev.product_name,
      dev.category,
      dev.total_sold_qty,
      dev.total_revenue,
      dev.avg_price_overall,
      dev.avg_category_price,
      dev.std_category_price,
      dev.price_z_score,
      rank_calc.revenue_rank_in_category
    FROM (
      SELECT
        ps.product_id,
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
      FROM (
        SELECT
          product_id,
          SUM(sold_qty) AS total_sold_qty,
          SUM(revenue) AS total_revenue,
          AVG(avg_sale_price) AS avg_price_overall
        FROM (
          SELECT
            order_date,
            product_id,
            COUNT(*) AS sold_qty,
            SUM(sale_price) AS revenue,
            AVG(sale_price) AS avg_sale_price
          FROM (
            SELECT
              oi.order_id,
              oi.product_id,
              oi.sale_price,
              DATE(o.created_at) AS order_date
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            JOIN `bigquery-public-data.thelook_ecommerce.orders` o
              ON oi.order_id = o.order_id
            WHERE o.status = 'Complete'
          )
          GROUP BY order_date, product_id
        )
        GROUP BY product_id
      ) ps
      JOIN (
        SELECT
          * EXCEPT (sku)
        FROM `bigquery-public-data.thelook_ecommerce.products`
      ) p
        ON ps.product_id = p.id
      JOIN (
        SELECT
          p2.category,
          AVG(ps2.avg_price_overall) AS avg_category_price,
          STDDEV(ps2.avg_price_overall) AS std_category_price
        FROM (
          SELECT
            product_id,
            AVG(avg_sale_price) AS avg_price_overall
          FROM (
            SELECT
              order_date,
              product_id,
              AVG(sale_price) AS avg_sale_price
            FROM (
              SELECT
                oi.order_id,
                oi.product_id,
                oi.sale_price,
                DATE(o.created_at) AS order_date
              FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
              JOIN `bigquery-public-data.thelook_ecommerce.orders` o
                ON oi.order_id = o.order_id
              WHERE o.status = 'Complete'
            )
            GROUP BY order_date, product_id
          )
          GROUP BY product_id
        ) ps2
        JOIN (
          SELECT
            * EXCEPT (sku)
          FROM `bigquery-public-data.thelook_ecommerce.products`
        ) p2
          ON ps2.product_id = p2.id
        GROUP BY p2.category
      ) cps
        ON p.category = cps.category
    ) dev
    LEFT JOIN (
      SELECT
        rank_base.product_id,
        rank_base.category,
        COUNT(*) + 1 AS revenue_rank_in_category
      FROM (
        SELECT
          ps3.product_id,
          ps3.total_revenue,
          p3.category
        FROM (
          SELECT
            product_id,
            SUM(revenue) AS total_revenue
          FROM (
            SELECT
              order_date,
              product_id,
              SUM(sale_price) AS revenue
            FROM (
              SELECT
                oi.order_id,
                oi.product_id,
                oi.sale_price,
                DATE(o.created_at) AS order_date
              FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
              JOIN `bigquery-public-data.thelook_ecommerce.orders` o
                ON oi.order_id = o.order_id
              WHERE o.status = 'Complete'
            )
            GROUP BY order_date, product_id
          )
          GROUP BY product_id
        ) ps3
        JOIN (
          SELECT
            * EXCEPT (sku)
          FROM `bigquery-public-data.thelook_ecommerce.products`
        ) p3
          ON ps3.product_id = p3.id
      ) rank_base
      LEFT JOIN (
        SELECT
          ps4.product_id,
          ps4.total_revenue,
          p4.category
        FROM (
          SELECT
            product_id,
            SUM(revenue) AS total_revenue
          FROM (
            SELECT
              order_date,
              product_id,
              SUM(sale_price) AS revenue
            FROM (
              SELECT
                oi.order_id,
                oi.product_id,
                oi.sale_price,
                DATE(o.created_at) AS order_date
              FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
              JOIN `bigquery-public-data.thelook_ecommerce.orders` o
                ON oi.order_id = o.order_id
              WHERE o.status = 'Complete'
            )
            GROUP BY order_date, product_id
          )
          GROUP BY product_id
        ) ps4
        JOIN (
          SELECT
            * EXCEPT (sku)
          FROM `bigquery-public-data.thelook_ecommerce.products`
        ) p4
          ON ps4.product_id = p4.id
      ) rank_compare
        ON rank_base.category = rank_compare.category
        AND rank_compare.total_revenue > rank_base.total_revenue
      GROUP BY rank_base.product_id, rank_base.category
    ) rank_calc
      ON dev.product_id = rank_calc.product_id
      AND dev.category = rank_calc.category
    WHERE rank_calc.revenue_rank_in_category <= 10
  ) ranked
  WHERE
    CASE
      WHEN ranked.price_z_score >= 2 THEN 'VERY_EXPENSIVE'
      WHEN ranked.price_z_score >= 1 THEN 'EXPENSIVE'
      WHEN ranked.price_z_score <= -2 THEN 'VERY_CHEAP'
      WHEN ranked.price_z_score <= -1 THEN 'CHEAP'
      ELSE 'NORMAL'
    END IN ('VERY_EXPENSIVE', 'VERY_CHEAP')
) final_output
ORDER BY
  final_output.category,
  final_output.revenue_rank_in_category;

