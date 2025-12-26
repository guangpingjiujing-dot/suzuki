-- =====================================================
-- 悪い例：WITH句をサブクエリ化、Window関数をGROUP BYで冗長化
-- =====================================================
SELECT
  final_ranked.distribution_center_id,
  final_ranked.distribution_center_name,
  final_ranked.total_orders,
  final_ranked.total_revenue,
  final_ranked.total_profit,
  final_ranked.avg_order_revenue,
  final_ranked.avg_products_per_order,
  final_ranked.avg_hours_to_ship,
  final_ranked.avg_hours_to_deliver,
  final_ranked.returned_orders,
  final_ranked.return_rate,
  final_ranked.revenue_rank,
  final_ranked.delivery_speed_rank,
  final_ranked.profit_quartile
FROM (
  SELECT
    geo.distribution_center_id,
    geo.distribution_center_name,
    geo.total_orders,
    geo.total_revenue,
    geo.total_profit,
    geo.avg_order_revenue,
    geo.avg_products_per_order,
    geo.avg_hours_to_ship,
    geo.avg_hours_to_deliver,
    geo.returned_orders,
    geo.return_rate,
    rev_rank.revenue_rank,
    speed_rank.delivery_speed_rank,
    quartile_calc.profit_quartile
  FROM (
    SELECT
      dc.id AS distribution_center_id,
      dc.name AS distribution_center_name,
      dcm.total_orders,
      dcm.total_revenue,
      dcm.total_profit,
      dcm.avg_order_revenue,
      dcm.avg_products_per_order,
      dcm.avg_hours_to_ship,
      dcm.avg_hours_to_deliver,
      dcm.returned_orders,
      dcm.return_rate
    FROM `bigquery-public-data.thelook_ecommerce.distribution_centers` dc
    JOIN (
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
      FROM (
        SELECT
          *,
          TIMESTAMP_DIFF(shipped_at, order_created_at, HOUR) AS hours_to_ship,
          TIMESTAMP_DIFF(delivered_at, shipped_at, HOUR) AS hours_to_deliver
        FROM (
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
          FROM (
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
          ) bo
          JOIN (
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
          ) oip
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
        )
        WHERE shipped_at IS NOT NULL
      )
      GROUP BY distribution_center_id
    ) dcm
      ON dc.id = dcm.distribution_center_id
  ) geo
  LEFT JOIN (
    SELECT
      rev_base.distribution_center_id,
      COUNT(*) + 1 AS revenue_rank
    FROM (
      SELECT
        dcm2.distribution_center_id,
        dcm2.total_revenue
      FROM (
        SELECT
          distribution_center_id,
          SUM(order_revenue) AS total_revenue
        FROM (
          SELECT
            bo.order_id,
            oip.distribution_center_id,
            SUM(oip.sale_price) AS order_revenue
          FROM (
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
          ) bo
          JOIN (
            SELECT
              oi.order_id,
              oi.product_id,
              oi.sale_price,
              p.distribution_center_id
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            JOIN `bigquery-public-data.thelook_ecommerce.products` p
              ON oi.product_id = p.id
          ) oip
            ON bo.order_id = oip.order_id
          GROUP BY bo.order_id, oip.distribution_center_id
        )
        GROUP BY distribution_center_id
      ) dcm2
    ) rev_base
    LEFT JOIN (
      SELECT
        dcm3.distribution_center_id,
        dcm3.total_revenue
      FROM (
        SELECT
          distribution_center_id,
          SUM(order_revenue) AS total_revenue
        FROM (
          SELECT
            bo.order_id,
            oip.distribution_center_id,
            SUM(oip.sale_price) AS order_revenue
          FROM (
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
          ) bo
          JOIN (
            SELECT
              oi.order_id,
              oi.product_id,
              oi.sale_price,
              p.distribution_center_id
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            JOIN `bigquery-public-data.thelook_ecommerce.products` p
              ON oi.product_id = p.id
          ) oip
            ON bo.order_id = oip.order_id
          GROUP BY bo.order_id, oip.distribution_center_id
        )
        GROUP BY distribution_center_id
      ) dcm3
    ) rev_compare
      ON rev_compare.total_revenue > rev_base.total_revenue
    GROUP BY rev_base.distribution_center_id
  ) rev_rank
    ON geo.distribution_center_id = rev_rank.distribution_center_id
  LEFT JOIN (
    SELECT
      speed_base.distribution_center_id,
      COUNT(*) + 1 AS delivery_speed_rank
    FROM (
      SELECT
        dcm4.distribution_center_id,
        dcm4.avg_hours_to_deliver
      FROM (
        SELECT
          distribution_center_id,
          AVG(hours_to_deliver) AS avg_hours_to_deliver
        FROM (
          SELECT
            bo.order_id,
            oip.distribution_center_id,
            TIMESTAMP_DIFF(bo.delivered_at, bo.shipped_at, HOUR) AS hours_to_deliver
          FROM (
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
              AND shipped_at IS NOT NULL
          ) bo
          JOIN (
            SELECT
              oi.order_id,
              oi.product_id,
              p.distribution_center_id
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            JOIN `bigquery-public-data.thelook_ecommerce.products` p
              ON oi.product_id = p.id
          ) oip
            ON bo.order_id = oip.order_id
        )
        GROUP BY distribution_center_id
      ) dcm4
    ) speed_base
    LEFT JOIN (
      SELECT
        dcm5.distribution_center_id,
        dcm5.avg_hours_to_deliver
      FROM (
        SELECT
          distribution_center_id,
          AVG(hours_to_deliver) AS avg_hours_to_deliver
        FROM (
          SELECT
            bo.order_id,
            oip.distribution_center_id,
            TIMESTAMP_DIFF(bo.delivered_at, bo.shipped_at, HOUR) AS hours_to_deliver
          FROM (
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
              AND shipped_at IS NOT NULL
          ) bo
          JOIN (
            SELECT
              oi.order_id,
              oi.product_id,
              p.distribution_center_id
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            JOIN `bigquery-public-data.thelook_ecommerce.products` p
              ON oi.product_id = p.id
          ) oip
            ON bo.order_id = oip.order_id
        )
        GROUP BY distribution_center_id
      ) dcm5
    ) speed_compare
      ON speed_compare.avg_hours_to_deliver < speed_base.avg_hours_to_deliver
    GROUP BY speed_base.distribution_center_id
  ) speed_rank
    ON geo.distribution_center_id = speed_rank.distribution_center_id
  LEFT JOIN (
    SELECT
      quartile_base.distribution_center_id,
      CASE
        WHEN quartile_base.rank_val <= quartile_total.total_count * 0.25 THEN 1
        WHEN quartile_base.rank_val <= quartile_total.total_count * 0.50 THEN 2
        WHEN quartile_base.rank_val <= quartile_total.total_count * 0.75 THEN 3
        ELSE 4
      END AS profit_quartile
    FROM (
      SELECT
        profit_rank.distribution_center_id,
        COUNT(*) AS rank_val
      FROM (
        SELECT
          dcm6.distribution_center_id,
          dcm6.total_profit
        FROM (
          SELECT
            distribution_center_id,
            SUM(order_profit) AS total_profit
          FROM (
            SELECT
              bo.order_id,
              oip.distribution_center_id,
              SUM(oip.item_profit) AS order_profit
            FROM (
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
            ) bo
            JOIN (
              SELECT
                oi.order_id,
                oi.product_id,
                oi.sale_price,
                p.cost,
                p.distribution_center_id,
                (oi.sale_price - p.cost) AS item_profit
              FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
              JOIN `bigquery-public-data.thelook_ecommerce.products` p
                ON oi.product_id = p.id
            ) oip
              ON bo.order_id = oip.order_id
            GROUP BY bo.order_id, oip.distribution_center_id
          )
          GROUP BY distribution_center_id
        ) dcm6
      ) profit_rank
      LEFT JOIN (
        SELECT
          dcm7.distribution_center_id,
          dcm7.total_profit
        FROM (
          SELECT
            distribution_center_id,
            SUM(order_profit) AS total_profit
          FROM (
            SELECT
              bo.order_id,
              oip.distribution_center_id,
              SUM(oip.item_profit) AS order_profit
            FROM (
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
            ) bo
            JOIN (
              SELECT
                oi.order_id,
                oi.product_id,
                oi.sale_price,
                p.cost,
                p.distribution_center_id,
                (oi.sale_price - p.cost) AS item_profit
              FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
              JOIN `bigquery-public-data.thelook_ecommerce.products` p
                ON oi.product_id = p.id
            ) oip
              ON bo.order_id = oip.order_id
            GROUP BY bo.order_id, oip.distribution_center_id
          )
          GROUP BY distribution_center_id
        ) dcm7
      ) profit_compare
        ON profit_compare.total_profit > profit_rank.total_profit
      GROUP BY profit_rank.distribution_center_id
    ) quartile_base
    CROSS JOIN (
      SELECT
        COUNT(*) AS total_count
      FROM (
        SELECT DISTINCT
          distribution_center_id
        FROM (
          SELECT
            bo.order_id,
            oip.distribution_center_id
          FROM (
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
          ) bo
          JOIN (
            SELECT
              oi.order_id,
              oi.product_id,
              p.distribution_center_id
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            JOIN `bigquery-public-data.thelook_ecommerce.products` p
              ON oi.product_id = p.id
          ) oip
            ON bo.order_id = oip.order_id
        )
      )
    ) quartile_total
  ) quartile_calc
    ON geo.distribution_center_id = quartile_calc.distribution_center_id
) final_ranked
WHERE
  final_ranked.revenue_rank <= 10
  AND final_ranked.total_orders >= 500
ORDER BY
  final_ranked.total_profit DESC;

