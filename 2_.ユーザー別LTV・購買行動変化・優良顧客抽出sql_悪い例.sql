-- =====================================================
-- 悪い例：WITH句をサブクエリ化、QUALIFY句を使わず、Window関数をGROUP BYで冗長化
-- =====================================================
SELECT
  final_result.id,
  final_result.first_name,
  final_result.last_name,
  final_result.gender,
  final_result.state,
  final_result.city,
  final_result.country,
  final_result.age,
  final_result.total_orders,
  final_result.ltv,
  final_result.avg_order_value,
  final_result.avg_items_per_order,
  final_result.avg_days_between_orders,
  final_result.customer_segment,
  final_result.rank_in_segment
FROM (
  SELECT
    seg.user_id,
    seg.total_orders,
    seg.ltv,
    seg.avg_order_value,
    seg.avg_items_per_order,
    seg.avg_days_between_orders,
    seg.customer_segment,
    rank_sub.rank_in_segment,
    u.id,
    u.first_name,
    u.last_name,
    u.gender,
    u.state,
    u.city,
    u.country,
    u.age
  FROM (
    SELECT
      kpi.user_id,
      kpi.total_orders,
      kpi.ltv,
      kpi.avg_order_value,
      kpi.avg_items_per_order,
      kpi.avg_days_between_orders,
      CASE
        WHEN kpi.ltv >= avg_bench.avg_ltv_all_users
         AND kpi.total_orders >= avg_bench.avg_orders_all_users
        THEN 'VIP'
        WHEN kpi.total_orders >= 2
        THEN 'REPEAT'
        ELSE 'ONE_TIME'
      END AS customer_segment
    FROM (
      SELECT
        user_id,
        COUNT(*) AS total_orders,
        SUM(order_revenue) AS ltv,
        AVG(order_revenue) AS avg_order_value,
        AVG(item_qty) AS avg_items_per_order,
        AVG(days_since_prev_order) AS avg_days_between_orders,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
      FROM (
        SELECT
          uo.user_id,
          uo.order_id,
          uo.order_date,
          uo.order_revenue,
          uo.item_qty,
          DATE_DIFF(uo.order_date, prev.prev_order_date, DAY) AS days_since_prev_order
        FROM (
          SELECT
            ob.user_id,
            ob.order_id,
            ob.order_date,
            r.order_revenue,
            r.item_qty
          FROM (
            SELECT
              o.order_id,
              o.user_id,
              DATE(o.created_at) AS order_date
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            WHERE o.status = 'Complete'
          ) ob
          JOIN (
            SELECT
              oi.order_id,
              SUM(oi.sale_price) AS order_revenue,
              COUNT(*) AS item_qty
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            GROUP BY oi.order_id
          ) r
            ON ob.order_id = r.order_id
        ) uo
        LEFT JOIN (
          SELECT
            uo2.user_id,
            uo2.order_date,
            MAX(uo3.order_date) AS prev_order_date
          FROM (
            SELECT
              ob.user_id,
              ob.order_id,
              DATE(o.created_at) AS order_date
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            WHERE o.status = 'Complete'
          ) uo2
          LEFT JOIN (
            SELECT
              ob.user_id,
              ob.order_id,
              DATE(o.created_at) AS order_date
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            WHERE o.status = 'Complete'
          ) uo3
            ON uo2.user_id = uo3.user_id
            AND uo3.order_date < uo2.order_date
          GROUP BY uo2.user_id, uo2.order_date
        ) prev
          ON uo.user_id = prev.user_id
          AND uo.order_date = prev.order_date
      )
      GROUP BY user_id
    ) kpi
    CROSS JOIN (
      SELECT
        AVG(ltv) AS avg_ltv_all_users,
        AVG(total_orders) AS avg_orders_all_users
      FROM (
        SELECT
          user_id,
          COUNT(*) AS total_orders,
          SUM(order_revenue) AS ltv
        FROM (
          SELECT
            ob.user_id,
            ob.order_id,
            r.order_revenue
          FROM (
            SELECT
              o.order_id,
              o.user_id,
              DATE(o.created_at) AS order_date
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            WHERE o.status = 'Complete'
          ) ob
          JOIN (
            SELECT
              oi.order_id,
              SUM(oi.sale_price) AS order_revenue
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            GROUP BY oi.order_id
          ) r
            ON ob.order_id = r.order_id
        )
        GROUP BY user_id
      )
    ) avg_bench
  ) seg
  JOIN (
    SELECT
      rank_base.user_id,
      rank_base.customer_segment,
      COUNT(*) + 1 AS rank_in_segment
    FROM (
      SELECT
        kpi2.user_id,
        kpi2.ltv,
        CASE
          WHEN kpi2.ltv >= avg_bench2.avg_ltv_all_users
           AND kpi2.total_orders >= avg_bench2.avg_orders_all_users
          THEN 'VIP'
          WHEN kpi2.total_orders >= 2
          THEN 'REPEAT'
          ELSE 'ONE_TIME'
        END AS customer_segment
      FROM (
        SELECT
          user_id,
          COUNT(*) AS total_orders,
          SUM(order_revenue) AS ltv
        FROM (
          SELECT
            ob.user_id,
            ob.order_id,
            r.order_revenue
          FROM (
            SELECT
              o.order_id,
              o.user_id,
              DATE(o.created_at) AS order_date
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            WHERE o.status = 'Complete'
          ) ob
          JOIN (
            SELECT
              oi.order_id,
              SUM(oi.sale_price) AS order_revenue
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            GROUP BY oi.order_id
          ) r
            ON ob.order_id = r.order_id
        )
        GROUP BY user_id
      ) kpi2
      CROSS JOIN (
        SELECT
          AVG(ltv) AS avg_ltv_all_users,
          AVG(total_orders) AS avg_orders_all_users
        FROM (
          SELECT
            user_id,
            COUNT(*) AS total_orders,
            SUM(order_revenue) AS ltv
          FROM (
            SELECT
              ob.user_id,
              ob.order_id,
              r.order_revenue
            FROM (
              SELECT
                o.order_id,
                o.user_id,
                DATE(o.created_at) AS order_date
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            WHERE o.status = 'Complete'
          ) ob
          JOIN (
            SELECT
              oi.order_id,
              SUM(oi.sale_price) AS order_revenue
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            GROUP BY oi.order_id
          ) r
            ON ob.order_id = r.order_id
        )
        GROUP BY user_id
      )
    ) avg_bench2
  ) rank_base
  LEFT JOIN (
    SELECT
      kpi3.user_id,
      kpi3.ltv,
      CASE
        WHEN kpi3.ltv >= avg_bench3.avg_ltv_all_users
         AND kpi3.total_orders >= avg_bench3.avg_orders_all_users
        THEN 'VIP'
        WHEN kpi3.total_orders >= 2
        THEN 'REPEAT'
        ELSE 'ONE_TIME'
      END AS customer_segment
    FROM (
      SELECT
        user_id,
        COUNT(*) AS total_orders,
        SUM(order_revenue) AS ltv
      FROM (
        SELECT
          ob.user_id,
          ob.order_id,
          r.order_revenue
        FROM (
          SELECT
            o.order_id,
            o.user_id,
            DATE(o.created_at) AS order_date
          FROM `bigquery-public-data.thelook_ecommerce.orders` o
          WHERE o.status = 'Complete'
        ) ob
        JOIN (
          SELECT
            oi.order_id,
            SUM(oi.sale_price) AS order_revenue
          FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
          GROUP BY oi.order_id
        ) r
          ON ob.order_id = r.order_id
      )
      GROUP BY user_id
    ) kpi3
    CROSS JOIN (
      SELECT
        AVG(ltv) AS avg_ltv_all_users,
        AVG(total_orders) AS avg_orders_all_users
      FROM (
        SELECT
          user_id,
          COUNT(*) AS total_orders,
          SUM(order_revenue) AS ltv
        FROM (
          SELECT
            ob.user_id,
            ob.order_id,
            r.order_revenue
          FROM (
            SELECT
              o.order_id,
              o.user_id,
              DATE(o.created_at) AS order_date
            FROM `bigquery-public-data.thelook_ecommerce.orders` o
            WHERE o.status = 'Complete'
          ) ob
          JOIN (
            SELECT
              oi.order_id,
              SUM(oi.sale_price) AS order_revenue
            FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
            GROUP BY oi.order_id
          ) r
            ON ob.order_id = r.order_id
        )
        GROUP BY user_id
      )
    ) avg_bench3
    ) rank_compare
      ON rank_base.customer_segment = rank_compare.customer_segment
      AND rank_compare.ltv > rank_base.ltv
    GROUP BY rank_base.user_id, rank_base.customer_segment
  ) rank_sub
    ON seg.user_id = rank_sub.user_id
    AND seg.customer_segment = rank_sub.customer_segment
  JOIN `bigquery-public-data.thelook_ecommerce.users` u
    ON seg.user_id = u.id
  WHERE rank_sub.rank_in_segment <= 100
) final_result
ORDER BY
  final_result.customer_segment DESC,
  final_result.ltv DESC;

