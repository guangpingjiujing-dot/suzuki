## Google アナリティクス ログ分析演習

#### 1: 日次セッション数のカウント

- 1-1) 2017 年 7 月 1 日から 2017 年 7 月 7 日までの 1 週間で、日別の合計セッション数（レコード数）をカウントし、日付の昇順で表示してください。

```sql
SELECT
  date,
  COUNT(*) AS sessions
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170707'
GROUP BY
  date
ORDER BY
  date ASC;
```

#### 2: デバイス別ユーザー分析(Struct 型)

- 2-1) デバイスのカテゴリ（`device.deviceCategory`）ごとに、ユニークなユーザー数（`fullVisitorId`のカウント）を求めてください。

```sql
SELECT
  device.deviceCategory,
  COUNT(DISTINCT fullVisitorId) AS unique_users
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
GROUP BY
  1;
```

#### 3: 流入元（Source/Medium）別の成果

- 3-1) `trafficSource.source` と `trafficSource.medium` を結合したチャネル名ごとに、合計セッション数と合計取引数（`totals.transactions`）を集計し、取引数が多い順に表示してください。

```sql
SELECT
  CONCAT(trafficSource.source, ' / ', trafficSource.medium) AS channel,
  COUNT(*) AS sessions,
  SUM(IFNULL(totals.transactions, 0)) AS total_transactions
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
GROUP BY
  1
ORDER BY
  total_transactions DESC;
```

#### 4: UNNEST を使ったヒットデータの展開

- 4-1) `hits` カラムは 1 セッション内の複数の行動が格納されています。`UNNEST(hits)` を使って、各ヒットのページパス（`hits.page.pagePath`）をユーザー ID とともに 10 件表示してください。

```sql
SELECT
  fullVisitorId,
  h.page.pagePath
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_20170701`,
  UNNEST(hits) AS h
LIMIT 10;
```

#### 5: 広告経由のコンバージョン率（CVR）

- 5-1) `medium` が "cpc" の広告経由のセッションに絞り、キャンペーン名（`trafficSource.campaign`）ごとに「セッション数」「購入ユーザー数」「CVR（購入/セッション）」を計算してください。

```sql
SELECT
  trafficSource.campaign,
  COUNT(*) AS sessions,
  COUNT(DISTINCT IF(totals.transactions >= 1, fullVisitorId, NULL)) AS purchasers,
  SAFE_DIVIDE(COUNT(DISTINCT IF(totals.transactions >= 1, fullVisitorId, NULL)), COUNT(*)) * 100 AS cvr_percent
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND trafficSource.medium = 'cpc'
GROUP BY
  1
ORDER BY
  sessions DESC;
```

#### 6: 本格的ファネル分析（3 ステップ）

- 6-1) 「サイト訪問者数」「商品閲覧者数」「購入完了者数」の 3 ステップのユーザー数と、それぞれのステップへの遷移率（ファネル）を、流入ソース（`trafficSource.source`）別に算出してください。

```sql
WITH funnel_data AS (
  SELECT
    trafficSource.source,
    fullVisitorId,
    MAX(IF(h.type = 'PAGE', 1, 0)) AS is_visit,
    MAX(IF(h.eCommerceAction.action_type = '2', 1, 0)) AS is_view,
    MAX(IF(h.eCommerceAction.action_type = '6', 1, 0)) AS is_purchase
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS h
  WHERE
    _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
  GROUP BY
    1, 2
)
SELECT
  source,
  SUM(is_visit) AS total_visitors,
  SUM(is_view) AS view_users,
  SUM(is_purchase) AS purchase_users,
  SAFE_DIVIDE(SUM(is_view), SUM(is_visit)) * 100 AS visit_to_view_rate,
  SAFE_DIVIDE(SUM(is_purchase), SUM(is_view)) * 100 AS view_to_purchase_rate
FROM
  funnel_data
GROUP BY
  1
ORDER BY
  total_visitors DESC;
```

#### 7: 商品別の売上ランキング（多段 UNNEST）

- `hits` の中にはさらに `product` という配列がネストされています。これをすべて展開し、商品名（`v2ProductName`）ごとの合計売上金額を算出してください。
  ※売上金額は `productRevenue` を 1,000,000 で割る必要があります。

```sql
SELECT
  p.v2ProductName,
  SUM(p.productRevenue) / 1000000 AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS h,
  UNNEST(h.product) AS p
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
  AND p.productRevenue IS NOT NULL
GROUP BY
  1
ORDER BY
  revenue DESC
LIMIT 10;
```

#### 8: 初回訪問から 2 回目訪問までの期間（Window 関数）

- 2 回以上訪問しているユーザーに絞り、初回訪問（`visitNumber=1`）と 2 回目訪問（`visitNumber=2`）の間の日数を計算し、その平均値を出してください。

```sql
WITH user_visits AS (
  SELECT
    fullVisitorId,
    visitNumber,
    PARSE_DATE('%Y%m%d', date) AS visit_date
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170101' AND '20170731'
    AND visitNumber IN (1, 2)
)
SELECT
  AVG(DATE_DIFF(v2.visit_date, v1.visit_date, DAY)) AS avg_days_to_return
FROM
  (SELECT * FROM user_visits WHERE visitNumber = 1) v1
JOIN
  (SELECT * FROM user_visits WHERE visitNumber = 2) v2
ON
  v1.fullVisitorId = v2.fullVisitorId;
```

#### 9: 商品ごとの「カゴ落ち率」（商品別ファネル）

- 商品詳細閲覧（`action_type='2'`）からカート追加（`action_type='3'`）に至る割合を商品別に計算し、歩留まりの悪い商品を見つけてください。

```sql
SELECT
  p.v2ProductName,
  COUNT(DISTINCT IF(h.eCommerceAction.action_type = '2', fullVisitorId, NULL)) AS viewers,
  COUNT(DISTINCT IF(h.eCommerceAction.action_type = '3', fullVisitorId, NULL)) AS add_to_cart_users,
  SAFE_DIVIDE(COUNT(DISTINCT IF(h.eCommerceAction.action_type = '3', fullVisitorId, NULL)),
              COUNT(DISTINCT IF(h.eCommerceAction.action_type = '2', fullVisitorId, NULL))) * 100 AS cart_add_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS h,
  UNNEST(h.product) AS p
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
GROUP BY
  1
HAVING
  viewers > 100
ORDER BY
  cart_add_rate ASC;
```

---

#### 10: リピート購入までの日数分布

- 1 回目の購入をした日から、2 回目の購入をするまでの日数をユーザーごとに計算してください。

```sql
WITH purchase_dates AS (
  SELECT
    fullVisitorId,
    PARSE_DATE('%Y%m%d', date) AS purchase_date,
    ROW_NUMBER() OVER(PARTITION BY fullVisitorId ORDER BY date, visitStartTime) AS purchase_seq
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    AND totals.transactions >= 1
)
SELECT
  DATE_DIFF(p2.purchase_date, p1.purchase_date, DAY) AS days_between_purchases,
  COUNT(*) AS user_count
FROM
  purchase_dates p1
JOIN
  purchase_dates p2 ON p1.fullVisitorId = p2.fullVisitorId AND p1.purchase_seq = 1 AND p2.purchase_seq = 2
GROUP BY
  1
ORDER BY
  1;
```

#### 11: 離脱率（Exit Rate）の高いページ

- ページビュー（PV）数が 500 回以上あるページの中で、そのページがセッションの最後になった割合（離脱数 / PV 数）が高い順に表示してください。

```sql
SELECT
  h.page.pagePath,
  COUNT(*) AS pageviews,
  SUM(IF(h.isExit = TRUE, 1, 0)) AS exits,
  SAFE_DIVIDE(SUM(IF(h.isExit = TRUE, 1, 0)), COUNT(*)) * 100 AS exit_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS h
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
  AND h.type = 'PAGE'
GROUP BY
  1
HAVING
  pageviews >= 500
ORDER BY
  exit_rate DESC;
```

#### 12: ユーザーあたりの合計 LTV（期間内合計購入金額）によるランク分け

- 期間内の合計購入金額（LTV）に基づき、ユーザーを 'High' (> $500), 'Medium' ($100-$500), 'Low' (< $100) の 3 段階に分け、それぞれのグループの人数を表示してください。

```sql
WITH user_revenue AS (
  SELECT
    fullVisitorId,
    SUM(totals.totalTransactionRevenue) / 1000000 AS total_rev
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
  GROUP BY
    1
)
SELECT
  CASE
    WHEN total_rev > 500 THEN '1. High'
    WHEN total_rev BETWEEN 100 AND 500 THEN '2. Medium'
    WHEN total_rev > 0 THEN '3. Low'
    ELSE '4. Non-Purchaser'
  END AS ltv_rank,
  COUNT(*) AS user_count
FROM
  user_revenue
GROUP BY
  1
ORDER BY
  1;
```
