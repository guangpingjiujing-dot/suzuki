WITH base AS (
  SELECT
    repo_name AS repo,
    DATE(author.date) AS dt,
    COUNT(*) AS commit_cnt,
    SUM(ARRAY_LENGTH(difference)) AS file_changes
  FROM `bigquery-public-data.github_repos.sample_commits`
  WHERE author.date BETWEEN '2008-01-01' AND '2025-12-31'
  GROUP BY repo, dt
),

-- 欠損日補完（カレンダー生成）
calendar AS (
  SELECT
    repo,
    day AS dt
  FROM (
    SELECT DISTINCT repo FROM base
  ),
  UNNEST(GENERATE_DATE_ARRAY('2008-01-01', '2025-12-31')) AS day
),

filled AS (
  SELECT
    c.repo,
    c.dt,
    IFNULL(b.commit_cnt, 0) AS commit_cnt,
    IFNULL(b.file_changes, 0) AS file_changes
  FROM calendar c
  LEFT JOIN base b
  ON c.repo = b.repo AND c.dt = b.dt
),

-- ローリング特徴量
rolling AS (
  SELECT
    *,
    SUM(commit_cnt) OVER w AS sum_7d,
    AVG(commit_cnt) OVER w AS avg_7d,
    MAX(commit_cnt) OVER w AS max_7d,
    MIN(commit_cnt) OVER w AS min_7d
  FROM filled
  WINDOW w AS (
    PARTITION BY repo
    ORDER BY dt
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  )
),

-- トレンド計算
trend AS (
  SELECT
    *,
    commit_cnt - LAG(commit_cnt) OVER w AS diff,
    SAFE_DIVIDE(commit_cnt,
                NULLIF(LAG(commit_cnt) OVER w, 0)) AS ratio
  FROM rolling
  WINDOW w AS (
    PARTITION BY repo ORDER BY dt
  )
),

-- サブクエリでrepo全体特性
repo_stats AS (
  SELECT
    repo,
    (
      SELECT AVG(commit_cnt)
      FROM filled f2
      WHERE f2.repo = f.repo
    ) AS overall_avg,
    (
      SELECT MAX(commit_cnt)
      FROM filled f3
      WHERE f3.repo = f.repo
    ) AS overall_max
  FROM filled f
  GROUP BY repo
),

-- 結合
enriched AS (
  SELECT
    t.*,
    rs.overall_avg,
    rs.overall_max
  FROM trend t
  JOIN repo_stats rs
  ON t.repo = rs.repo
),

-- フェーズ分類（構造ベース）
labeled AS (
  SELECT
    *,
    CASE
      WHEN commit_cnt <= overall_avg * 0.5 THEN 'QUIET'
      WHEN ratio > 1.2 AND commit_cnt < overall_max * 0.7 THEN 'BUILDUP'
      WHEN commit_cnt >= overall_max * 0.7 THEN 'PEAK'
      WHEN ratio < 0.7 THEN 'COOLDOWN'
      ELSE 'OTHER'
    END AS phase
  FROM enriched
)

-- MATCH_RECOGNIZEでスプリント検出
SELECT *
FROM labeled
MATCH_RECOGNIZE (
  PARTITION BY repo
  ORDER BY dt

  MEASURES
    MATCH_NUMBER() AS match_id,
    FIRST(A.dt) AS start_dt,
    LAST(C.dt) AS peak_dt,
    COUNT(*) AS duration

  PATTERN (A+ X* B+ X* C)

  DEFINE
    A AS phase = 'QUIET',

    B AS phase = 'BUILDUP'
         AND commit_cnt >= PREV(commit_cnt) * 0.9,

    C AS phase = 'PEAK'
         AND commit_cnt >= overall_avg * 1.2,

    X AS phase = 'OTHER'
);