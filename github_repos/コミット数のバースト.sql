WITH base AS (
  SELECT
    repo_name AS repo,
    DATE(author.date) AS dt,
    COUNT(*) AS commit_cnt
  FROM `bigquery-public-data.github_repos.sample_commits`
  WHERE author.date BETWEEN '2008-01-01' AND '2025-12-31'
  GROUP BY repo, dt
),

scored AS (
  SELECT
    *,
    AVG(commit_cnt) OVER w AS avg,
    STDDEV(commit_cnt) OVER w AS std,
    SAFE_DIVIDE(commit_cnt - AVG(commit_cnt) OVER w,
                STDDEV(commit_cnt) OVER w) AS z
  FROM base
  WINDOW w AS (
    PARTITION BY repo
    ORDER BY dt
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  )
),

labeled AS (
  SELECT
    *,
    CASE
      WHEN z >= 1.5 THEN 'BURST'
      ELSE 'NORMAL'
    END AS state
  FROM scored
)

SELECT *
FROM labeled
MATCH_RECOGNIZE (
  PARTITION BY repo
  ORDER BY dt

  MEASURES
    MATCH_NUMBER() AS match_id,
    FIRST(A.dt) AS start_dt,
    LAST(B.dt) AS end_dt,
    COUNT(*) AS length

  PATTERN (A{2,} B{2,})

  DEFINE
    A AS state = 'NORMAL',
    B AS state = 'BURST'
         AND commit_cnt > PREV(commit_cnt)
);