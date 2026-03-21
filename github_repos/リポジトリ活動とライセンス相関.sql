-- ============================================================
-- リポジトリのコミット活動とライセンス・言語の相関分析
-- ============================================================

WITH
-- 1. リポジトリごとのコミット統計（CTE）
commit_stats AS (
  SELECT
    repo_name,
    COUNT(*) AS total_commits,
    COUNT(DISTINCT DATE(author.date)) AS active_days,
    MIN(DATE(author.date)) AS first_commit,
    MAX(DATE(author.date)) AS last_commit,
    SUM(ARRAY_LENGTH(difference)) AS total_file_changes
  FROM `bigquery-public-data.github_repos.sample_commits`
  WHERE author.date BETWEEN '2008-01-01' AND '2025-12-31'
  GROUP BY repo_name
),

-- 2. 月別コミット推移（CTE）
monthly_commits AS (
  SELECT
    repo_name,
    DATE_TRUNC(DATE(author.date), MONTH) AS month,
    COUNT(*) AS commit_cnt
  FROM `bigquery-public-data.github_repos.sample_commits`
  WHERE author.date BETWEEN '2008-01-01' AND '2025-12-31'
  GROUP BY repo_name, month
),

-- 3. 月次トレンド（前月比）をサブクエリで計算
monthly_trend AS (
  SELECT
    mc.repo_name,
    mc.month,
    mc.commit_cnt,
    LAG(mc.commit_cnt) OVER (PARTITION BY mc.repo_name ORDER BY mc.month) AS prev_month_cnt,
    SAFE_DIVIDE(
      mc.commit_cnt - LAG(mc.commit_cnt) OVER (PARTITION BY mc.repo_name ORDER BY mc.month),
      NULLIF(LAG(mc.commit_cnt) OVER (PARTITION BY mc.repo_name ORDER BY mc.month), 0)
    ) AS month_over_month_ratio
  FROM monthly_commits mc
),

-- 4. sample_repos と licenses を結合（サブクエリでフィルタ）
repos_with_license AS (
  SELECT
    sr.repo_name,
    sr.watch_count,
    COALESCE(l.license, 'UNLICENSED') AS license
  FROM `bigquery-public-data.github_repos.sample_repos` sr
  LEFT JOIN `bigquery-public-data.github_repos.licenses` l
    ON sr.repo_name = l.repo_name
  WHERE sr.repo_name IN (
    SELECT repo_name
    FROM `bigquery-public-data.github_repos.sample_commits`
    WHERE author.date >= '2008-01-01'
    GROUP BY repo_name
    HAVING COUNT(*) >= 10
  )
),

-- 5. 言語情報（ARRAY を UNNEST するサブクエリ）
language_primary AS (
  SELECT
    repo_name,
    (
      SELECT lang.name
      FROM UNNEST(language) AS lang
      ORDER BY lang.bytes DESC
      LIMIT 1
    ) AS main_language,
    (
      SELECT SUM(lang.bytes)
      FROM UNNEST(language) AS lang
    ) AS total_bytes
  FROM `bigquery-public-data.github_repos.languages`
  WHERE repo_name IN (SELECT repo_name FROM repos_with_license)
),

-- 6. ファイル数（サブクエリで集計）
file_counts AS (
  SELECT
    repo_name,
    COUNT(*) AS file_count,
    COUNT(DISTINCT path) AS unique_paths
  FROM (
    SELECT repo_name, path
    FROM `bigquery-public-data.github_repos.sample_files`
    WHERE repo_name IN (SELECT repo_name FROM repos_with_license)
  )
  GROUP BY repo_name
),

-- 7. ライセンス別の集計（CTE 内でサブクエリ）
license_agg AS (
  SELECT
    rwl.license,
    COUNT(DISTINCT rwl.repo_name) AS repo_count,
    (
      SELECT AVG(cs.total_commits)
      FROM commit_stats cs
      WHERE cs.repo_name IN (
        SELECT repo_name FROM repos_with_license r2 WHERE r2.license = rwl.license
      )
    ) AS avg_commits_per_license
  FROM repos_with_license rwl
  GROUP BY rwl.license
),

-- 8. 統合ビュー（複数 CTE とサブクエリを組み合わせ）
combined AS (
  SELECT
    rwl.repo_name,
    rwl.watch_count,
    rwl.license,
    cs.total_commits,
    cs.active_days,
    cs.first_commit,
    cs.last_commit,
    cs.total_file_changes,
    lp.main_language,
    lp.total_bytes,
    fc.file_count,
    fc.unique_paths,
    (
      SELECT AVG(mt.month_over_month_ratio)
      FROM monthly_trend mt
      WHERE mt.repo_name = rwl.repo_name
        AND mt.month_over_month_ratio IS NOT NULL
    ) AS avg_mom_ratio
  FROM repos_with_license rwl
  INNER JOIN commit_stats cs
    ON rwl.repo_name = cs.repo_name
  LEFT JOIN language_primary lp
    ON rwl.repo_name = lp.repo_name
  LEFT JOIN file_counts fc
    ON rwl.repo_name = fc.repo_name
),

-- 9. スコア計算（相関スコアをサブクエリで正規化）
scored AS (
  SELECT
    *,
    SAFE_DIVIDE(total_commits, NULLIF(file_count, 0)) AS commits_per_file,
    SAFE_DIVIDE(total_file_changes, NULLIF(total_commits, 0)) AS avg_changes_per_commit,
    -- 全体でのコミット数順位（サブクエリで相対比較）
    (
      SELECT COUNT(*) + 1
      FROM combined c2
      WHERE c2.total_commits > combined.total_commits
    ) AS commit_rank
  FROM combined
),

-- 10. ライセンス別ランキング（ウィンドウ関数 + サブクエリ）
ranked AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY license ORDER BY total_commits DESC) AS rank_in_license,
    RANK() OVER (PARTITION BY main_language ORDER BY watch_count DESC) AS rank_in_language
  FROM scored
  WHERE total_commits >= (
    SELECT APPROX_QUANTILES(total_commits, 100)[OFFSET(25)]
    FROM scored
  )
)

-- ============================================================
-- 最終出力：ライセンス・言語の相関サマリ
-- ============================================================
SELECT
  r.license,
  r.main_language,
  COUNT(*) AS repo_count,
  AVG(r.total_commits) AS avg_commits,
  AVG(r.commits_per_file) AS avg_commits_per_file,
  AVG(r.avg_mom_ratio) AS avg_growth_trend,
  MAX(r.watch_count) AS max_watch_count
FROM ranked r
WHERE r.main_language IS NOT NULL
  AND r.license NOT IN (
    SELECT license
    FROM license_agg
    WHERE repo_count < 2
  )
GROUP BY r.license, r.main_language
HAVING COUNT(*) >= 1
ORDER BY avg_commits DESC, repo_count DESC
LIMIT 50;