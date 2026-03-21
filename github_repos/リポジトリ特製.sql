WITH file_base AS (
  SELECT
    f.repo_name,
    f.path,
    f.id,
    c.content,
    c.size
  FROM `bigquery-public-data.github_repos.sample_files` f
  JOIN `bigquery-public-data.github_repos.sample_contents` c
  ON f.id = c.id
  WHERE c.binary = FALSE
),

-- ファイルタイプ分類
file_types AS (
  SELECT
    repo_name,
    path,
    size,

    CASE
      WHEN path LIKE '%.py' THEN 'python'
      WHEN path LIKE '%.js' THEN 'javascript'
      WHEN path LIKE '%.java' THEN 'java'
      WHEN path LIKE '%.go' THEN 'go'
      WHEN path LIKE '%.cpp' OR path LIKE '%.cc' THEN 'cpp'
      ELSE 'other'
    END AS file_type
  FROM file_base
),

-- ディレクトリ深さ
path_features AS (
  SELECT
    repo_name,
    path,
    size,
    ARRAY_LENGTH(SPLIT(path, '/')) AS depth
  FROM file_types
),

-- コード内容の簡易解析
content_features AS (
  SELECT
    repo_name,

    -- コメント率（超簡易）
    AVG(
      SAFE_DIVIDE(
        ARRAY_LENGTH(REGEXP_EXTRACT_ALL(content, r'//|#|/\*')),
        LENGTH(content)
      )
    ) AS comment_density,

    -- import文の多さ
    AVG(
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(content, r'\bimport\b'))
    ) AS import_freq,

    -- 関数っぽいもの
    AVG(
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(content, r'\bfunction\b|\bdef\b'))
    ) AS function_freq

  FROM file_base
  GROUP BY repo_name
),

-- ファイル構造統計
structure_stats AS (
  SELECT
    repo_name,

    COUNT(*) AS file_count,
    AVG(size) AS avg_size,
    MAX(size) AS max_size,

    AVG(depth) AS avg_depth,
    MAX(depth) AS max_depth

  FROM path_features
  GROUP BY repo_name
),

-- 言語情報
language_stats AS (
  SELECT
    repo_name,

    -- language は ARRAY<STRUCT<...>> なので UNNEST してから ARRAY_AGG
    (SELECT ARRAY_AGG(lang.name ORDER BY lang.bytes DESC) FROM UNNEST(language) AS lang) AS lang_list,

    (
      SELECT lang.name
      FROM UNNEST(language) lang
      ORDER BY lang.bytes DESC
      LIMIT 1
    ) AS main_language

  FROM `bigquery-public-data.github_repos.languages`
),

-- ファイルタイプ分布
file_type_dist AS (
  SELECT
    repo_name,

    COUNTIF(file_type = 'python') AS py_files,
    COUNTIF(file_type = 'javascript') AS js_files,
    COUNTIF(file_type = 'java') AS java_files,

    COUNT(*) AS total_files

  FROM file_types
  GROUP BY repo_name
),

-- サブクエリを使った偏り指標
type_ratios AS (
  SELECT
    ftd.*,

    SAFE_DIVIDE(py_files, total_files) AS py_ratio,
    SAFE_DIVIDE(js_files, total_files) AS js_ratio,
    SAFE_DIVIDE(java_files, total_files) AS java_ratio,

    (
      SELECT MAX(x)
      FROM UNNEST([
        SAFE_DIVIDE(py_files, total_files),
        SAFE_DIVIDE(js_files, total_files),
        SAFE_DIVIDE(java_files, total_files)
      ]) AS x
    ) AS dominant_ratio

  FROM file_type_dist ftd
),

-- 全結合
combined AS (
  SELECT
    ss.repo_name,
    ss.file_count,
    ss.avg_size,
    ss.max_size,
    ss.avg_depth,
    ss.max_depth,

    cf.comment_density,
    cf.import_freq,
    cf.function_freq,

    tr.py_ratio,
    tr.js_ratio,
    tr.java_ratio,
    tr.dominant_ratio,

    ls.main_language

  FROM structure_stats ss
  LEFT JOIN content_features cf
    ON ss.repo_name = cf.repo_name
  LEFT JOIN type_ratios tr
    ON ss.repo_name = tr.repo_name
  LEFT JOIN language_stats ls
    ON ss.repo_name = ls.repo_name
),

-- スコアリング
scored AS (
  SELECT
    *,

    -- 複雑さ
    avg_depth * LOG(1 + file_count) AS complexity_score,

    -- 可読性
    comment_density * 100 AS readability_score,

    -- モジュール性
    SAFE_DIVIDE(function_freq, file_count) AS modularity_score,

    -- 技術集中度
    dominant_ratio AS tech_focus_score

  FROM combined
)

-- 最終出力
SELECT
  *,
  RANK() OVER (ORDER BY complexity_score DESC) AS complexity_rank,
  RANK() OVER (ORDER BY readability_score DESC) AS readability_rank,
  RANK() OVER (ORDER BY modularity_score DESC) AS modularity_rank
FROM scored
LIMIT 100;