"""BigQuery の基本的な操作を確認するサンプルプログラム"""

import os
from google.cloud import bigquery
from dotenv import load_dotenv

# .env ファイルから環境変数を読み込む
load_dotenv()

# デフォルト設定値
DEFAULT_PROJECT_ID = None
DEFAULT_DATASET_ID = None


def list_datasets(project_id: str) -> None:
    """
    プロジェクト内のデータセット一覧を取得して表示する

    Args:
        project_id: BigQuery プロジェクト ID
    """
    print(f"\n=== データセット一覧 ({project_id}) ===")
    client = bigquery.Client(project=project_id)

    datasets = client.list_datasets()
    found = False
    for dataset in datasets:
        found = True
        print(f"  - {dataset.dataset_id}")
        print(f"    Full ID: {dataset.full_dataset_id}")

    if not found:
        print("データセットが見つかりませんでした。")


def list_tables(project_id: str, dataset_id: str) -> None:
    """
    データセット内のテーブル一覧を取得して表示する

    Args:
        project_id: BigQuery プロジェクト ID
        dataset_id: データセット ID
    """
    print(f"\n=== テーブル一覧 ({project_id}.{dataset_id}) ===")
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)

    try:
        tables = client.list_tables(dataset_ref)
        found = False
        for table in tables:
            found = True
            print(f"  - {table.table_id}")
            print(f"    Full ID: {table.full_table_id}")

        if not found:
            print("テーブルが見つかりませんでした。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")


def get_table_info(project_id: str, dataset_id: str, table_id: str) -> None:
    """
    テーブルの詳細情報を取得して表示する

    Args:
        project_id: BigQuery プロジェクト ID
        dataset_id: データセット ID
        table_id: テーブル ID
    """
    print(f"\n=== テーブル情報 ({project_id}.{dataset_id}.{table_id}) ===")
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        print(f"テーブル ID: {table.table_id}")
        print(f"フルテーブル ID: {table.full_table_id}")
        print(f"行数: {table.num_rows:,}")
        print(f"サイズ: {table.num_bytes / (1024 * 1024):.2f} MB")
        print(f"作成日時: {table.created}")
        print(f"更新日時: {table.modified}")

        print("\nスキーマ:")
        for field in table.schema:
            print(f"  - {field.name}: {field.field_type} ({'NULLABLE' if field.mode == 'NULLABLE' else 'REQUIRED'})")
    except Exception as e:
        print(f"エラーが発生しました: {e}")


def execute_query(project_id: str, query: str) -> None:
    """
    SQL クエリを実行して結果を表示する

    Args:
        project_id: BigQuery プロジェクト ID
        query: 実行する SQL クエリ
    """
    print(f"\n=== クエリ実行 ===")
    print(f"クエリ: {query}")
    print("-" * 80)

    client = bigquery.Client(project=project_id)

    try:
        query_job = client.query(query)
        results = query_job.result()

        # 結果を表示
        rows = list(results)
        if not rows:
            print("結果がありませんでした。")
            return

        # カラム名を取得
        columns = [field.name for field in results.schema]
        print(f"\nカラム: {', '.join(columns)}")
        print("-" * 80)

        # 結果を表示（最大10行まで）
        max_rows = 10
        for i, row in enumerate(rows[:max_rows]):
            print(f"行 {i + 1}: {dict(row)}")

        if len(rows) > max_rows:
            print(f"\n... 他 {len(rows) - max_rows} 行が省略されました")

        print(f"\n合計行数: {len(rows)}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")


def preview_table_data(project_id: str, dataset_id: str, table_id: str, limit: int = 5) -> None:
    """
    テーブルのデータをプレビューする

    Args:
        project_id: BigQuery プロジェクト ID
        dataset_id: データセット ID
        table_id: テーブル ID
        limit: 表示する行数（デフォルト: 5）
    """
    query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_id}`
    LIMIT {limit}
    """
    execute_query(project_id, query)


def main() -> None:
    """メイン処理"""
    # 環境変数から設定値を読み込む
    PROJECT_ID = os.getenv("PROJECT_ID", DEFAULT_PROJECT_ID)
    DATASET_ID = os.getenv("DATASET_ID", DEFAULT_DATASET_ID)

    if not PROJECT_ID:
        raise ValueError("PROJECT_ID 環境変数が設定されていません。.env ファイルを確認してください。")

    print("=" * 80)
    print("BigQuery 操作確認スクリプト")
    print("=" * 80)

    # 1. データセット一覧を表示
    list_datasets(PROJECT_ID)

    # 2. データセットIDが指定されている場合、テーブル一覧を表示
    if DATASET_ID:
        list_tables(PROJECT_ID, DATASET_ID)

        # 3. テーブルIDが指定されている場合、テーブル情報を表示
        TABLE_ID = os.getenv("TABLE_ID")
        if TABLE_ID:
            get_table_info(PROJECT_ID, DATASET_ID, TABLE_ID)
            preview_table_data(PROJECT_ID, DATASET_ID, TABLE_ID)

    # 4. カスタムクエリの例
    # 必要に応じてコメントアウトして使用を無効化できます
    custom_query = f"""
    SELECT COUNT(*) as total_rows
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """
    execute_query(PROJECT_ID, custom_query)

    print("\n" + "=" * 80)
    print("処理が完了しました。")
    print("=" * 80)


if __name__ == "__main__":
    main()
