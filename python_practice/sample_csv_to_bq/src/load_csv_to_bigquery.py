"""CSV データを BigQuery にロードするサンプルプログラム"""

import os
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
from dotenv import load_dotenv

# .env ファイルから環境変数を読み込む
load_dotenv()

# デフォルト設定値
DEFAULT_CSV_PATH = "data/sample_sales.csv"


def load_csv_to_bigquery(
    csv_path: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> None:
    """
    CSV ファイルを読み込んで BigQuery にロードする

    Args:
        csv_path: CSV ファイルのパス
        project_id: BigQuery プロジェクト ID
        dataset_id: データセット ID
        table_id: テーブル ID
    """
    # CSV ファイルを読み込む
    print(f"Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)

    print(f"Loaded {len(df)} rows")
    print("\nData preview:")
    print(df.head())

    # BigQuery クライアントの作成
    client = bigquery.Client(project=project_id)

    # テーブル参照の作成
    table_ref = client.dataset(dataset_id).table(table_id)

    # スキーマの自動検出
    # autodetect=True の動作:
    # - BigQuery が CSV ファイルの内容を自動的に解析してスキーマ（カラム名、データ型）を推測します
    # - テーブルが存在しない場合: 自動検出されたスキーマで新しいテーブルが自動作成されます
    # - テーブルが既に存在する場合: 既存のスキーマと互換性があるかチェックされます
    #   互換性がない場合はエラーになります（write_disposition の設定に関わらず）
    # 注意: データ型の推測は完璧ではない場合があるため、重要なテーブルでは明示的にスキーマを指定することを推奨します
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # データを BigQuery にロード
    print(f"\nLoading data to BigQuery: {project_id}.{dataset_id}.{table_id}")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    # ジョブの完了を待つ
    job.result()

    # ロード結果の確認
    table = client.get_table(table_ref)
    print(f"\nSuccessfully loaded {table.num_rows} rows to {table_id}")


if __name__ == "__main__":
    # 環境変数から設定値を読み込む
    CSV_PATH = os.getenv("CSV_PATH", DEFAULT_CSV_PATH)
    PROJECT_ID = os.getenv("PROJECT_ID")
    DATASET_ID = os.getenv("DATASET_ID")
    TABLE_ID = os.getenv("TABLE_ID")

    # 必須の環境変数が設定されているか確認
    if not PROJECT_ID:
        raise ValueError("PROJECT_ID 環境変数が設定されていません。.env ファイルを確認してください。")
    if not DATASET_ID:
        raise ValueError("DATASET_ID 環境変数が設定されていません。.env ファイルを確認してください。")
    if not TABLE_ID:
        raise ValueError("TABLE_ID 環境変数が設定されていません。.env ファイルを確認してください。")

    load_csv_to_bigquery(
        csv_path=CSV_PATH,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
    )
