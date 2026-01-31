# CSV データを BigQuery にロードするサンプルプロジェクト

このプロジェクトは、pandas と google-cloud-bigquery を使って CSV データを BigQuery にロードする実務ベースのサンプルです。

## セットアップ

### 1. 依存関係のインストール

```bash
uv sync
```

### 2. BigQuery 認証の設定

Google Cloud の認証情報を設定します。

1. Google Cloud Console でプロジェクトを作成
2. BigQuery API を有効化
3. サービスアカウントを作成し、JSON キーをダウンロード
4. ダウンロードした JSON キーをプロジェクトの適切な場所に配置（例：`credential.json`）

### 3. 環境変数の設定（.env ファイル）

プロジェクトのルートディレクトリに `.env` ファイルを作成し、以下の内容を記述します：

```bash
# .env ファイル
# BigQuery 設定
PROJECT_ID=your-project-id
DATASET_ID=sample_dataset
TABLE_ID=sales_data

# CSV ファイルのパス（オプション、デフォルト: data/sample_sales.csv）
CSV_PATH=data/sample_sales.csv

# Google Cloud 認証情報のパス
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
```

**重要**: `.env` ファイルには機密情報が含まれるため、**必ず Git にコミットしないでください**。

**注意**: `GOOGLE_APPLICATION_CREDENTIALS` のパスは、プロジェクトのルートディレクトリからの相対パス、または絶対パスで指定できます。`load_dotenv()` により、`.env` ファイルの内容が自動的に環境変数として設定されます。

## 使い方

### CSV データのロード

```bash
uv run python src/load_csv_to_bigquery.py
```

### BigQuery 操作の確認

BigQuery の基本的な操作（データセット一覧、テーブル一覧、クエリ実行など）を確認するスクリプト：

```bash
uv run python src/bigquery_operations.py
```

このスクリプトでは以下の操作が可能です：
- データセット一覧の取得
- テーブル一覧の取得
- テーブル情報の表示（行数、サイズ、スキーマなど）
- テーブルデータのプレビュー
- カスタム SQL クエリの実行（コード内でコメントアウトされている部分を編集して使用）

### コードチェック

```bash
# すべてのチェックを実行
uv run poe check
```

## データ形式

`data/sample_sales.csv` は以下の形式の CSV ファイルです：

```csv
date,product_name,quantity,price,category
2024-01-01,商品A,10,1000,カテゴリ1
...
```

実際のデータに合わせて、CSV ファイルの形式を変更してください。
