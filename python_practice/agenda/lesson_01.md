# 第1回：開発環境の構築と BigQuery へのデータロード

## 学習目標
- 現代的な Python 開発環境（uv、Ruff、mypy、poethepoet、pre-commit）を構築できるようになる
- pandas と google-cloud-bigquery を使って CSV データを BigQuery にロードできるようになる

---

## uv とは

この授業では、従来の `pip` の代わりに **uv** というパッケージマネージャを使用します。uv は pip と比較して以下のような利点があります：

### 主な利点

1. **圧倒的な速度**
   - pip と比較して数倍以上の速さでパッケージのインストールが可能
   - Rust で実装されており、高速な処理が可能

2. **依存関係の完全な文書化**
   - `uv.lock` ファイルに、インストールされたすべてのパッケージの正確なバージョンとハッシュ値が自動的に記録される
   - Python のバージョン情報も含まれるため、環境の再現が容易
   - `pip freeze` のように手動で実行する必要がない

3. **プロジェクトの包括的な管理**
   - パッケージ管理だけでなく、Python のバージョン管理も統合されている
   - `pyenv` など別ツールでバージョンを切り替える手間が不要
   - 仮想環境（`.venv`）も自動的に管理される

これらの利点により、uv は現在の Python 開発環境のデファクトスタンダードとなっています。

---

## 1. 開発環境の構築

### 1.1 uv のインストール

まず、uv をインストールします。環境に応じたインストール方法は[公式ドキュメント](https://github.com/astral-sh/uv)を参照してください。

Windows の場合：
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

macOS/Linux の場合：
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 1.2 プロジェクトの初期化

作業ディレクトリでプロジェクトを初期化します：

```bash
uv init sample_dev_env
cd sample_dev_env
```

これで以下のような構成のプロジェクトが作成されます：

#### 作成されるファイルとディレクトリ

- **`.git/`** - Git リポジトリ
  - `uv init` を実行すると、自動的に Git リポジトリが初期化されます
  - バージョン管理をすぐに始められます

- **`.gitignore`** - Git の除外設定
  - Python プロジェクトで一般的に除外すべきファイル（`__pycache__/`、`.venv/` など）が自動的に設定されます
  - 仮想環境やキャッシュファイルが Git に含まれないようになります

- **`.python-version`** - Python バージョン情報
  - このプロジェクトで使用する Python のバージョンが記録されます
  - 例：`3.12.0` など
  - uv はこのファイルを参照して、適切な Python バージョンを自動的に使用します

- **`README.md`** - プロジェクトの説明
  - プロジェクトの概要や使い方を記載するファイル
  - 初期状態では基本的なテンプレートが作成されます

- **`main.py`** - サンプルファイル
  - プロジェクトのエントリーポイントとなるサンプルコード
  - すぐに実行できる簡単なプログラムが含まれています

- **`pyproject.toml`** - プロジェクト設定ファイル
  - プロジェクトのメタデータ（名前、バージョン、説明など）を定義
  - 依存関係の管理に使用されます
  - 初期状態では以下のような内容が含まれます：
    ```toml
    [project]
    name = "sample-dev-env"
    version = "0.1.0"
    description = "Add your description here"
    requires-python = ">=3.11"
    dependencies = []
    ```

#### パッケージ追加後に作成されるファイル

`uv add` コマンドでパッケージを追加すると、以下のファイルが作成・更新されます：

- **`uv.lock`** - 依存関係のロックファイル
  - インストールされたすべてのパッケージの**正確なバージョン**と**ハッシュ値**が記録されます
  - 例：`numpy` を追加すると、`numpy` とその依存パッケージの詳細情報が記録されます
  - このファイルにより、**いつ、誰が、どの環境で** `uv sync` を実行しても、**バイナリレベルでの同一性**が保証されます
  - `uv.lock` の内容例：
    ```toml
    [[package]]
    name = "numpy"
    version = "2.4.1"
    source = { registry = "https://pypi.org/simple" }
    sdist = { url = "...", hash = "sha256:...", ... }
    wheels = [...]
    ```
  - このファイルは**必ず Git にコミット**してください。チーム開発や環境再現に必要です

- **`.venv/`** - 仮想環境ディレクトリ（自動生成）
  - プロジェクト専用の Python 仮想環境が作成されます
  - このディレクトリは `.gitignore` に含まれているため、Git には含まれません
  - 他の環境で再現する場合は `uv sync` を実行すれば自動的に作成されます

- **`pyproject.toml`** - 依存関係の追加
  - `uv add` で追加したパッケージが `dependencies` セクションに自動的に追加されます
  - 開発依存関係（`--dev` フラグ付き）は `[project.optional-dependencies]` セクションに追加されます

### 1.3 開発ツールのインストール

開発環境で使用するツールをインストールします：

```bash
uv add numpy
uv add --dev ruff mypy poethepoet pre-commit
```

#### `--dev` オプションについて

`--dev` オプションは、**開発依存関係（開発時のみ必要なパッケージ）**をインストールする際に使用します。

**通常の依存関係と開発依存関係の違い：**

- **通常の依存関係**（`uv add` のみ）
  - アプリケーションの実行時に必要なパッケージ
  - 例：`numpy`、`pandas`、`google-cloud-bigquery` など
  - `pyproject.toml` の `[project]` セクションの `dependencies` に追加される
  - 本番環境でも必要

- **開発依存関係**（`uv add --dev`）
  - 開発時のみ必要なツール（コードチェック、テスト、フォーマットなど）
  - 例：`ruff`（リンタ・フォーマッタ）、`mypy`（型検査）、`poethepoet`（タスクランナー）、`pre-commit`（コミット前チェック）など
  - `pyproject.toml` の `[project.optional-dependencies]` セクションに追加される
  - 本番環境では不要

**なぜ開発依存関係を分けるのか：**
- 本番環境へのデプロイ時に、開発ツールを含めないことで環境を軽量化できる
- 依存関係が明確になり、どのパッケージが本番で必要かを把握しやすくなる
- チーム開発で、開発環境と本番環境の違いを明確にできる

パッケージを削除したい場合は、`uv remove` コマンドを使用します：

```bash
# 通常の依存関係から削除
uv remove パッケージ名
uv remove numpy
```

---

## 2. 開発環境の動作確認（サンプルプログラム）

開発環境が正しく動作するか、簡単なサンプルプログラムで確認します。

### 2.1 サンプルプロジェクトのセットアップ

`sample_dev_env` ディレクトリに移動して、プロジェクトをセットアップします：

```bash
cd sample_dev_env
uv sync
uv run pre-commit install
```

### 2.2 サンプルプログラムの確認

まず、意図的に問題を含むコードを作成して、リンタやフォーマッタがどのように動作するかを確認します。

`src/main.py` に以下のコードを記述します（意図的に問題を含んでいます）：

```python
import sys
import os

def greet(  name: str  ) -> str:
    """名前を受け取って挨拶を返す関数"""
    if name == None:
        return f"Hello, World!"
    return f"Hello, {name}!"


def calculate_sum(numbers: list[int]) -> int:
    """数値のリストを受け取って合計を返す関数"""
    total = 0
    for num in numbers:
        total += num
    
    unused_var = 100
    
    return total


if __name__ == "__main__":
    message = greet("Python")
    print( f"{message}" )
    
    numbers = [1, 2, 3, 4, 5]
    result = calculate_sum(numbers)
    print(f"Sum: {result}")
```

このコードには以下の問題があります：
- `sys` と `os` をインポートしているが、使用していない
- 関数の引数の前後に不要なスペースがある
- `None` との比較に `==` を使っている（`is None` を使うべき）
- `unused_var` という変数を定義しているが使用していない
- f-string に不要な `f` プレフィックスがある（変数埋め込みがない場合）

### 2.3 各ツールの動作確認

これらの問題を、リンタやフォーマッタがどのように検出・修正するか確認します。

#### Ruff によるフォーマット

まず、コードのフォーマット（スタイルの統一）を確認します：

```bash
uv run ruff format src/
```

実行すると、以下のような問題が自動的に修正されます：
- 関数の引数の前後の不要なスペースが削除される
- f-string の不要な `f` プレフィックスが削除される

修正後は `1 file reformatted` と表示されます。

#### Ruff によるリントチェック

次に、コードの品質チェックを実行します：

```bash
uv run ruff check src/
```

実行すると、以下のような問題が検出されます：
- `sys` と `os` が未使用であること
- `None` との比較に `==` を使っていること
- `unused_var` が未使用であること

自動修正可能な問題は `--fix` オプションで修正できます：

```bash
uv run ruff check --fix src/
```

#### mypy による型検査

mypy は、Python で**静的型検査**を行うツールです。Ruff が「構文や規約」をチェックするのに対し、mypy は「データの型の整合性」をチェックする役割を担います。

##### Python の型システムについて

Python は**動的型付け言語**です。これは、プログラムの実行時に型が決まる言語という意味です。一方、**静的型付け言語**（C++、Java など）では、プログラムを実行する前に型の整合性をチェックします。

動的型付けの利点は簡潔なコーディングが可能なことですが、実行するまで型の不整合を検出できず、処理の途中でエラーが発生する可能性があります。

mypy を使うことで、Python でも静的型付け言語風の型検査を行うことができ、**実行前に型の不整合を検出**できます。

##### 型ヒントとは

型ヒント（Type Hints）とは、コード内で型を明記する Python の機能です。

**変数の型ヒント：**
```python
num: int = 8
name: str = "Python"
```

**関数の型ヒント：**
```python
def greet(name: str) -> str:
    return f"Hello, {name}!"
```

- 引数の型は `変数名: 型` の形式で記述
- 戻り値の型は `-> 型` の形式で記述
- 戻り値がない場合は `-> None` と記述

型ヒント自体はコードの挙動に影響しませんが、可読性を向上させ、mypy による型検査の情報源となります。

##### mypy の実行

型検査を実行します：

```bash
uv run mypy src/
```

##### 型エラーの例

型エラーがある場合、例えば以下のようなコードでは：

```python
def greet(name: str) -> str:
    return f"Hello, {name}!"

greet(123)  # int を渡しているが、str が期待されている
```

mypy は以下のようなエラーを出力します：

```
src/main.py:5: error: Argument 1 to "greet" has incompatible type "int"; expected "str"  [arg-type]
Found 1 error in 1 file (checked 1 source file)
```

##### 型推論

関数の入出力に型ヒントを記述すれば、関数内の変数の型は mypy が自動的に推論（型推論）してくれます：

```python
def calculate_sum(numbers: list[int]) -> int:
    total = 0  # mypy は total が int であることを推論
    for num in numbers:  # mypy は num が int であることを推論
        total += num
    return total
```

##### 型ヒントがない場合

mypy は型ヒントを情報源としているため、型ヒントがないコードに対してはチェックを行いません：

```python
def greet(name):  # 型ヒントがない
    return f"Hello, {name}!"

greet(123)  # 型エラーは検出されない
```

この場合、`uv run mypy src/` を実行しても `Success: no issues found` と表示されます。

そのため、mypy の効果を最大限に活用するには、**関数の入出力に型ヒントを記述する**ことが重要です。

#### 修正後のコード例

Ruff による自動修正後、コードは以下のようになります：

```python
import os  # sys は削除される（未使用のため）


def greet(name: str) -> str:
    """名前を受け取って挨拶を返す関数"""
    if name is None:  # == None から is None に修正
        return "Hello, World!"  # f プレフィックスが削除
    return f"Hello, {name}!"


def calculate_sum(numbers: list[int]) -> int:
    """数値のリストを受け取って合計を返す関数"""
    total = 0
    for num in numbers:
        total += num
    
    # unused_var は削除される（未使用のため）
    
    return total


if __name__ == "__main__":
    message = greet("Python")
    print(message)  # f プレフィックスが削除
    
    numbers = [1, 2, 3, 4, 5]
    result = calculate_sum(numbers)
    print(f"Sum: {result}")
```


#### poethepoet による一括チェック

poethepoet を使うことで、複数のチェックコマンドを1つのコマンドで実行できます。

まず、`pyproject.toml` に以下の設定を追加します：

```toml
[tool.poe.tasks]
format = "uv run ruff format ."
lint = "uv run ruff check --fix ."
type-check = "uv run mypy ."
check = ["format", "lint", "type-check"]
```

これで、以下のコマンドで一括チェックが実行できます：

```bash
uv run poe check
```

このコマンドは、`format`、`lint`、`type-check` の順に実行されます。

#### pre-commit の設定

pre-commit を使うことで、Git コミット前に自動的にコードチェックを実行できます。

`.pre-commit-config.yaml` ファイルを作成し、以下の内容を記述します：

```yaml
repos:
  - repo: local
    hooks:
      - id: poe-check
        name: poe check
        entry: uv run poe check
        language: system
        types: [python]
        pass_filenames: false
```

pre-commit をインストールします：

```bash
uv run pre-commit install
```

これで、Git でコミットする前に自動的に `poe check` が実行され、問題がある場合はコミットが拒否されます。

pre-commit をアンインストール（無効化）する場合は、以下のコマンドを実行します：

```bash
uv run pre-commit uninstall
```

これで、Git フックが削除され、コミット前の自動チェックが無効になります。

### 2.4 プログラムの実行

```bash
uv run python src/main.py
```

---

## 3. 実務ベースのサンプル：CSV データを BigQuery にロード

開発環境の動作確認ができたら、実務でよくある「CSV データを BigQuery にロードする」処理を実装します。

### 3.1 サンプルプロジェクトのセットアップ

`sample_csv_to_bq` ディレクトリに移動して、プロジェクトをセットアップします：

```bash
cd ../sample_csv_to_bq
uv sync
uv run pre-commit install
```

### 3.2 環境変数の設定（.env ファイル）

設定値をコードに直接書くのではなく、`.env` ファイルを使って環境変数として管理します。

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
GOOGLE_APPLICATION_CREDENTIALS=./credential.json.json
```

**重要**: `.env` ファイルには機密情報が含まれるため、**必ず Git にコミットしないでください**。`.gitignore` に `.env` が含まれていることを確認してください。

**注意**: `GOOGLE_APPLICATION_CREDENTIALS` のパスは、プロジェクトのルートディレクトリからの相対パス、または絶対パスで指定できます。`load_dotenv()` により、`.env` ファイルの内容が自動的に環境変数として設定されます。

### 3.3 サンプル CSV データの確認

`data/sample_sales.csv` には以下のデータが含まれています：

```csv
date,product_name,quantity,price,category
2024-01-01,商品A,10,1000,カテゴリ1
2024-01-02,商品B,5,2000,カテゴリ2
2024-01-03,商品A,8,1000,カテゴリ1
2024-01-04,商品C,3,3000,カテゴリ2
2024-01-05,商品B,12,2000,カテゴリ2
```

### 3.4 BigQuery 認証の設定

BigQuery に接続するには、Google Cloud の認証情報が必要です。

1. Google Cloud Console でプロジェクトを作成
2. BigQuery API を有効化
3. サービスアカウントを作成し、JSON キーをダウンロード
4. ダウンロードした JSON キーをプロジェクトの適切な場所に配置（例：`credential.json`）
5. `.env` ファイルに `GOOGLE_APPLICATION_CREDENTIALS` を設定（上記の 3.2 を参照）

### 3.5 CSV を BigQuery にロードするプログラムの確認

`src/load_csv_to_bigquery.py` には以下のコードが記述されています：

```python
"""CSV データを BigQuery にロードするサンプルプログラム"""

import os
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
from dotenv import load_dotenv

# .env ファイルから環境変数を読み込む
load_dotenv()


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
    CSV_PATH = os.getenv("CSV_PATH", "data/sample_sales.csv")
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
```


### 3.6 プログラムの実行

`.env` ファイルに設定値を記述したら、プログラムを実行します：

```bash
uv run python src/load_csv_to_bigquery.py
```

**環境変数の確認方法：**

環境変数が正しく読み込まれているか確認したい場合は、以下のように確認できます：

```bash
# Windows PowerShell
Get-Content .env

# macOS/Linux
cat .env
```

また、プログラム実行時に環境変数が設定されていない場合は、エラーメッセージが表示されます。

---

## 4. まとめ

今回の授業では以下のことを学習しました：

1. **開発環境の構築**
   - uv によるプロジェクト管理
   - Ruff によるコード品質チェック
   - mypy による型検査
   - poethepoet によるタスク管理
   - pre-commit によるコミット前チェック

2. **実務での活用**
   - pandas による CSV データの読み込み
   - BigQuery へのデータロード
   - 型ヒントを使ったコードの可読性向上

次回以降は、より高度なデータ処理や分析手法について学習していきます。

---

## 補足：よくあるエラーと対処法

### BigQuery 認証エラー

```
google.auth.exceptions.DefaultCredentialsError: Could not automatically determine credentials.
```

**対処法**: `GOOGLE_APPLICATION_CREDENTIALS` 環境変数が正しく設定されているか確認してください。

### パッケージが見つからないエラー

```
ModuleNotFoundError: No module named 'pandas'
```

**対処法**: `uv sync` を実行して依存関係をインストールしてください。

### 型エラー（mypy）

```
error: Argument 1 to "load_csv_to_bigquery" has incompatible type "str"; expected "Path"
```

**対処法**: 型ヒントを確認し、必要に応じて型変換を行ってください。
