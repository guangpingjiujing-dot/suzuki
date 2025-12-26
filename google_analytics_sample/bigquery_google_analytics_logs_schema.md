## テーブル概要

このテーブルは**1 セッション = 1 レコード**の粒度でデータが格納されています。

- 各レコードは 1 つのセッション（訪問）を表します
- `visitId`がセッションを一意に識別する ID です
- 1 セッション内の複数の行動（ページビュー、イベントなど）は`hits`配列に格納されています
- セッション集計値（ページビュー数、滞在時間など）は`totals`構造体に格納されています

## カラム説明

### 基本カラム

| カラム名               | データ型 | 説明                                                      |
| ---------------------- | -------- | --------------------------------------------------------- |
| `visitorId`            | INT64    | 訪問者 ID（数値）                                         |
| `visitNumber`          | INT64    | 訪問回数（1 回目、2 回目など）                            |
| `visitId`              | INT64    | セッション ID（1 セッションを一意に識別）                 |
| `visitStartTime`       | INT64    | セッション開始時刻（Unix タイムスタンプ）                 |
| `date`                 | STRING   | セッション日付（YYYYMMDD 形式）                           |
| `fullVisitorId`        | STRING   | ユニークな訪問者 ID（文字列、ユーザー識別に使用）         |
| `userId`               | STRING   | ユーザー ID（ログインしている場合）                       |
| `channelGrouping`      | STRING   | チャネルグループ（Organic Search、Direct、Referral など） |
| `socialEngagementType` | STRING   | ソーシャルエンゲージメントタイプ                          |

### totals（セッション集計値）

| カラム名                         | データ型 | 説明                                               |
| -------------------------------- | -------- | -------------------------------------------------- |
| `totals.visits`                  | INT64    | 訪問数（通常は 1）                                 |
| `totals.hits`                    | INT64    | ヒット数（セッション内のイベント数）               |
| `totals.pageviews`               | INT64    | ページビュー数                                     |
| `totals.timeOnSite`              | INT64    | サイト滞在時間（秒）                               |
| `totals.bounces`                 | INT64    | バウンス数（1 ページのみで離脱した場合 1）         |
| `totals.transactions`            | INT64    | 取引数（購入回数）                                 |
| `totals.transactionRevenue`      | INT64    | 取引収益（マイクロ単位、100 万で割る必要あり）     |
| `totals.totalTransactionRevenue` | INT64    | 合計取引収益（マイクロ単位、100 万で割る必要あり） |
| `totals.newVisits`               | INT64    | 新規訪問数（新規ユーザーの場合 1）                 |
| `totals.screenviews`             | INT64    | スクリーンビュー数（モバイルアプリ）               |
| `totals.uniqueScreenviews`       | INT64    | ユニークスクリーンビュー数                         |
| `totals.timeOnScreen`            | INT64    | スクリーン滞在時間（秒）                           |
| `totals.sessionQualityDim`       | INT64    | セッション品質指標                                 |

### trafficSource（流入元情報）

| カラム名                                       | データ型 | 説明                                   |
| ---------------------------------------------- | -------- | -------------------------------------- |
| `trafficSource.referralPath`                   | STRING   | リファラーパス                         |
| `trafficSource.campaign`                       | STRING   | キャンペーン名                         |
| `trafficSource.source`                         | STRING   | 流入元（例：google、yahoo）            |
| `trafficSource.medium`                         | STRING   | メディア（例：organic、cpc、referral） |
| `trafficSource.keyword`                        | STRING   | 検索キーワード                         |
| `trafficSource.adContent`                      | STRING   | 広告コンテンツ                         |
| `trafficSource.isTrueDirect`                   | BOOL     | ダイレクトアクセスかどうか             |
| `trafficSource.campaignCode`                   | STRING   | キャンペーンコード                     |
| `trafficSource.adwordsClickInfo.campaignId`    | INT64    | AdWords キャンペーン ID                |
| `trafficSource.adwordsClickInfo.adGroupId`     | INT64    | AdWords 広告グループ ID                |
| `trafficSource.adwordsClickInfo.creativeId`    | INT64    | 広告クリエイティブ ID                  |
| `trafficSource.adwordsClickInfo.criteriaId`    | INT64    | 検索条件 ID                            |
| `trafficSource.adwordsClickInfo.page`          | INT64    | 広告が表示されたページ番号             |
| `trafficSource.adwordsClickInfo.slot`          | STRING   | 広告スロット位置                       |
| `trafficSource.adwordsClickInfo.gclId`         | STRING   | Google Click ID                        |
| `trafficSource.adwordsClickInfo.customerId`    | INT64    | AdWords 顧客 ID                        |
| `trafficSource.adwordsClickInfo.adNetworkType` | STRING   | 広告ネットワークタイプ                 |
| `trafficSource.adwordsClickInfo.isVideoAd`     | BOOL     | 動画広告かどうか                       |

### device（デバイス情報）

| カラム名                        | データ型 | 説明                                        |
| ------------------------------- | -------- | ------------------------------------------- |
| `device.browser`                | STRING   | ブラウザ名（例：Chrome、Firefox）           |
| `device.browserVersion`         | STRING   | ブラウザバージョン                          |
| `device.browserSize`            | STRING   | ブラウザサイズ                              |
| `device.operatingSystem`        | STRING   | OS 名（例：Windows、Macintosh、Android）    |
| `device.operatingSystemVersion` | STRING   | OS バージョン                               |
| `device.isMobile`               | BOOL     | モバイルデバイスかどうか                    |
| `device.mobileDeviceBranding`   | STRING   | モバイルデバイスブランド                    |
| `device.mobileDeviceModel`      | STRING   | モバイルデバイスモデル                      |
| `device.deviceCategory`         | STRING   | デバイスカテゴリ（desktop、mobile、tablet） |
| `device.language`               | STRING   | 言語設定                                    |
| `device.screenResolution`       | STRING   | 画面解像度                                  |
| `device.screenColors`           | STRING   | 画面色数                                    |
| `device.javaEnabled`            | BOOL     | Java 有効かどうか                           |
| `device.flashVersion`           | STRING   | Flash バージョン                            |

### geoNetwork（地理情報）

| カラム名                     | データ型 | 説明                 |
| ---------------------------- | -------- | -------------------- |
| `geoNetwork.continent`       | STRING   | 大陸名               |
| `geoNetwork.subContinent`    | STRING   | サブ大陸名           |
| `geoNetwork.country`         | STRING   | 国名                 |
| `geoNetwork.region`          | STRING   | 地域名               |
| `geoNetwork.metro`           | STRING   | 都市圏               |
| `geoNetwork.city`            | STRING   | 都市名               |
| `geoNetwork.cityId`          | STRING   | 都市 ID              |
| `geoNetwork.networkDomain`   | STRING   | ネットワークドメイン |
| `geoNetwork.latitude`        | STRING   | 緯度                 |
| `geoNetwork.longitude`       | STRING   | 経度                 |
| `geoNetwork.networkLocation` | STRING   | ネットワーク位置     |

### hits（ヒット配列）

`hits`は配列型で、1 セッション内の複数の行動（ページビュー、イベントなど）が格納されています。`UNNEST(hits)`で展開して使用します。

#### hits 基本フィールド

| カラム名             | データ型 | 説明                                             |
| -------------------- | -------- | ------------------------------------------------ |
| `hits.hitNumber`     | INT64    | ヒット番号（セッション内での順序）               |
| `hits.time`          | INT64    | ヒット発生時刻（セッション開始からの経過ミリ秒） |
| `hits.hour`          | INT64    | ヒット発生時刻（時）                             |
| `hits.minute`        | INT64    | ヒット発生時刻（分）                             |
| `hits.isSecure`      | BOOL     | HTTPS かどうか                                   |
| `hits.isInteraction` | BOOL     | インタラクションヒットかどうか                   |
| `hits.isEntrance`    | BOOL     | エントランスページかどうか                       |
| `hits.isExit`        | BOOL     | 離脱ページかどうか（セッションの最後のページ）   |
| `hits.referer`       | STRING   | リファラー URL                                   |
| `hits.type`          | STRING   | ヒットタイプ（PAGE、EVENT、TRANSACTION など）    |

#### hits.page（ページ情報）

| カラム名                   | データ型 | 説明                           |
| -------------------------- | -------- | ------------------------------ |
| `hits.page.pagePath`       | STRING   | ページパス（URL のパス部分）   |
| `hits.page.hostname`       | STRING   | ホスト名                       |
| `hits.page.pageTitle`      | STRING   | ページタイトル                 |
| `hits.page.searchKeyword`  | STRING   | 検索キーワード（サイト内検索） |
| `hits.page.searchCategory` | STRING   | 検索カテゴリ                   |
| `hits.page.pagePathLevel1` | STRING   | ページパスレベル 1             |
| `hits.page.pagePathLevel2` | STRING   | ページパスレベル 2             |
| `hits.page.pagePathLevel3` | STRING   | ページパスレベル 3             |
| `hits.page.pagePathLevel4` | STRING   | ページパスレベル 4             |

#### hits.eCommerceAction（e コマースアクション）

| カラム名                           | データ型 | 説明                                                                                                                                          |
| ---------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `hits.eCommerceAction.action_type` | STRING   | アクションタイプ（'0': 不明、'1': クリック、'2': 商品詳細表示、'3': カート追加、'4': チェックアウト開始、'5': チェックアウト、'6': 購入完了） |
| `hits.eCommerceAction.step`        | INT64    | チェックアウトステップ番号                                                                                                                    |
| `hits.eCommerceAction.option`      | STRING   | アクションオプション                                                                                                                          |

#### hits.product（商品配列）

`hits.product`は配列型で、1 ヒット内に複数の商品情報が格納されています。`UNNEST(hits.product)`で展開して使用します。

| カラム名                                | データ型 | 説明                                           |
| --------------------------------------- | -------- | ---------------------------------------------- |
| `hits.product.productSKU`               | STRING   | 商品 SKU                                       |
| `hits.product.v2ProductName`            | STRING   | 商品名（バージョン 2）                         |
| `hits.product.v2ProductCategory`        | STRING   | 商品カテゴリ                                   |
| `hits.product.productVariant`           | STRING   | 商品バリアント                                 |
| `hits.product.productBrand`             | STRING   | 商品ブランド                                   |
| `hits.product.productRevenue`           | INT64    | 商品売上（マイクロ単位、100 万で割る必要あり） |
| `hits.product.localProductRevenue`      | INT64    | ローカル商品売上                               |
| `hits.product.productPrice`             | INT64    | 商品価格（マイクロ単位）                       |
| `hits.product.localProductPrice`        | INT64    | ローカル商品価格                               |
| `hits.product.productQuantity`          | INT64    | 商品数量                                       |
| `hits.product.productRefundAmount`      | INT64    | 返金金額                                       |
| `hits.product.localProductRefundAmount` | INT64    | ローカル返金金額                               |
| `hits.product.isImpression`             | BOOL     | インプレッションかどうか                       |
| `hits.product.isClick`                  | BOOL     | クリックかどうか                               |
| `hits.product.productListName`          | STRING   | 商品リスト名                                   |
| `hits.product.productListPosition`      | INT64    | 商品リスト内の位置                             |

#### hits.transaction（取引情報）

| カラム名                                   | データ型 | 説明                     |
| ------------------------------------------ | -------- | ------------------------ |
| `hits.transaction.transactionId`           | STRING   | 取引 ID                  |
| `hits.transaction.transactionRevenue`      | INT64    | 取引収益（マイクロ単位） |
| `hits.transaction.transactionTax`          | INT64    | 取引税額                 |
| `hits.transaction.transactionShipping`     | INT64    | 送料                     |
| `hits.transaction.affiliation`             | STRING   | アフィリエイト           |
| `hits.transaction.currencyCode`            | STRING   | 通貨コード               |
| `hits.transaction.localTransactionRevenue` | INT64    | ローカル取引収益         |
| `hits.transaction.transactionCoupon`       | STRING   | クーポンコード           |

#### hits.item（アイテム情報）

| カラム名                     | データ型 | 説明                 |
| ---------------------------- | -------- | -------------------- |
| `hits.item.transactionId`    | STRING   | 取引 ID              |
| `hits.item.productName`      | STRING   | 商品名               |
| `hits.item.productCategory`  | STRING   | 商品カテゴリ         |
| `hits.item.productSku`       | STRING   | 商品 SKU             |
| `hits.item.itemQuantity`     | INT64    | アイテム数量         |
| `hits.item.itemRevenue`      | INT64    | アイテム収益         |
| `hits.item.currencyCode`     | STRING   | 通貨コード           |
| `hits.item.localItemRevenue` | INT64    | ローカルアイテム収益 |

#### hits.eventInfo（イベント情報）

| カラム名                       | データ型 | 説明               |
| ------------------------------ | -------- | ------------------ |
| `hits.eventInfo.eventCategory` | STRING   | イベントカテゴリ   |
| `hits.eventInfo.eventAction`   | STRING   | イベントアクション |
| `hits.eventInfo.eventLabel`    | STRING   | イベントラベル     |
| `hits.eventInfo.eventValue`    | INT64    | イベント値         |

#### hits.social（ソーシャル情報）

| カラム名                                     | データ型 | 説明                                             |
| -------------------------------------------- | -------- | ------------------------------------------------ |
| `hits.social.socialInteractionNetwork`       | STRING   | ソーシャルインタラクションネットワーク           |
| `hits.social.socialInteractionAction`        | STRING   | ソーシャルインタラクションアクション             |
| `hits.social.socialInteractions`             | INT64    | ソーシャルインタラクション数                     |
| `hits.social.socialInteractionTarget`        | STRING   | ソーシャルインタラクションターゲット             |
| `hits.social.socialNetwork`                  | STRING   | ソーシャルネットワーク                           |
| `hits.social.uniqueSocialInteractions`       | INT64    | ユニークソーシャルインタラクション数             |
| `hits.social.hasSocialSourceReferral`        | STRING   | ソーシャルソースリファラルがあるか               |
| `hits.social.socialInteractionNetworkAction` | STRING   | ソーシャルインタラクションネットワークアクション |

### customDimensions（カスタムディメンション配列）

| カラム名                   | データ型 | 説明                               |
| -------------------------- | -------- | ---------------------------------- |
| `customDimensions[].index` | INT64    | カスタムディメンションインデックス |
| `customDimensions[].value` | STRING   | カスタムディメンション値           |

### 注意事項

- **金額の単位**: `totals.transactionRevenue`、`totals.totalTransactionRevenue`、`hits.product.productRevenue`などはマイクロ単位（1,000,000 倍）で格納されています。実際の金額を取得するには 100 万で割る必要があります。
- **配列の展開**: `hits`や`hits.product`は配列型のため、`UNNEST()`関数を使用して展開する必要があります。
- **日付の変換**: `date`カラムは`YYYYMMDD`形式の文字列です。日付型に変換する場合は`PARSE_DATE('%Y%m%d', date)`を使用します。
- **NULL 値の扱い**: 多くのカラムは NULL の可能性があるため、`IFNULL()`や`SAFE_DIVIDE()`などの関数を使用して安全に処理する必要があります。
