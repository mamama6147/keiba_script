# 競馬データ収集スクリプト

競馬のレース結果や馬の情報を効率的に収集するスクリプト群です。

## 含まれるスクリプト

- **direct-race-scraper.py**: レース結果をスクレイピングするスクリプト
- **fixed-horse-scraper.py**: 馬の情報をスクレイピングするスクリプト
- **collect-race-data.sh**: 1年分のデータを競馬場ごとに収集するシェルスクリプト

## 使い方

### レースデータの収集

```bash
# 効率的モードでレースデータを収集する場合（推奨）
python direct-race-scraper.py --year 2023 --places 01 05 09 --batch_size 3 --pause 45 --max_races 500 --efficient

# シンプルな使用例
python direct-race-scraper.py --year 2023 --efficient
```

### 馬データの収集

```bash
# 特定の馬IDリストから馬データを収集（既存のデータはスキップ）
python fixed-horse-scraper.py --source file --file "keiba_data/horse_ids_2023_20230101_120000.json" --batch_size 3 --pause 45 --limit 500 --skip-existing

# 特定の馬IDリストから馬データを収集（すべての馬を再取得）
python fixed-horse-scraper.py --source file --file "keiba_data/horse_ids_2023_20230101_120000.json" --batch_size 3 --pause 45 --limit 500
```

### 年間データの自動収集（推奨）

```bash
# 対話モードで実行（レースデータと馬情報の両方を収集、既存の馬情報はスキップ）
./collect-race-data.sh --year 2023 --max 2000 --batch 3 --pause 45

# レースデータのみを収集（馬情報は収集しない）
./collect-race-data.sh --year 2023 --max 2000 --batch 3 --no-horses

# 馬情報のみを収集（レースデータは収集しない、既存の馬情報はスキップ）
./collect-race-data.sh --year 2023 --batch 3 --horses-only

# 馬情報のみを収集（既存の馬情報も含めて再取得）
./collect-race-data.sh --year 2023 --batch 3 --horses-only --no-skip-horses

# バックグラウンドで実行（推奨）
nohup bash ./collect-race-data.sh --year 2023 --max 2000 --batch 3 > nohup-2023.out 2>&1 &
```

### シェルスクリプトのオプション一覧

| オプション | 説明 |
|------------|------|
| `-y, --year YEAR` | 対象年を指定（デフォルト: 現在の年） |
| `-m, --max NUM` | 競馬場ごとの最大取得レース数（デフォルト: 1000） |
| `-b, --batch NUM` | バッチサイズ（デフォルト: 2） |
| `-p, --pause SECONDS` | バッチ間の待機時間（秒）（デフォルト: 2） |
| `-k, --keep NUM` | 保持する中間結果ファイル数（デフォルト: 1） |
| `--keep-all` | すべての中間結果ファイルを保持 |
| `--no-cleanup` | 最終結果後も中間ファイルを削除しない |
| `--no-horses` | 馬情報の収集を行わない |
| `--horses-only` | レースデータをスキップし、馬情報のみを収集 |
| `--no-skip-horses` | 既存の馬情報もスキップせずに再取得する |
| `--collect-all-horses` | 既存の馬情報も含めてすべて取得する（`--no-skip-horses`の別名） |

## 効率的なデータ収集の仕組み

スクリプトには実際の競馬開催ルールに基づいた効率的なレース収集ロジックが実装されており、不要なリクエストを最小限に抑えます：

1. **レースIDの構造を利用した最適化**
   - レースID = `[年][競馬場][開催回][開催日][レース番号]`
   - 例: `202001010101` = 2020年・札幌(01)・第1回・1日目・1R

2. **競馬の開催ルールに基づくスキップロジック**
   - **開催回の連続性**: 5回目が無い場合、6回目も無いとみなす（例：5回目が無いのに6回目があることはない）
   - **レースの連続性**: 4Rが無い場合、5R以降も無いとみなす（例：4Rが無いのに5Rがあることはない）
   - **開催日の独立性**: 各開催日は開催される場合もされない場合もある（例：3日目が無くても4日目はある場合がある）

3. **階層的チェック処理**
   - 競馬場 → 開催回 → 開催日 → レース番号の順で階層的に検索
   - 各レベルで存在チェックを行い、効率的にデータを収集
   - リクエスト回数を大幅に削減（約80-90%のリクエスト削減）

4. **検索プロセス**
   - 開催1日目の1Rをチェック（開催回の存在確認）
     - 存在しない → その競馬場の残りの開催回もスキップ
     - 存在する → その日の全レースを処理
   - 各開催日の1Rをチェック
     - 存在しない → 次の開催日へ（開催日はスキップされることがある）
     - 存在する → その日の全レースを処理
   - 各レースを順次チェック
     - あるレースが存在しない → その日の残りのレースもスキップ（レースは連続的に存在する）

## 特徴

- **新機能: 競馬開催ルールに基づく最適化処理**
- **新機能: リアルタイムコンソールログ出力**
- **新機能: 自動ループ終了判定**（不要な検索を自動スキップ）
- 天候、馬場状態などの詳細情報を含む
- サーバー負荷を考慮したバッチ処理機能
- 中間結果の保存による途中経過の保全
- 馬情報の一括収集
- 収集したCSVファイルの自動結合
- 馬IDの集約処理
- レースデータと馬情報の収集プロセスを個別に制御可能
- 既存データの重複収集防止機能

## 出力ファイル

スクリプト実行後、以下のファイルが生成されます：

- `keiba_data/races_[年]_[タイムスタンプ].csv` - 収集したすべてのレースデータ
- `keiba_data/horse_ids_[年]_[タイムスタンプ].json` - 収集したすべての馬ID
- `horse_data/horse_info_[タイムスタンプ].csv` - 収集した馬の情報
- `horse_data/horse_info_[タイムスタンプ].json` - 収集した馬の情報（JSON形式、血統情報など詳細データを含む）
- `horse_data/horse_history_[タイムスタンプ].csv` - 馬の出走履歴
- `horse_data/horse_training_[タイムスタンプ].csv` - 馬の調教データ（--include_training オプション使用時）

## デバッグとログ

- リアルタイムでログが標準出力（コンソール）に表示されるようになりました
- 全てのログは `direct_race_scraping.log` にも保存されます
- 馬情報収集ログは `horse_scraping.log` と `scraping_logs/` ディレクトリに保存されます
- バックグラウンド実行時は `nohup-[年].out` にログが保存されます

## 注意事項

- サーバー負荷を考慮して `--pause` パラメータで適切な間隔を設定してください
- 長時間の実行が必要な場合は `nohup` コマンドでバックグラウンド実行をお勧めします
- 大量のデータを収集する場合は、`--max`パラメータを2000程度に設定することをお勧めします
- デフォルトでは既存の馬情報はスキップされます。すべての馬を再取得したい場合は `--no-skip-horses` オプションを使用してください

## 更新履歴

- **2025-03-12**: 競馬開催ルールに基づく効率的なレース検索アルゴリズムを実装
  - 開催回・開催日・レース番号の階層的な存在チェックを追加
  - ある開催回が存在しなければ、それ以降の開催回も存在しないと判断
  - あるレースが存在しなければ、それ以降のレースも存在しないと判断
  - リアルタイムログ出力を追加
