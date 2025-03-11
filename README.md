# 競馬データ収集スクリプト

競馬のレース結果や馬の情報を効率的に収集するスクリプト群です。

## 含まれるスクリプト

- **direct-race-scraper.py**: レース結果をスクレイピングするスクリプト
- **fixed-horse-scraper.py**: 馬の情報をスクレイピングするスクリプト
- **collect-race-data.sh**: 1年分のデータを競馬場ごとに収集するシェルスクリプト

## 使い方

### レースデータの収集

```bash
# 通常モードでレースデータを収集する場合
python direct-race-scraper.py --year 2023 --places 01 05 09 --batch_size 3 --pause 45 --max_races 500

# 効率的モードでレースデータを収集する場合
python direct-race-scraper.py --year 2023 --places 01 05 09 --batch_size 3 --pause 45 --max_races 500 --efficient
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
nohup bash ./collect-race-data.sh --year 2020 --max 2000 --batch 3 &
```

### シェルスクリプトのオプション一覧

| オプション | 説明 |
|------------|------|
| `-y, --year YEAR` | 対象年を指定（デフォルト: 現在の年） |
| `-m, --max NUM` | 競馬場ごとの最大取得レース数（デフォルト: 300） |
| `-b, --batch NUM` | バッチサイズ（デフォルト: 2） |
| `-p, --pause SECONDS` | バッチ間の待機時間（秒）（デフォルト: 5） |
| `-k, --keep NUM` | 保持する中間結果ファイル数（デフォルト: 1） |
| `--keep-all` | すべての中間結果ファイルを保持 |
| `--no-cleanup` | 最終結果後も中間ファイルを削除しない |
| `--no-horses` | 馬情報の収集を行わない |
| `--horses-only` | レースデータをスキップし、馬情報のみを収集 |
| `--no-skip-horses` | 既存の馬情報もスキップせずに再取得する |
| `--collect-all-horses` | 既存の馬情報も含めてすべて取得する（`--no-skip-horses`の別名） |

## 効率的なデータ収集の仕組み

スクリプトには効率的なレース収集ロジックが実装されており、不要なリクエストを最小限に抑えます：

1. **レースIDの構造を利用した最適化**
   - レースID = `[年][競馬場][開催回][開催日][レース番号]`
   - 例: `202001010101` = 2020年・札幌(01)・第1回・1日目・1R

2. **スキップロジック**
   - あるレース（例：2R）が存在しなければ、同じ日の後続レース（3R以降）をスキップ
   - ある開催日（例：2日目）の1Rが存在しなければ、次の開催日（3日目）に進む
   - ある開催回のすべての開催日で1Rが存在しなければ、次の開催回に進む

3. **既存データのスキップ**
   - 既存の馬情報ファイル（`horse_data/horse_info_*.csv`）から馬IDを抽出
   - 既に情報が取得済みの馬はスキップすることで、重複収集を防止
   - 完全再取得も`--no-skip-horses`オプションで選択可能

4. **競馬場ごとの個別処理**
   - 各競馬場（01-10）ごとに個別にデータを収集
   - すべての競馬場のデータを漏れなく取得

## 特徴

- 天候、馬場状態などの詳細情報を含む
- サーバー負荷を考慮したバッチ処理機能
- 中間結果の保存による途中経過の保全
- 馬情報の一括収集
- 効率的なスキップロジックによる処理時間の短縮（約70-80%のリクエスト削減）
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

## 注意事項

- サーバー負荷を考慮して `--pause` パラメータで適切な間隔を設定してください
- 長時間の実行が必要な場合は `nohup` コマンドでバックグラウンド実行をお勧めします
- ログファイルは `direct_race_scraping.log`、`horse_scraping.log` と `scraping_logs/` ディレクトリに保存されます
- 大量のデータを収集する場合は、`--max`パラメータを2000程度に設定することをお勧めします
- デフォルトでは既存の馬情報はスキップされます。すべての馬を再取得したい場合は `--no-skip-horses` オプションを使用してください
