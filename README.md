# 競馬データ収集スクリプト

競馬のレース結果や馬の情報を効率的に収集するスクリプト群です。

## 含まれるスクリプト

- **direct-race-scraper.py**: レース結果をスクレイピングするスクリプト
- **fixed-horse-scraper.py**: 馬の情報をスクレイピングするスクリプト
- **collect-race-data.sh**: 1年分のデータを四半期ごとに収集するシェルスクリプト

## 使い方

```bash
# レースデータのみを収集する場合
python direct-race-scraper.py --year 2023 --months 1 2 3 --batch_size 3 --pause 45 --max_races 100

# 馬データのみを収集する場合
python fixed-horse-scraper.py --source file --file "keiba_data/horse_ids_2023_all_20230101_120000.json" --batch_size 3 --pause 45 --limit 100

# 年間データの自動収集
./collect-race-data.sh --year 2023 --max 500 --batch 3 --pause 45
```

## 特徴

- 天候、馬場状態などの詳細情報を含む
- サーバー負荷を考慮したバッチ処理機能
- 中間結果の保存による途中経過の保全
- 馬情報の一括収集
