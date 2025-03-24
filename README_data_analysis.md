# 競馬データ分析スクリプト

keiba_scriptで収集したデータを分析・前処理し、競馬AI開発の準備を行うためのスクリプト群です。

## 概要

このリポジトリには、以下の3つの主要なスクリプトが含まれています：

1. **data_preparation.py**: データのクリーニングと前処理を行います
2. **exploratory_analysis.py**: 探索的データ分析を実行します
3. **feature_engineering.py**: 機械学習のための特徴量エンジニアリングを行います

これらのスクリプトは、keiba_scriptで収集した2020年から2025年までの競馬データを対象に、AIモデル開発の前段階として必要なデータ準備・分析を行うために設計されています。

## 前提条件

以下のPythonライブラリが必要です：

```bash
pip install pandas numpy matplotlib seaborn scikit-learn
```

また、`keiba_script`で収集した以下のファイルが必要です：

- `keiba_data/races_*.csv` - レース結果データ
- `horse_data/horse_info_*.csv` - 馬の基本情報
- `horse_data/horse_history_*.csv` - 馬の出走履歴
- `horse_data/horse_training_*.csv` - 馬の調教データ（オプション）

## 使用方法

### 1. データ準備と前処理

```bash
python data_preparation.py
```

このスクリプトは以下の処理を行います：

- 複数のCSVファイルの統合
- 重複データの削除
- 欠損値の処理と型変換
- 外れ値の検出と処理

処理されたデータは `preprocessed_data` ディレクトリに保存されます。

### 2. 探索的データ分析

```bash
python exploratory_analysis.py
```

このスクリプトは以下の分析を行います：

- レースデータの基本統計分析
- 馬のパフォーマンス分析
- 騎手・調教師の分析
- 馬場・天候・季節の影響分析

分析結果とグラフは `analysis_results` ディレクトリに保存されます。

### 3. 特徴量エンジニアリング

```bash
python feature_engineering.py
```

このスクリプトは機械学習モデルのための特徴量を作成します：

- 時系列特徴量（過去の成績、休養期間など）
- 騎手の特徴量（過去の勝率など）
- ペース・上がりタイムの特徴量
- 血統と馬場適性の特徴量

作成された特徴量は `feature_data` ディレクトリに保存され、モデリングに使用できる形式のデータセットも生成されます。

## 出力ファイル

### データ準備

- `preprocessed_data/cleaned_races_[タイムスタンプ].csv` - クリーニング済みレースデータ
- `preprocessed_data/cleaned_horse_info_[タイムスタンプ].csv` - クリーニング済み馬情報
- `preprocessed_data/cleaned_horse_history_[タイムスタンプ].csv` - クリーニング済み出走履歴

### 探索的データ分析

- `analysis_results/yearly_races.png` - 年ごとのレース数グラフ
- `analysis_results/track_condition_times.png` - 馬場状態別の平均タイムグラフ
- `analysis_results/top_win_rate_horses.png` - 勝率上位の馬グラフ
- `analysis_results/top_jockeys.png` - 勝率上位の騎手グラフ
- 他、各種統計情報のCSVファイル

### 特徴量エンジニアリング

- `feature_data/race_features_[タイムスタンプ].csv` - レース特徴量
- `feature_data/horse_features_[タイムスタンプ].csv` - 馬特徴量
- `feature_data/modeling_dataset_[タイムスタンプ].csv` - モデリング用統合データセット

## 特徴量の説明

モデリング用データセットには以下のような特徴量が含まれています：

### 馬の過去成績に関する特徴量

- `過去3走着順平均`: 直近3レースの平均着順
- `累積勝率`: その時点までの勝率
- `累積複勝率`: その時点までの複勝率（3着以内に入る率）
- `休養日数`: 前走からの休養日数
- `同コース距離_過去平均着順`: 同一コース・距離での過去の平均着順
- `同コース距離_勝率`: 同一コース・距離での過去の勝率

### 騎手に関する特徴量

- `騎手_直近成績_勝率`: 騎手の直近90日間の勝率
- `騎手_同コース_勝率`: 騎手の同一コースでの勝率

### ペース・タイムに関する特徴量

- `first_position`: 最初のコーナーでの位置
- `position_change`: レース中の順位変動
- `early_pace`: 序盤のペース
- `middle_pace`: 中盤のペース
- `late_pace`: 終盤のペース
- `上がりタイム_相対`: レース平均との上がりタイム差

### 血統に関する特徴量

- `父系_*`: 主要な父系統のダミー変数
- `母父系_*`: 主要な母父系統のダミー変数
- `馬場適性_*`: 各馬場状態での適性スコア

## 開発ロードマップ

このデータ準備・分析は、競馬AI開発ロードマップのPhase 1（データ準備と分析）を構成しています。今後のPhaseでは以下を予定しています：

- **Phase 2**: モデル開発
- **Phase 3**: システム統合と展開
- **Phase 4**: 検証とテスト
- **Phase 5**: 改善と拡張

## 注意事項

- 大量のデータを扱うため、十分なメモリを搭載したマシンでの実行を推奨します
- 特徴量エンジニアリングは計算コストが高いため、実行に時間がかかることがあります
- モデルの学習・評価用にデータを分割する際は、時系列データであることを考慮し、時間的にトレーニングデータとテストデータを分けるようにしてください
