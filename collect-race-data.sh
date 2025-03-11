#!/bin/bash

# 競馬レースデータと馬情報を効率的に収集するスクリプト
# 注意: サーバーに負荷をかけないよう十分な間隔を設けています

# デフォルト値
TARGET_YEAR=$(date +"%Y")  # デフォルトは現在の年
MAX_RACES=300
BATCH_SIZE=2
PAUSE_SECONDS=5
KEEP_INTERMEDIATE=1        # 保持する中間結果ファイル数（各タイプごと）
CLEANUP_INTERMEDIATE=true  # 最終結果が得られた後に中間ファイルを削除するか

# 使い方の表示
function show_usage {
    echo "使用方法: $0 [オプション]"
    echo ""
    echo "オプション:"
    echo "  -y, --year YEAR       対象年を指定 (デフォルト: 現在の年)"
    echo "  -m, --max NUM         1クエリあたりの最大取得レース数 (デフォルト: $MAX_RACES)"
    echo "  -b, --batch NUM       バッチサイズ (デフォルト: $BATCH_SIZE)"
    echo "  -p, --pause SECONDS   バッチ間の待機時間（秒） (デフォルト: $PAUSE_SECONDS)"
    echo "  -k, --keep NUM        保持する中間結果ファイル数 (デフォルト: $KEEP_INTERMEDIATE)"
    echo "      --keep-all        すべての中間結果ファイルを保持"
    echo "      --no-cleanup      最終結果後も中間ファイルを削除しない"
    echo "  -h, --help            このヘルプを表示"
    echo ""
    echo "例: $0 --year 2023 --max 500 --batch 3 --pause 45 --keep 2"
    exit 1
}

# コマンドライン引数の解析
while [[ $# -gt 0 ]]; do
    case $1 in
        -y|--year)
            TARGET_YEAR="$2"
            shift 2
            ;;
        -m|--max)
            MAX_RACES="$2"
            shift 2
            ;;
        -b|--batch)
            BATCH_SIZE="$2"
            shift 2
            ;;
        -p|--pause)
            PAUSE_SECONDS="$2"
            shift 2
            ;;
        -k|--keep)
            KEEP_INTERMEDIATE="$2"
            shift 2
            ;;
        --keep-all)
            KEEP_INTERMEDIATE=9999
            shift
            ;;
        --no-cleanup)
            CLEANUP_INTERMEDIATE=false
            shift
            ;;
        -h|--help)
            show_usage
            ;;
        *)
            echo "エラー: 不明なオプション: $1"
            show_usage
            ;;
    esac
done

# 入力値の検証
if ! [[ "$TARGET_YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "エラー: 年は4桁の数字で指定してください。例: 2024"
    exit 1
fi

if ! [[ "$MAX_RACES" =~ ^[0-9]+$ ]]; then
    echo "エラー: 最大レース数は正の整数で指定してください。"
    exit 1
fi

if ! [[ "$BATCH_SIZE" =~ ^[0-9]+$ ]] || [[ "$BATCH_SIZE" -lt 1 ]]; then
    echo "エラー: バッチサイズは1以上の整数で指定してください。"
    exit 1
fi

if ! [[ "$PAUSE_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "エラー: 待機時間は秒単位の正の整数で指定してください。"
    exit 1
fi

if ! [[ "$KEEP_INTERMEDIATE" =~ ^[0-9]+$ ]] || [[ "$KEEP_INTERMEDIATE" -lt 0 ]]; then
    echo "エラー: 保持する中間ファイル数は0以上の整数で指定してください。"
    exit 1
fi

# 出力ディレクトリの設定
OUTPUT_DIR="keiba_data"

# 出力ディレクトリを作成
mkdir -p scraping_logs
mkdir -p keiba_data
mkdir -p horse_data
mkdir -p keiba_data/debug_html
mkdir -p horse_data/debug_html

# 中間ファイルを管理する関数
cleanup_intermediate_files() {
    local pattern=$1
    local keep=$2
    local dir=$3
    
    # パターンに合致するファイルを古い順にリスト
    files=($(ls -t ${dir}/${pattern} 2>/dev/null))
    
    # 保持するファイル数を超える古いファイルを削除
    if [ ${#files[@]} -gt $keep ]; then
        for ((i=$keep; i<${#files[@]}; i++)); do
            echo "古い中間ファイルを削除: ${files[$i]}"
            rm "${files[$i]}"
        done
    fi
}

# 現在の日時を取得（ファイル名に使用）
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "======================================================================="
echo "  ${TARGET_YEAR}年競馬データ収集タスク - 開始: $(date)"
echo "======================================================================="
echo "  対象年: ${TARGET_YEAR}年"
echo "  最大レース数: $MAX_RACES"
echo "  バッチサイズ: $BATCH_SIZE"
echo "  バッチ間待機時間: $PAUSE_SECONDS秒"
echo "======================================================================="

# ファイル名の確認と表示
RACE_SCRAPER="direct-race-scraper.py"
HORSE_SCRAPER="fixed-horse-scraper.py"

if [ ! -f "$RACE_SCRAPER" ]; then
    echo "エラー: $RACE_SCRAPER が見つかりません。"
    echo "スクリプトと同じディレクトリに配置されていることを確認してください。"
    exit 1
fi

if [ ! -f "$HORSE_SCRAPER" ]; then
    echo "エラー: $HORSE_SCRAPER が見つかりません。"
    echo "スクリプトと同じディレクトリに配置されていることを確認してください。"
    exit 1
fi

echo "使用するスクリプト:"
echo "- レーススクレイパー: $RACE_SCRAPER"
echo "- 馬情報スクレイパー: $HORSE_SCRAPER"

# 一括でその年のすべてのレースデータを収集
echo "[TASK 1] ${TARGET_YEAR}年のレースデータを収集 (開始: $(date))"
python $RACE_SCRAPER --year $TARGET_YEAR --batch_size $BATCH_SIZE --pause $PAUSE_SECONDS --max_races $MAX_RACES > scraping_logs/races_${TIMESTAMP}.log 2>&1

# 中間ファイルのクリーンアップ
cleanup_intermediate_files "intermediate_races_*" $KEEP_INTERMEDIATE "$OUTPUT_DIR"
cleanup_intermediate_files "intermediate_race_infos_*" $KEEP_INTERMEDIATE "$OUTPUT_DIR"

echo "レースデータ収集完了 - 30分休止します ($(date))"
sleep 1800  # 30分休止

# 最新の馬IDファイルを取得
LATEST_HORSE_IDS=$(ls -t keiba_data/horse_ids_*.json 2>/dev/null | head -1)

# 馬情報の収集
if [ -z "$LATEST_HORSE_IDS" ]; then
    echo "警告: 馬IDファイルが見つかりません。人気馬・活躍馬の情報を収集します。"
    # 代替収集方法を使用
    echo "[TASK 2] 人気馬・活躍馬の情報を収集 (開始: $(date))"
    python $HORSE_SCRAPER --source recent --years $TARGET_YEAR $(($TARGET_YEAR-1)) --batch_size $BATCH_SIZE --pause $PAUSE_SECONDS --limit $MAX_RACES > scraping_logs/active_horses_${TIMESTAMP}.log 2>&1
else
    echo "馬情報の収集に使用するファイル: $LATEST_HORSE_IDS"

    # 取得したレースに出場した馬の詳細情報を収集
    echo "[TASK 2] 出場馬の詳細情報を収集 (開始: $(date))"
    python $HORSE_SCRAPER --source file --file "$LATEST_HORSE_IDS" --batch_size $BATCH_SIZE --pause $PAUSE_SECONDS --limit $MAX_RACES > scraping_logs/race_horses_${TIMESTAMP}.log 2>&1
fi

echo "馬データ収集完了 ($(date))"

# 重要レース（GI・GIIなど）の収集
echo "[TASK 3] 重要レースの追加確認 (開始: $(date))"

# 主要競馬場で開催される可能性が高い重要レースを対象に追加収集
echo "主要競馬場のレースデータ確認 (開始: $(date))"
python $RACE_SCRAPER --year $TARGET_YEAR --places 05 06 08 09 --batch_size $BATCH_SIZE --pause $PAUSE_SECONDS --max_races $(($MAX_RACES/2)) > scraping_logs/major_tracks_races_${TIMESTAMP}.log 2>&1

# データ収集の結果サマリーを表示
echo "======================================================================="
echo "  収集データサマリー ($(date))"
echo "======================================================================="

# レースデータファイルの統計
echo "レースデータファイル:"
race_files=$(ls -lh keiba_data/races_*.csv 2>/dev/null)
if [ -n "$race_files" ]; then
  echo "$race_files"
  total_races=0
  for csv_file in $(ls keiba_data/races_*.csv 2>/dev/null); do
    lines=$(wc -l < $csv_file)
    lines=$((lines - 1)) # ヘッダー行を除く
    echo "- $csv_file: $lines レース"
    total_races=$((total_races + lines))
  done
  echo "総レース数: $total_races"
else
  echo "  レースCSVファイルがありません"
fi

# 馬情報データファイルの統計
echo "馬情報データファイル:"
horse_files=$(ls -lh horse_data/horse_info_*.csv 2>/dev/null)
if [ -n "$horse_files" ]; then
  echo "$horse_files"
  total_horses=0
  for csv_file in $(ls horse_data/horse_info_*.csv 2>/dev/null); do
    lines=$(wc -l < $csv_file)
    lines=$((lines - 1)) # ヘッダー行を除く
    echo "- $csv_file: $lines 頭"
    total_horses=$((total_horses + lines))
  done
  echo "総馬数: $total_horses"
else
  echo "  馬情報CSVファイルがありません"
fi

# すべての処理が終了したら、設定に応じて中間ファイルを完全にクリーンアップ
if [ "$CLEANUP_INTERMEDIATE" = true ]; then
    echo "中間ファイルの最終クリーンアップを実行します..."
    
    # 中間ファイルを全て削除
    cleanup_intermediate_files "intermediate_races_*" 0 "$OUTPUT_DIR"
    cleanup_intermediate_files "intermediate_race_infos_*" 0 "$OUTPUT_DIR"
    cleanup_intermediate_files "intermediate_horse_info_*" 0 "horse_data"
    cleanup_intermediate_files "intermediate_horse_history_*" 0 "horse_data"
    cleanup_intermediate_files "intermediate_horse_training_*" 0 "horse_data"
    
    echo "中間ファイルのクリーンアップが完了しました。"
fi

echo "======================================================================="
echo "  ${TARGET_YEAR}年競馬データ収集タスク - 完了: $(date)"
echo "======================================================================="
