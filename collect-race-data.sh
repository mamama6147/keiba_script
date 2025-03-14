#!/bin/bash

# 競馬レースデータと馬情報を効率的に収集するスクリプト
# 注意: サーバーに負荷をかけないよう十分な間隔を設けています

# デフォルト値
TARGET_YEAR=$(date +"%Y")  # デフォルトは現在の年
MAX_RACES=1000
BATCH_SIZE=2
PAUSE_SECONDS=2
KEEP_INTERMEDIATE=1        # 保持する中間結果ファイル数（各タイプごと）
CLEANUP_INTERMEDIATE=true  # 最終結果が得られた後に中間ファイルを削除するか
COLLECT_HORSES=true        # 馬情報を収集するかどうか
SKIP_EXISTING_HORSES=true  # 既存の馬情報をスキップするかどうか
RESET_PROGRESS=false       # 進捗ファイルをリセットするか
ORGANIZE_FILES=true        # 処理完了後にファイルを年別フォルダに整理するか

# 使い方の表示
function show_usage {
    echo "使用方法: $0 [オプション]"
    echo ""
    echo "オプション:"
    echo "  -y, --year YEAR       対象年を指定 (デフォルト: 現在の年)"
    echo "  -m, --max NUM         競馬場ごとの最大取得レース数 (デフォルト: $MAX_RACES)"
    echo "  -b, --batch NUM       バッチサイズ (デフォルト: $BATCH_SIZE)"
    echo "  -p, --pause SECONDS   バッチ間の待機時間（秒） (デフォルト: $PAUSE_SECONDS)"
    echo "  -k, --keep NUM        保持する中間結果ファイル数 (デフォルト: $KEEP_INTERMEDIATE)"
    echo "      --keep-all        すべての中間結果ファイルを保持"
    echo "      --no-cleanup      最終結果後も中間ファイルを削除しない"
    echo "      --no-horses       馬情報の収集を行わない"
    echo "      --horses-only     レースデータをスキップし、馬情報のみを収集"
    echo "      --no-skip-horses  既存の馬情報もスキップせずに再取得する"
    echo "      --collect-all-horses 既存の馬情報も含めてすべて取得する (--no-skip-horsesの別名)"
    echo "      --reset-progress  進捗ファイルをリセットして対象競馬場のデータを再収集"
    echo "      --no-organize     処理完了後にファイルを年別フォルダに整理しない"
    echo "  -h, --help            このヘルプを表示"
    echo ""
    echo "例: $0 --year 2023 --max 500 --batch 3 --pause 45 --keep 2"
    echo "例: $0 --year 2023 --max 500 --no-horses"
    echo "例: $0 --year 2023 --horses-only --no-skip-horses"
    echo "例: $0 --year 2023 --reset-progress"
    exit 1
}

# レースデータ収集フラグ（デフォルトはtrue）
COLLECT_RACES=true

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
        --no-horses)
            COLLECT_HORSES=false
            shift
            ;;
        --horses-only)
            COLLECT_RACES=false
            COLLECT_HORSES=true
            shift
            ;;
        --no-skip-horses|--collect-all-horses)
            SKIP_EXISTING_HORSES=false
            shift
            ;;
        --reset-progress)
            RESET_PROGRESS=true
            shift
            ;;
        --no-organize)
            ORGANIZE_FILES=false
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

# ファイル整理関数：指定年のファイルをkeiba_data内の年別ディレクトリに移動
organize_files_by_year() {
    local year=$1
    local year_dir="${OUTPUT_DIR}/${year}"
    
    echo "======================================================================="
    echo "  ${year}年のレースデータファイルを整理しています..."
    echo "======================================================================="
    
    # keiba_data内に年別ディレクトリを作成
    mkdir -p "${year_dir}"
    mkdir -p "${year_dir}/debug_html"
    
    # レースデータ関連ファイルの移動
    echo "レースデータ関連ファイルを移動中..."
    
    # レースCSVファイル
    race_csv_files=$(find ${OUTPUT_DIR} -maxdepth 1 -name "races_${year}_*.csv" -type f)
    for file in $race_csv_files; do
        mv "$file" "${year_dir}/"
        echo "移動: $file -> ${year_dir}/"
    done
    
    # レース情報JSONファイル
    info_json_files=$(find ${OUTPUT_DIR} -maxdepth 1 -name "race_infos_${year}_*.json" -type f)
    for file in $info_json_files; do
        mv "$file" "${year_dir}/"
        echo "移動: $file -> ${year_dir}/"
    done
    
    # 馬IDJSONファイル
    horse_id_files=$(find ${OUTPUT_DIR} -maxdepth 1 -name "horse_ids_${year}_*.json" -type f)
    for file in $horse_id_files; do
        mv "$file" "${year_dir}/"
        echo "移動: $file -> ${year_dir}/"
    done
    
    # デバッグHTMLファイルの移動（特定の年に属するファイルのみ）
    echo "デバッグファイルを移動中..."
    debug_files=$(find ${OUTPUT_DIR}/debug_html -name "race_${year}*.html" -type f)
    for file in $debug_files; do
        filename=$(basename "$file")
        mv "$file" "${year_dir}/debug_html/"
        echo "移動: $file -> ${year_dir}/debug_html/"
    done
    
    # 進捗ファイルをコピー（バックアップとして）
    if [ -f "${OUTPUT_DIR}/race_scraping_progress_${year}.txt" ]; then
        cp "${OUTPUT_DIR}/race_scraping_progress_${year}.txt" "${year_dir}/"
        echo "コピー: ${OUTPUT_DIR}/race_scraping_progress_${year}.txt -> ${year_dir}/"
    fi
    
    # 移動結果の報告
    race_files=$(find ${year_dir} -maxdepth 1 -name "races_*.csv" | wc -l)
    info_files=$(find ${year_dir} -maxdepth 1 -name "race_infos_*.json" | wc -l)
    id_files=$(find ${year_dir} -maxdepth 1 -name "horse_ids_*.json" | wc -l)
    debug_files=$(find ${year_dir}/debug_html -type f | wc -l)
    
    echo "ファイル整理完了："
    echo "- レースデータ: ${race_files} ファイル"
    echo "- レース情報: ${info_files} ファイル"
    echo "- 馬ID: ${id_files} ファイル"
    echo "- デバッグHTML: ${debug_files} ファイル"
    
    echo "======================================================================="
    echo "  ${year}年のデータを ${year_dir}/ ディレクトリに整理しました"
    echo "======================================================================="
}

# 現在の日時を取得（ファイル名に使用）
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "======================================================================="
echo "  ${TARGET_YEAR}年競馬データ収集タスク - 開始: $(date)"
echo "======================================================================="
echo "  対象年: ${TARGET_YEAR}年"
echo "  競馬場ごとの最大レース数: $MAX_RACES"
echo "  バッチサイズ: $BATCH_SIZE"
echo "  バッチ間待機時間: $PAUSE_SECONDS秒"
echo "  レースデータ収集: $([ "$COLLECT_RACES" = true ] && echo "有効" || echo "無効")"
echo "  馬情報収集: $([ "$COLLECT_HORSES" = true ] && echo "有効" || echo "無効")"
echo "  既存馬情報のスキップ: $([ "$SKIP_EXISTING_HORSES" = true ] && echo "有効" || echo "無効")"
echo "  進捗リセット: $([ "$RESET_PROGRESS" = true ] && echo "有効" || echo "無効")"
echo "  ファイル整理: $([ "$ORGANIZE_FILES" = true ] && echo "有効" || echo "無効")"
echo "======================================================================="

# ファイル名の確認と表示
RACE_SCRAPER="direct-race-scraper.py"
HORSE_SCRAPER="fixed-horse-scraper.py"

if [ "$COLLECT_RACES" = true ] && [ ! -f "$RACE_SCRAPER" ]; then
    echo "エラー: $RACE_SCRAPER が見つかりません。"
    echo "スクリプトと同じディレクトリに配置されていることを確認してください。"
    exit 1
fi

if [ "$COLLECT_HORSES" = true ] && [ ! -f "$HORSE_SCRAPER" ]; then
    echo "エラー: $HORSE_SCRAPER が見つかりません。"
    echo "スクリプトと同じディレクトリに配置されていることを確認してください。"
    exit 1
fi

echo "使用するスクリプト:"
if [ "$COLLECT_RACES" = true ]; then
    echo "- レーススクレイパー: $RACE_SCRAPER"
fi
if [ "$COLLECT_HORSES" = true ]; then
    echo "- 馬情報スクレイパー: $HORSE_SCRAPER"
fi

# 各競馬場ごとに個別にレースデータを収集
if [ "$COLLECT_RACES" = true ]; then
    PLACE_CODES=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10")
    PLACE_NAMES=("札幌" "函館" "福島" "新潟" "東京" "中山" "中京" "京都" "阪神" "小倉")

    # 進捗リセットオプションの設定
    RESET_OPTION=""
    if [ "$RESET_PROGRESS" = true ]; then
        RESET_OPTION="--reset_progress"
        echo "注意: 進捗ファイルをリセットして競馬場ごとにデータを再収集します"
    fi

    for i in "${!PLACE_CODES[@]}"; do
        PLACE_CODE=${PLACE_CODES[$i]}
        PLACE_NAME=${PLACE_NAMES[$i]}
        
        echo "[TASK 1-${i}] ${TARGET_YEAR}年 ${PLACE_NAME}(${PLACE_CODE})競馬場のレースデータを収集 (開始: $(date))"
        python $RACE_SCRAPER --year $TARGET_YEAR --places $PLACE_CODE --batch_size $BATCH_SIZE --pause $PAUSE_SECONDS --max_races $MAX_RACES --efficient $RESET_OPTION > scraping_logs/races_${PLACE_CODE}_${TIMESTAMP}.log 2>&1
        
        # 中間ファイルのクリーンアップ
        cleanup_intermediate_files "intermediate_races_*" $KEEP_INTERMEDIATE "$OUTPUT_DIR"
        cleanup_intermediate_files "intermediate_race_infos_*" $KEEP_INTERMEDIATE "$OUTPUT_DIR"
        
        # 各競馬場の収集後、少し休止
        echo "${PLACE_NAME}(${PLACE_CODE})競馬場のデータ収集完了 - 10分休止します ($(date))"
        sleep 600  # 10分休止
    done

    # 収集したCSVファイルを1つに結合
    echo "[TASK 2] 収集したデータファイルを結合します (開始: $(date))"
    FINAL_CSV_FILE="keiba_data/races_${TARGET_YEAR}_${TIMESTAMP}.csv"

    # CSVファイルが存在するか確認
    RACE_CSV_EXISTS=$(ls -t keiba_data/races_*.csv 2>/dev/null | head -1)
    
    if [ -n "$RACE_CSV_EXISTS" ]; then
        # ヘッダー行だけを最初に取得
        head -n 1 "$RACE_CSV_EXISTS" > "$FINAL_CSV_FILE" 

        # 各CSVファイルからヘッダーを除いたデータ行を結合ファイルに追加
        for csv_file in $(ls -t keiba_data/races_*.csv 2>/dev/null); do
            if [ "$csv_file" != "$FINAL_CSV_FILE" ]; then
                tail -n +2 "$csv_file" >> "$FINAL_CSV_FILE"
            fi
        done

        echo "データファイルの結合が完了しました: $FINAL_CSV_FILE"
    else
        echo "警告: 結合対象のレースCSVファイルが見つかりません。"
    fi

    # 収集したCSVファイルから馬IDを抽出して結合
    echo "[TASK 3] すべての馬IDを1つのファイルに集約します"
    FINAL_HORSE_IDS="keiba_data/horse_ids_${TARGET_YEAR}_${TIMESTAMP}.json"

    # 一時的なJSONマージスクリプト
    MERGE_SCRIPT="merge_horse_ids.py"
    cat > $MERGE_SCRIPT << 'EOL'
#!/usr/bin/env python
import json
import glob
import sys

# 結合先のファイル名
output_file = sys.argv[1]

# 全ての馬IDを格納するセット（重複を排除）
all_horse_ids = set()

# 全てのhorse_ids_*.jsonファイルを読み込み
for filename in glob.glob('keiba_data/horse_ids_*.json'):
    if filename == output_file:
        continue
    try:
        with open(filename, 'r') as f:
            horse_ids = json.load(f)
            # リストまたは文字列の馬IDを追加
            if isinstance(horse_ids, list):
                all_horse_ids.update(horse_ids)
            elif isinstance(horse_ids, str):
                all_horse_ids.add(horse_ids)
    except Exception as e:
        print(f"Error reading {filename}: {e}")

# 重複のない馬IDリストをJSONファイルに書き出し
with open(output_file, 'w') as f:
    json.dump(list(all_horse_ids), f)

print(f"Merged {len(all_horse_ids)} unique horse IDs to {output_file}")
EOL

    # スクリプトを実行
    python $MERGE_SCRIPT $FINAL_HORSE_IDS
    rm $MERGE_SCRIPT
fi

# 馬情報の収集
if [ "$COLLECT_HORSES" = true ]; then
    # 馬情報収集のタスク番号を設定
    HORSE_TASK_NUM=$([ "$COLLECT_RACES" = true ] && echo "4" || echo "1")
    
    # 最新の馬IDファイルを取得
    LATEST_HORSE_IDS=$(ls -t keiba_data/horse_ids_*.json 2>/dev/null | head -1)

    # 既存の馬情報をスキップするオプション
    SKIP_OPTION=""
    if [ "$SKIP_EXISTING_HORSES" = true ]; then
        SKIP_OPTION="--skip-existing"
        echo "既存の馬情報はスキップします"
    else
        echo "すべての馬情報を再取得します"
    fi

    if [ -z "$LATEST_HORSE_IDS" ]; then
        echo "警告: 馬IDファイルが見つかりません。人気馬・活躍馬の情報を収集します。"
        # 代替収集方法を使用
        echo "[TASK $HORSE_TASK_NUM] 人気馬・活躍馬の情報を収集 (開始: $(date))"
        python $HORSE_SCRAPER --source recent --years $TARGET_YEAR $(($TARGET_YEAR-1)) --batch_size $BATCH_SIZE --pause $PAUSE_SECONDS --limit $MAX_RACES $SKIP_OPTION > scraping_logs/active_horses_${TIMESTAMP}.log 2>&1
    else
        echo "馬情報の収集に使用するファイル: $LATEST_HORSE_IDS"

        # 取得したレースに出場した馬の詳細情報を収集
        echo "[TASK $HORSE_TASK_NUM] 出場馬の詳細情報を収集 (開始: $(date))"
        python $HORSE_SCRAPER --source file --file "$LATEST_HORSE_IDS" --batch_size $BATCH_SIZE --pause $PAUSE_SECONDS --limit $MAX_RACES $SKIP_OPTION > scraping_logs/race_horses_${TIMESTAMP}.log 2>&1
    fi

    echo "馬データ収集完了 ($(date))"
fi

# データ収集の結果サマリーを表示
echo "======================================================================="
echo "  収集データサマリー ($(date))"
echo "======================================================================="

# レースデータファイルの統計
if [ "$COLLECT_RACES" = true ]; then
    echo "最終レースデータファイル:"
    FINAL_CSV_FILE="keiba_data/races_${TARGET_YEAR}_${TIMESTAMP}.csv"
    if [ -f "$FINAL_CSV_FILE" ]; then
        lines=$(wc -l < "$FINAL_CSV_FILE")
        lines=$((lines - 1)) # ヘッダー行を除く
        echo "- $FINAL_CSV_FILE: $lines レース"
    else
        echo "  結合されたレースCSVファイルがありません"
    fi
fi

# 馬情報データファイルの統計
if [ "$COLLECT_HORSES" = true ]; then
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

# 処理完了後に年別フォルダにファイルを整理
if [ "$ORGANIZE_FILES" = true ]; then
    organize_files_by_year $TARGET_YEAR
fi

echo "======================================================================="
echo "  ${TARGET_YEAR}年競馬データ収集タスク - 完了: $(date)"
echo "======================================================================="
