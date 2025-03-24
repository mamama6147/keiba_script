import pandas as pd
import glob
import os
import json
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# データ保存用のディレクトリを作成
output_dir = 'preprocessed_data'
os.makedirs(output_dir, exist_ok=True)

def integrate_data():
    """収集したデータを統合する関数"""
    print("=== データの統合を開始 ===")
    
    # レースデータの統合
    race_files = glob.glob('keiba_data/races_*.csv')
    race_dfs = []
    for file in race_files:
        df = pd.read_csv(file, encoding='utf-8-sig')
        # ファイル名から年を抽出して列として追加
        year = file.split('races_')[1].split('_')[0]
        df['file_year'] = year
        race_dfs.append(df)

    races_df = pd.concat(race_dfs, ignore_index=True)
    print(f"レースデータ: {len(races_df)}行, {races_df.shape[1]}列")

    # 馬の基本情報の統合
    horse_info_files = glob.glob('horse_data/horse_info_*.csv')
    horse_info_dfs = [pd.read_csv(file, encoding='utf-8-sig') for file in horse_info_files]
    horse_info_df = pd.concat(horse_info_dfs, ignore_index=True)
    print(f"馬情報データ: {len(horse_info_df)}行, {horse_info_df.shape[1]}列")

    # 馬の出走履歴の統合
    horse_history_files = glob.glob('horse_data/horse_history_*.csv')
    horse_history_dfs = [pd.read_csv(file, encoding='utf-8-sig') for file in horse_history_files]
    horse_history_df = pd.concat(horse_history_dfs, ignore_index=True)
    print(f"出走履歴データ: {len(horse_history_df)}行, {horse_history_df.shape[1]}列")

    # 調教データの統合（存在する場合）
    horse_training_files = glob.glob('horse_data/horse_training_*.csv')
    if horse_training_files:
        horse_training_dfs = [pd.read_csv(file, encoding='utf-8-sig') for file in horse_training_files]
        horse_training_df = pd.concat(horse_training_dfs, ignore_index=True)
        print(f"調教データ: {len(horse_training_df)}行, {horse_training_df.shape[1]}列")
    else:
        horse_training_df = None
        print("調教データが見つかりませんでした")
    
    print("=== データの統合が完了 ===")
    
    return races_df, horse_info_df, horse_history_df, horse_training_df

def remove_duplicates(races_df, horse_info_df, horse_history_df, horse_training_df):
    """重複データの削除と一貫性確保"""
    print("=== 重複データの削除を開始 ===")
    
    # 重複レコードの削除
    races_df_clean = races_df.drop_duplicates(subset=['race_id', 'horse_id'], keep='last')
    print(f"重複削除前のレースデータ: {len(races_df)}行, 重複削除後: {len(races_df_clean)}行")
    
    # 馬情報の重複削除（同じ馬IDで最新のデータを保持）
    if 'birth_date' in horse_info_df.columns:
        horse_info_df_clean = horse_info_df.sort_values('birth_date', ascending=False).drop_duplicates(subset=['horse_id'], keep='first')
    else:
        horse_info_df_clean = horse_info_df.drop_duplicates(subset=['horse_id'], keep='last')
    print(f"重複削除前の馬情報データ: {len(horse_info_df)}行, 重複削除後: {len(horse_info_df_clean)}行")
    
    # 出走履歴の重複削除
    if horse_history_df is not None:
        horse_history_df_clean = horse_history_df.drop_duplicates(subset=['horse_id', 'race_id'], keep='last')
        print(f"重複削除前の出走履歴データ: {len(horse_history_df)}行, 重複削除後: {len(horse_history_df_clean)}行")
    else:
        horse_history_df_clean = None
    
    # 調教データの重複削除（日付と馬IDの組み合わせで最新を保持）
    if horse_training_df is not None:
        if 'date' in horse_training_df.columns:
            horse_training_df_clean = horse_training_df.sort_values('date', ascending=False).drop_duplicates(subset=['horse_id', 'date'], keep='first')
        else:
            horse_training_df_clean = horse_training_df.drop_duplicates()
        print(f"重複削除前の調教データ: {len(horse_training_df)}行, 重複削除後: {len(horse_training_df_clean)}行")
    else:
        horse_training_df_clean = None
    
    print("=== 重複データの削除が完了 ===")
    
    return races_df_clean, horse_info_df_clean, horse_history_df_clean, horse_training_df_clean

def handle_missing_values(races_df):
    """欠損値の処理と型変換"""
    print("=== 欠損値の処理と型変換を開始 ===")
    
    # レースデータの欠損値確認
    missing_values = races_df.isnull().sum()
    print("レースデータの欠損値:")
    print(missing_values[missing_values > 0])
    
    # 日付データの型変換（race_dateが文字列形式の場合）
    if 'race_date' in races_df.columns:
        try:
            # 日付フォーマットを検出して変換
            sample_date = races_df['race_date'].dropna().iloc[0]
            if '年' in str(sample_date) and '月' in str(sample_date) and '日' in str(sample_date):
                # 「2023年4月1日」形式の場合
                races_df['race_date'] = pd.to_datetime(races_df['race_date'], format='%Y年%m月%d日', errors='coerce')
            else:
                # その他の形式の場合はpandasに推測させる
                races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
        except:
            print("日付変換に失敗しました。形式を確認してください。")
    
    # 数値データの型変換
    # 着順をint型に変換（競走除外や失格などは欠損値として扱う）
    if '着順' in races_df.columns:
        # 着順が数値以外の場合の処理（'除', '取', '失', '中', '降'などの特殊値）
        def convert_rank(rank):
            try:
                return int(rank)
            except:
                return np.nan
                
        races_df['着順_数値'] = races_df['着順'].apply(convert_rank)
    
    # タイム（文字列フォーマット "1:23.4" など）を秒数に変換する関数
    def convert_time_to_seconds(time_str):
        if pd.isna(time_str):
            return None
        try:
            if ':' in str(time_str):
                minutes, seconds = str(time_str).split(':')
                return float(minutes) * 60 + float(seconds)
            else:
                # タイムが秒だけで記録されている場合
                return float(time_str)
        except:
            return None
    
    # タイムを秒に変換
    if 'タイム' in races_df.columns:
        races_df['タイム_秒'] = races_df['タイム'].apply(convert_time_to_seconds)
    
    # 距離をint型に変換
    if 'distance' in races_df.columns:
        races_df['distance'] = pd.to_numeric(races_df['distance'], errors='coerce')
    
    # 重要なカテゴリカル変数の欠損値を「不明」で埋める
    categorical_cols = ['weather', 'track_condition', 'course_type', 'race_class']
    for col in categorical_cols:
        if col in races_df.columns:
            races_df[col] = races_df[col].fillna('不明')
    
    # 馬体重と馬体重変化の処理
    if '体重' in races_df.columns:
        races_df['体重'] = pd.to_numeric(races_df['体重'], errors='coerce')
    
    if '体重変化' in races_df.columns:
        races_df['体重変化'] = pd.to_numeric(races_df['体重変化'], errors='coerce')
    
    print("=== 欠損値の処理と型変換が完了 ===")
    
    return races_df

def handle_outliers(races_df):
    """外れ値の検出と処理"""
    print("=== 外れ値の検出と処理を開始 ===")
    
    races_df_copy = races_df.copy()
    
    # タイムの外れ値を検出
    if 'タイム_秒' in races_df_copy.columns:
        # 距離別にタイムの分布を確認
        if 'distance' in races_df_copy.columns:
            plt.figure(figsize=(12, 8))
            sns.boxplot(x='distance', y='タイム_秒', data=races_df_copy)
            plt.title('距離別のタイム分布')
            plt.xticks(rotation=90)
            plt.savefig(f'{output_dir}/time_by_distance.png')
            plt.close()
            
            # 距離ごとに外れ値を処理
            grouped = races_df_copy.groupby('distance')
            clean_dfs = []
            
            for distance, group in grouped:
                if len(group) < 2:  # 統計処理には最低2つのデータが必要
                    clean_dfs.append(group)
                    continue
                    
                # Z-scoreを計算
                z_scores = np.abs((group['タイム_秒'] - group['タイム_秒'].mean()) / group['タイム_秒'].std())
                # Z-scoreが3未満のデータのみを保持
                clean_group = group[z_scores < 3]
                clean_dfs.append(clean_group)
            
            races_df_clean = pd.concat(clean_dfs)
            print(f"タイムの外れ値除去前: {len(races_df_copy)}行, 外れ値除去後: {len(races_df_clean)}行")
            races_df_copy = races_df_clean.copy()
    
    # 馬体重の外れ値を検出
    if '体重' in races_df_copy.columns:
        plt.figure(figsize=(10, 6))
        sns.histplot(races_df_copy['体重'].dropna(), bins=50)
        plt.title('馬体重の分布')
        plt.savefig(f'{output_dir}/weight_distribution.png')
        plt.close()
        
        # 極端な体重値を除外（例: 300kg未満や700kg超は誤記の可能性）
        weight_mask = (races_df_copy['体重'] >= 300) & (races_df_copy['体重'] <= 700)
        races_df_clean = races_df_copy[weight_mask | races_df_copy['体重'].isna()]
        print(f"体重の外れ値除去前: {len(races_df_copy)}行, 外れ値除去後: {len(races_df_clean)}行")
        races_df_copy = races_df_clean.copy()
    
    print("=== 外れ値の検出と処理が完了 ===")
    
    return races_df_copy

def main():
    """メイン実行関数"""
    # 1. データの統合
    races_df, horse_info_df, horse_history_df, horse_training_df = integrate_data()
    
    # 2. 重複の排除
    races_df, horse_info_df, horse_history_df, horse_training_df = remove_duplicates(
        races_df, horse_info_df, horse_history_df, horse_training_df
    )
    
    # 3. 欠損値の処理と型変換
    races_df = handle_missing_values(races_df)
    
    # 4. 外れ値の検出と処理
    races_df_clean = handle_outliers(races_df)
    
    # 前処理されたデータを保存
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    races_df_clean.to_csv(f'{output_dir}/cleaned_races_{timestamp}.csv', index=False, encoding='utf-8-sig')
    horse_info_df.to_csv(f'{output_dir}/cleaned_horse_info_{timestamp}.csv', index=False, encoding='utf-8-sig')
    
    if horse_history_df is not None:
        horse_history_df.to_csv(f'{output_dir}/cleaned_horse_history_{timestamp}.csv', index=False, encoding='utf-8-sig')
    
    if horse_training_df is not None:
        horse_training_df.to_csv(f'{output_dir}/cleaned_horse_training_{timestamp}.csv', index=False, encoding='utf-8-sig')
    
    print(f"前処理済みデータを {output_dir} ディレクトリに保存しました。")
    print(f"タイムスタンプ: {timestamp}")
    
    return races_df_clean, horse_info_df, horse_history_df, horse_training_df

if __name__ == "__main__":
    main()
