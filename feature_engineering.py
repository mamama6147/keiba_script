import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import glob

# 前処理済みデータが保存されているディレクトリ
input_dir = 'preprocessed_data'
# 特徴量エンジニアリング結果を保存するディレクトリ
output_dir = 'feature_data'
os.makedirs(output_dir, exist_ok=True)

def load_preprocessed_data():
    """前処理済みデータを読み込む関数"""
    print("=== 前処理済みデータの読み込みを開始 ===")
    
    # 最新の前処理済みファイルを検索
    race_files = sorted(glob.glob(f'{input_dir}/cleaned_races_*.csv'), reverse=True)
    horse_info_files = sorted(glob.glob(f'{input_dir}/cleaned_horse_info_*.csv'), reverse=True)
    horse_history_files = sorted(glob.glob(f'{input_dir}/cleaned_horse_history_*.csv'), reverse=True)
    
    # 最新のファイルを読み込む
    races_df = pd.read_csv(race_files[0], encoding='utf-8-sig') if race_files else None
    horse_info_df = pd.read_csv(horse_info_files[0], encoding='utf-8-sig') if horse_info_files else None
    horse_history_df = pd.read_csv(horse_history_files[0], encoding='utf-8-sig') if horse_history_files else None
    
    if races_df is not None:
        print(f"レースデータ: {len(races_df)}行, {races_df.shape[1]}列")
    else:
        print("レースデータが見つかりませんでした。")
    
    if horse_info_df is not None:
        print(f"馬情報データ: {len(horse_info_df)}行, {horse_info_df.shape[1]}列")
    else:
        print("馬情報データが見つかりませんでした。")
    
    if horse_history_df is not None:
        print(f"出走履歴データ: {len(horse_history_df)}行, {horse_history_df.shape[1]}列")
    else:
        print("出走履歴データが見つかりませんでした。")
    
    print("=== 前処理済みデータの読み込みが完了 ===")
    
    return races_df, horse_info_df, horse_history_df

def create_time_series_features(races_df):
    """時系列特徴量の作成"""
    print("=== 時系列特徴量の作成を開始 ===")
    
    if races_df is None:
        print("レースデータがありません。")
        return None
    
    # レースデータを日付順にソート
    if 'race_date' in races_df.columns:
        if not pd.api.types.is_datetime64_dtype(races_df['race_date']):
            races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
        
        races_df = races_df.sort_values('race_date')
    
    # 馬ごとの過去レース結果を集計する関数
    def calculate_horse_features(group):
        # グループを日付順にソート
        if 'race_date' in group.columns:
            group = group.sort_values('race_date')
        
        # 累積出走回数
        group['出走回数'] = range(1, len(group) + 1)
        
        # 過去3走の着順平均
        if '着順_数値' in group.columns:
            group['過去3走着順平均'] = group['着順_数値'].rolling(window=3, min_periods=1).mean().shift(1)
            
            # 過去の勝率計算
            group['累積勝利数'] = (group['着順_数値'] == 1).cumsum().shift(1).fillna(0)
            group['累積勝率'] = group['累積勝利数'] / group['出走回数'].shift(1).fillna(1)
            
            # 過去の複勝率（3着以内）
            group['累積複勝数'] = ((group['着順_数値'] >= 1) & (group['着順_数値'] <= 3)).cumsum().shift(1).fillna(0)
            group['累積複勝率'] = group['累積複勝数'] / group['出走回数'].shift(1).fillna(1)
        
        # 過去のタイム情報
        if 'タイム_秒' in group.columns and 'distance' in group.columns:
            # 距離別の平均タイム（過去全走）
            group['平均タイム'] = group.groupby('distance')['タイム_秒'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
            
            # 最近3走の平均タイム
            group['過去3走タイム平均'] = group['タイム_秒'].rolling(window=3, min_periods=1).mean().shift(1)
        
        # 休養期間（日数）
        if 'race_date' in group.columns:
            group['前走日'] = group['race_date'].shift(1)
            group['休養日数'] = (group['race_date'] - group['前走日']).dt.days
        
        # 同一コース・距離での成績
        if 'course_type' in group.columns and 'distance' in group.columns and '着順_数値' in group.columns:
            # コース・距離の組み合わせを作成
            group['コース距離'] = group['course_type'] + '_' + group['distance'].astype(str)
            
            # 同一コース・距離での過去平均着順
            group['同コース距離_過去平均着順'] = group.groupby('コース距離')['着順_数値'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
            
            # 同一コース・距離での過去勝率
            win_counts = (group['着順_数値'] == 1).astype(int)
            group['同コース距離_勝利数'] = group.groupby('コース距離')[win_counts.name].transform(
                lambda x: x.cumsum().shift(1)
            ).fillna(0)
            
            group['同コース距離_出走回数'] = group.groupby('コース距離').cumcount().shift(1).fillna(0) + 1
            group['同コース距離_勝率'] = group['同コース距離_勝利数'] / group['同コース距離_出走回数']
        
        return group
    
    # 馬ごとに特徴量を計算
    print("馬ごとの時系列特徴量を計算中...")
    horse_features = races_df.groupby('horse_id').apply(calculate_horse_features)
    
    # グループ化による多重インデックスを解除
    horse_features = horse_features.reset_index(drop=True)
    
    # 特徴量の欠損値を埋める
    numerical_features = ['過去3走着順平均', '累積勝率', '累積複勝率', '過去3走タイム平均', 
                          '休養日数', '同コース距離_過去平均着順', '同コース距離_勝率']
    
    for feature in numerical_features:
        if feature in horse_features.columns:
            # 中央値で埋める
            median_value = horse_features[feature].median()
            horse_features[feature] = horse_features[feature].fillna(median_value)
            print(f"特徴量 '{feature}' の欠損値を中央値 {median_value:.4f} で補完しました。")
    
    # 特徴量の分布を確認
    for feature in numerical_features:
        if feature in horse_features.columns:
            plt.figure(figsize=(10, 6))
            sns.histplot(horse_features[feature].dropna(), bins=50, kde=True)
            plt.title(f'{feature}の分布')
            plt.savefig(f'{output_dir}/{feature}_distribution.png')
            plt.close()
    
    print("=== 時系列特徴量の作成が完了 ===")
    
    return horse_features

def create_jockey_features(races_df):
    """騎手の特徴量作成"""
    print("=== 騎手の特徴量作成を開始 ===")
    
    if races_df is None or '騎手' not in races_df.columns:
        print("騎手データが利用できません。")
        return races_df
    
    # 日付型の確認
    if 'race_date' in races_df.columns and not pd.api.types.is_datetime64_dtype(races_df['race_date']):
        races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
    
    # 直近の成績を集計する期間（日数）
    recent_days = 90
    
    # 騎手ごとの過去成績を計算する関数
    def calculate_jockey_features(df):
        df = df.sort_values('race_date') if 'race_date' in df.columns else df
        results = []
        
        total_rows = len(df)
        for idx, row in df.iterrows():
            if idx % 1000 == 0:
                print(f"騎手特徴量の計算: {idx}/{total_rows} 行完了")
                
            jockey = row['騎手']
            
            # 日付ベースのフィルタリング
            if 'race_date' in df.columns:
                race_date = row['race_date']
                
                # 当該レース以前のデータのみを対象
                past_data = df[(df['race_date'] < race_date) & (df['騎手'] == jockey)]
                
                # 直近90日間のデータ
                recent_data = past_data[past_data['race_date'] >= (race_date - pd.Timedelta(days=recent_days))]
            else:
                # 日付がない場合はインデックスベースで判断
                past_data = df[(df.index < idx) & (df['騎手'] == jockey)]
                recent_data = past_data.iloc[-30:] if len(past_data) > 30 else past_data  # 直近30件
            
            # 過去全ての成績
            all_rides = len(past_data)
            all_wins = sum(past_data['着順_数値'] == 1) if '着順_数値' in past_data.columns else 0
            all_win_rate = all_wins / all_rides if all_rides > 0 else 0
            
            # 直近の成績
            recent_rides = len(recent_data)
            recent_wins = sum(recent_data['着順_数値'] == 1) if '着順_数値' in recent_data.columns else 0
            recent_win_rate = recent_wins / recent_rides if recent_rides > 0 else 0
            
            # 騎手と同コースでの成績
            if 'course_type' in df.columns:
                course = row['course_type']
                course_data = past_data[past_data['course_type'] == course]
                course_rides = len(course_data)
                course_wins = sum(course_data['着順_数値'] == 1) if '着順_数値' in course_data.columns else 0
                course_win_rate = course_wins / course_rides if course_rides > 0 else 0
            else:
                course_rides = 0
                course_wins = 0
                course_win_rate = 0
            
            results.append({
                'race_id': row['race_id'],
                'horse_id': row['horse_id'],
                '騎手_全成績_騎乗数': all_rides,
                '騎手_全成績_勝利数': all_wins,
                '騎手_全成績_勝率': all_win_rate,
                '騎手_直近成績_騎乗数': recent_rides,
                '騎手_直近成績_勝利数': recent_wins,
                '騎手_直近成績_勝率': recent_win_rate,
                '騎手_同コース_騎乗数': course_rides,
                '騎手_同コース_勝利数': course_wins,
                '騎手_同コース_勝率': course_win_rate
            })
        
        return pd.DataFrame(results)
    
    # 計算量を削減するためサンプルデータで先にテスト
    sample_size = min(10000, len(races_df))
    print(f"騎手特徴量の計算: サンプル {sample_size} 件でテスト実行します")
    
    sampled_races = races_df.sample(sample_size, random_state=42)
    jockey_features_sample = calculate_jockey_features(sampled_races)
    
    # サンプルで特徴量が作成できた場合、全データで計算
    if jockey_features_sample is not None and len(jockey_features_sample) > 0:
        print(f"全データ {len(races_df)} 件で騎手特徴量を計算します")
        jockey_features = calculate_jockey_features(races_df)
        
        # 特徴量の有効性を確認（勝率と着順の関係）
        if '着順_数値' in races_df.columns:
            jockey_correlation = jockey_features['騎手_直近成績_勝率'].corr(
                pd.merge(
                    jockey_features[['race_id', 'horse_id', '騎手_直近成績_勝率']], 
                    races_df[['race_id', 'horse_id', '着順_数値']], 
                    on=['race_id', 'horse_id']
                )['着順_数値']
            )
            print(f"騎手の直近勝率と着順の相関係数: {jockey_correlation:.4f}")
            
            # 散布図で関係を確認
            plt.figure(figsize=(10, 6))
            plt.scatter(
                jockey_features['騎手_直近成績_勝率'].iloc[:5000],  # プロット数を制限
                pd.merge(
                    jockey_features[['race_id', 'horse_id', '騎手_直近成績_勝率']], 
                    races_df[['race_id', 'horse_id', '着順_数値']], 
                    on=['race_id', 'horse_id']
                )['着順_数値'].iloc[:5000]
            )
            plt.title('騎手の直近勝率と着順の関係')
            plt.xlabel('騎手の直近90日間の勝率')
            plt.ylabel('着順')
            plt.grid(True)
            plt.savefig(f'{output_dir}/jockey_winrate_vs_rank.png')
            plt.close()
        
        # 特徴量の結合
        print("元のデータフレームに騎手特徴量を結合します")
        races_df = pd.merge(
            races_df, 
            jockey_features[['race_id', 'horse_id', '騎手_直近成績_勝率', '騎手_同コース_勝率']], 
            on=['race_id', 'horse_id'], 
            how='left'
        )
    else:
        print("騎手特徴量の計算でエラーが発生したため、スキップします")
    
    print("=== 騎手の特徴量作成が完了 ===")
    
    return races_df

def create_pace_features(races_df):
    """ペース・上がりタイムの特徴量作成"""
    print("=== ペース・上がりタイムの特徴量作成を開始 ===")
    
    if races_df is None:
        print("レースデータがありません。")
        return races_df
    
    races_df_copy = races_df.copy()
    
    # 通過順位からペース特徴量を作成
    if '通過順' in races_df_copy.columns:
        # 通過順をパース（例: "3-3-2-1" -> [3, 3, 2, 1]）
        def parse_passage_order(order_str):
            if pd.isna(order_str) or order_str == '':
                return []
            try:
                return [int(pos) for pos in str(order_str).split('-')]
            except:
                return []
        
        races_df_copy['通過順リスト'] = races_df_copy['通過順'].apply(parse_passage_order)
        
        # 各馬の通過順変化を特徴量として追加
        def extract_pace_features(passage_list):
            result = {}
            
            if not passage_list or len(passage_list) < 2:
                result['first_position'] = None
                result['position_change'] = None
                result['early_pace'] = None
                result['middle_pace'] = None
                result['late_pace'] = None
                return result
            
            # 最初の通過順
            result['first_position'] = passage_list[0]
            
            # 最後と最初の順位差
            result['position_change'] = passage_list[-1] - passage_list[0]
            
            # ペースの特徴（序盤、中盤、終盤）
            if len(passage_list) >= 3:
                # 序盤: 1→2の変化
                result['early_pace'] = passage_list[1] - passage_list[0]
                
                # 中盤: 中間地点の変化
                mid_idx = len(passage_list) // 2
                result['middle_pace'] = passage_list[mid_idx] - passage_list[mid_idx - 1]
                
                # 終盤: 最後の変化
                result['late_pace'] = passage_list[-1] - passage_list[-2]
            else:
                # 通過順が少ない場合
                result['early_pace'] = None
                result['middle_pace'] = None
                result['late_pace'] = passage_list[-1] - passage_list[0]  # 全体の変化
            
            return result
        
        # 通過順特徴量を適用
        print("通過順データから特徴量を抽出中...")
        pace_features = races_df_copy['通過順リスト'].apply(extract_pace_features)
        pace_df = pd.DataFrame(pace_features.tolist())
        
        # 特徴量をメインデータフレームに結合
        races_df_copy = pd.concat([races_df_copy, pace_df], axis=1)
        
        # 特徴量の分布を可視化
        pace_columns = ['first_position', 'position_change', 'early_pace', 'middle_pace', 'late_pace']
        for col in pace_columns:
            if col in races_df_copy.columns:
                plt.figure(figsize=(10, 6))
                sns.histplot(races_df_copy[col].dropna(), bins=30, kde=True)
                plt.title(f'{col}の分布')
                plt.savefig(f'{output_dir}/{col}_distribution.png')
                plt.close()
    
    # 上がりタイムの特徴量
    if '上がり' in races_df_copy.columns:
        # 上がりタイムを数値に変換
        def convert_last_3f(last_3f):
            if pd.isna(last_3f) or last_3f == '':
                return None
            try:
                return float(last_3f)
            except:
                return None
        
        races_df_copy['上がりタイム'] = races_df_copy['上がり'].apply(convert_last_3f)
        
        # レースごとの上がりタイム平均
        races_df_copy['上がりタイム_レース平均'] = races_df_copy.groupby('race_id')['上がりタイム'].transform('mean')
        
        # 上がりタイムの相対値（レース平均との差）
        races_df_copy['上がりタイム_相対'] = races_df_copy['上がりタイム'] - races_df_copy['上がりタイム_レース平均']
        
        # 特徴量の分布を可視化
        plt.figure(figsize=(10, 6))
        sns.histplot(races_df_copy['上がりタイム_相対'].dropna(), bins=30, kde=True)
        plt.title('上がりタイム_相対の分布')
        plt.savefig(f'{output_dir}/relative_last_3f_distribution.png')
        plt.close()
    
    print("=== ペース・上がりタイムの特徴量作成が完了 ===")
    
    return races_df_copy

def create_pedigree_features(horse_info_df, races_df):
    """血統と馬場適性の特徴量作成"""
    print("=== 血統と馬場適性の特徴量作成を開始 ===")
    
    if horse_info_df is None or races_df is None:
        print("馬情報またはレースデータがありません。")
        return horse_info_df, races_df
    
    horse_info_df_copy = horse_info_df.copy()
    
    # 血統情報から特徴を抽出
    if 'father' in horse_info_df_copy.columns:
        # 主要な父系統のカウント
        father_counts = horse_info_df_copy['father'].value_counts()
        top_fathers = father_counts[father_counts >= 10].index.tolist()
        
        print(f"主要な父系統（10頭以上）: {len(top_fathers)}系統")
        
        # 主要父系のワンホットエンコーディング
        for father in top_fathers[:30]:  # 上位30系統までエンコード
            horse_info_df_copy[f'父系_{father}'] = (horse_info_df_copy['father'] == father).astype(int)
    
    # 母父系統のカウント
    if 'maternal_grandfather' in horse_info_df_copy.columns:
        mgf_counts = horse_info_df_copy['maternal_grandfather'].value_counts()
        top_mgfs = mgf_counts[mgf_counts >= 10].index.tolist()
        
        print(f"主要な母父系統（10頭以上）: {len(top_mgfs)}系統")
        
        # 主要母父系のワンホットエンコーディング
        for mgf in top_mgfs[:30]:  # 上位30系統までエンコード
            horse_info_df_copy[f'母父系_{mgf}'] = (horse_info_df_copy['maternal_grandfather'] == mgf).astype(int)
    
    # 馬場適性の分析
    # 血統と馬場状態のクロス分析 - 父系と馬場適性の関係
    if 'father' in horse_info_df_copy.columns and 'horse_id' in horse_info_df_copy.columns:
        # 馬情報とレース結果を結合
        if 'track_condition' in races_df.columns and 'horse_id' in races_df.columns:
            # 父系ごとの馬場別成績を集計
            father_track_analysis = races_df.merge(
                horse_info_df_copy[['horse_id', 'father']], 
                on='horse_id', 
                how='left'
            ).dropna(subset=['father'])
            
            father_track_stats = father_track_analysis.groupby(['father', 'track_condition']).agg(
                races_count=('race_id', 'count'),
                win_count=('着順_数値', lambda x: sum(x == 1)),
                avg_rank=('着順_数値', 'mean')
            ).reset_index()
            
            # 勝率を計算
            father_track_stats['win_rate'] = father_track_stats['win_count'] / father_track_stats['races_count'] * 100
            
            # 十分なサンプルがある父系のみをフィルタリング（各馬場状態で最低3レース）
            min_races_per_condition = 3
            filtered_stats = father_track_stats[father_track_stats['races_count'] >= min_races_per_condition]
            
            # 上位の父系のみを表示
            top_fathers = filtered_stats.groupby('father')['races_count'].sum().nlargest(20).index.tolist()
            top_father_stats = filtered_stats[filtered_stats['father'].isin(top_fathers)]
            
            # ヒートマップで馬場適性を可視化
            plt.figure(figsize=(15, 10))
            pivot_data = top_father_stats.pivot(index='father', columns='track_condition', values='win_rate')
            sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='YlGnBu')
            plt.title('父系統別の馬場適性（勝率%）')
            plt.tight_layout()
            plt.savefig(f'{output_dir}/father_track_condition_heatmap.png')
            plt.close()
            
            # 馬場適性スコアを作成
            # 各父系の馬場別勝率から相対スコアを計算
            track_conditions = races_df['track_condition'].unique()
            
            # 各馬場状態の平均勝率を計算
            avg_win_rates = father_track_stats.groupby('track_condition')['win_rate'].mean().to_dict()
            
            # 父系ごとに各馬場状態での適性スコアを計算（平均との差）
            father_adaptability = {}
            
            for _, row in filtered_stats.iterrows():
                father = row['father']
                condition = row['track_condition']
                win_rate = row['win_rate']
                
                if father not in father_adaptability:
                    father_adaptability[father] = {}
                
                # 平均との差をスコアとする
                avg_rate = avg_win_rates.get(condition, 0)
                score = win_rate - avg_rate
                
                father_adaptability[father][condition] = score
            
            # 馬ごとに馬場適性スコアを付与
            def assign_track_adaptability(row):
                father = row['father']
                scores = {}
                
                if father in father_adaptability:
                    for condition in track_conditions:
                        score = father_adaptability[father].get(condition, 0)
                        scores[f'馬場適性_{condition}'] = score
                else:
                    for condition in track_conditions:
                        scores[f'馬場適性_{condition}'] = 0
                
                return pd.Series(scores)
            
            # 適性スコアを計算して追加
            adaptability_scores = horse_info_df_copy.apply(assign_track_adaptability, axis=1)
            horse_info_df_copy = pd.concat([horse_info_df_copy, adaptability_scores], axis=1)
            
            # 馬場適性スコアの上位馬を確認
            for condition in track_conditions:
                score_col = f'馬場適性_{condition}'
                if score_col in horse_info_df_copy.columns:
                    print(f"{condition}馬場の適性スコア上位馬:")
                    top_horses = horse_info_df_copy.nlargest(10, score_col)[['horse_id', 'name', 'father', score_col]]
                    print(top_horses)
    
    print("=== 血統と馬場適性の特徴量作成が完了 ===")
    
    return horse_info_df_copy, races_df

def integrate_features_and_save(horse_features, races_df, horse_info_df):
    """特徴量の統合と保存"""
    print("=== 特徴量の統合と保存を開始 ===")
    
    if horse_features is None or races_df is None:
        print("特徴量または元データがありません。")
        return None
    
    # 特徴量の相関行列を可視化
    numerical_features = horse_features.select_dtypes(include=['number']).columns.tolist()
    
    # 出走回数や着順など、特徴と直接関係ないカラムは除外
    exclude_cols = ['race_id', 'horse_id', '出走回数', '着順_数値', 'タイム_秒', '体重', '体重変化']
    numerical_features = [col for col in numerical_features if col not in exclude_cols]
    
    if len(numerical_features) > 2:  # 少なくとも2つ以上の特徴量が必要
        # サンプルを取得（大きな相関行列は可視化が難しいため）
        selected_features = numerical_features[:15]  # 最大15特徴まで
        
        # 相関行列の計算と可視化
        corr_matrix = horse_features[selected_features].corr()
        
        plt.figure(figsize=(15, 15))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f')
        plt.title('特徴量間の相関関係')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/feature_correlation_matrix.png')
        plt.close()
    
    # 前処理済みデータの保存
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 特徴量を含むメインデータセットを保存
    horse_features.to_csv(f'{output_dir}/race_features_{timestamp}.csv', index=False, encoding='utf-8-sig')
    print(f"レース特徴量を保存しました: race_features_{timestamp}.csv")
    
    # 血統特徴量を保存（もし作成されていれば）
    if horse_info_df is not None:
        horse_info_df.to_csv(f'{output_dir}/horse_features_{timestamp}.csv', index=False, encoding='utf-8-sig')
        print(f"馬特徴量を保存しました: horse_features_{timestamp}.csv")
    
    # モデリング用に統合されたデータセットを保存
    if 'horse_id' in horse_features.columns and horse_info_df is not None and 'horse_id' in horse_info_df.columns:
        # 血統特徴のみを抽出
        pedigree_cols = [col for col in horse_info_df.columns if col.startswith('父系_') or col.startswith('母父系_') or col.startswith('馬場適性_')]
        pedigree_cols.append('horse_id')
        
        # 馬情報と特徴量を結合
        merged_data = horse_features.merge(
            horse_info_df[pedigree_cols],
            on='horse_id',
            how='left'
        )
        
        # 最終データセットを保存
        merged_data.to_csv(f'{output_dir}/modeling_dataset_{timestamp}.csv', index=False, encoding='utf-8-sig')
        print(f"モデリング用データセットを保存しました: modeling_dataset_{timestamp}.csv")
        
        return merged_data
    
    print("=== 特徴量の統合と保存が完了 ===")
    
    return horse_features

def main():
    """メイン実行関数"""
    # 1. 前処理済みデータの読み込み
    races_df, horse_info_df, horse_history_df = load_preprocessed_data()
    
    if races_df is None:
        print("レースデータが利用できないため、処理を中止します。")
        return
    
    # 2. 時系列特徴量の作成
    horse_features = create_time_series_features(races_df)
    
    # 3. 騎手の特徴量作成
    races_df_with_jockey = create_jockey_features(races_df)
    
    if horse_features is not None and races_df_with_jockey is not None:
        # 騎手特徴量をメインの特徴量データフレームにマージ
        horse_features = pd.merge(
            horse_features,
            races_df_with_jockey[['race_id', 'horse_id', '騎手_直近成績_勝率', '騎手_同コース_勝率']].drop_duplicates(subset=['race_id', 'horse_id']),
            on=['race_id', 'horse_id'],
            how='left'
        )
    
    # 4. ペース・上がりタイムの特徴量作成
    races_df_with_pace = create_pace_features(races_df)
    
    if horse_features is not None and races_df_with_pace is not None:
        # ペース特徴量をメインの特徴量データフレームにマージ
        pace_cols = ['race_id', 'horse_id', 'first_position', 'position_change', 'early_pace', 'middle_pace', 'late_pace', 
                     '上がりタイム', '上がりタイム_レース平均', '上がりタイム_相対']
        
        merge_cols = [col for col in pace_cols if col in races_df_with_pace.columns]
        
        if len(merge_cols) > 2:  # race_id, horse_id以外にも列がある場合
            horse_features = pd.merge(
                horse_features,
                races_df_with_pace[merge_cols].drop_duplicates(subset=['race_id', 'horse_id']),
                on=['race_id', 'horse_id'],
                how='left'
            )
    
    # 5. 血統と馬場適性の特徴量作成
    horse_info_df_with_pedigree, _ = create_pedigree_features(horse_info_df, races_df)
    
    # 6. 特徴量の統合と保存
    final_dataset = integrate_features_and_save(horse_features, races_df, horse_info_df_with_pedigree)
    
    if final_dataset is not None:
        print(f"最終的なモデリングデータセットの形状: {final_dataset.shape}")
        print(f"含まれる特徴量: {final_dataset.columns.tolist()}")
    
    print("特徴量エンジニアリングのすべての処理が完了しました！")

if __name__ == "__main__":
    main()
