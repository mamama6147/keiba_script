import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

# 前処理済みデータが保存されているディレクトリ
input_dir = 'preprocessed_data'
# 分析結果を保存するディレクトリ
output_dir = 'analysis_results'
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

def analyze_race_statistics(races_df):
    """レースデータの基本統計分析"""
    print("=== レースデータの基本統計分析を開始 ===")
    
    if races_df is None:
        print("レースデータがありません。")
        return
    
    # 基本統計量の確認
    print("=== レースデータの基本統計 ===")
    if 'タイム_秒' in races_df.columns:
        time_stats = races_df['タイム_秒'].describe()
        print(time_stats)
        
        # 統計情報をファイルに保存
        time_stats.to_csv(f'{output_dir}/time_statistics.csv')
    
    # 年ごとのレース数
    if 'race_date' in races_df.columns:
        # 日付型に変換
        if not pd.api.types.is_datetime64_dtype(races_df['race_date']):
            races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
        
        races_df['year'] = races_df['race_date'].dt.year
        yearly_races = races_df.groupby('year').size()
        print("\n年ごとのレース数:")
        print(yearly_races)
        
        # グラフを作成
        plt.figure(figsize=(10, 6))
        yearly_races.plot(kind='bar')
        plt.title('年ごとのレース数')
        plt.xlabel('年')
        plt.ylabel('レース数')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/yearly_races.png')
        plt.close()
        
        # 月ごとのレース数
        races_df['month'] = races_df['race_date'].dt.month
        monthly_races = races_df.groupby(['year', 'month']).size().unstack()
        
        plt.figure(figsize=(12, 8))
        monthly_races.plot(kind='bar', stacked=True)
        plt.title('年・月別レース数')
        plt.xlabel('年')
        plt.ylabel('レース数')
        plt.legend(title='月')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/monthly_races_by_year.png')
        plt.close()
    
    # コース種別ごとのレース数
    if 'course_type' in races_df.columns:
        course_races = races_df.groupby('course_type').size()
        print("\nコース種別ごとのレース数:")
        print(course_races)
        
        plt.figure(figsize=(10, 6))
        course_races.plot(kind='pie', autopct='%1.1f%%')
        plt.title('コース種別の割合')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/course_type_distribution.png')
        plt.close()
    
    # 馬場状態ごとの平均タイム
    if 'track_condition' in races_df.columns and 'タイム_秒' in races_df.columns:
        track_times = races_df.groupby(['track_condition', 'distance'])['タイム_秒'].mean().reset_index()
        
        plt.figure(figsize=(12, 8))
        for track in track_times['track_condition'].unique():
            data = track_times[track_times['track_condition'] == track]
            if len(data) > 0:  # データが存在する場合のみプロット
                plt.plot(data['distance'], data['タイム_秒'], 'o-', label=track)
        
        plt.title('馬場状態別の平均タイム（距離別）')
        plt.xlabel('距離 (m)')
        plt.ylabel('平均タイム (秒)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f'{output_dir}/track_condition_times.png')
        plt.close()
        
        # 詳細な統計情報をCSVとして保存
        detailed_track_stats = races_df.groupby(['track_condition', 'distance']).agg({
            'タイム_秒': ['count', 'mean', 'std', 'min', 'max'],
            'race_id': 'nunique'
        }).reset_index()
        
        detailed_track_stats.columns = ['馬場状態', '距離', 'サンプル数', '平均タイム', 'タイム標準偏差', '最速タイム', '最遅タイム', 'レース数']
        detailed_track_stats.to_csv(f'{output_dir}/track_condition_detailed_stats.csv', index=False, encoding='utf-8-sig')
    
    print("=== レースデータの基本統計分析が完了 ===")

def analyze_horse_performance(races_df, horse_info_df):
    """馬のパフォーマンス分析"""
    print("=== 馬のパフォーマンス分析を開始 ===")
    
    if races_df is None:
        print("レースデータがありません。")
        return
    
    # 勝率上位の馬を分析
    if 'horse_id' in races_df.columns and '着順_数値' in races_df.columns:
        # 十分な出走回数のある馬のみ対象
        min_races = 5
        
        # 馬ごとの成績を集計
        horse_stats = races_df.groupby('horse_id').agg(
            races_count=('race_id', 'count'),
            win_count=('着順_数値', lambda x: sum(x == 1)),
            place_count=('着順_数値', lambda x: sum((x >= 1) & (x <= 3))),
            avg_rank=('着順_数値', 'mean')
        ).reset_index()
        
        # 勝率と連対率を計算
        horse_stats['win_rate'] = horse_stats['win_count'] / horse_stats['races_count'] * 100
        horse_stats['place_rate'] = horse_stats['place_count'] / horse_stats['races_count'] * 100
        
        # 十分な出走回数のある馬のみをフィルタリング
        qualified_horses = horse_stats[horse_stats['races_count'] >= min_races]
        
        # 勝率上位の馬
        top_win_horses = qualified_horses.sort_values('win_rate', ascending=False).head(20)
        print("\n勝率上位の馬:")
        print(top_win_horses[['horse_id', 'races_count', 'win_count', 'win_rate']])
        
        # 馬名を結合（可能な場合）
        if horse_info_df is not None and 'horse_id' in horse_info_df.columns and 'name' in horse_info_df.columns:
            top_win_horses = top_win_horses.merge(
                horse_info_df[['horse_id', 'name']], 
                on='horse_id', 
                how='left'
            )
        
        # 勝率上位馬をグラフ化
        plt.figure(figsize=(12, 8))
        x_col = 'name' if 'name' in top_win_horses.columns else 'horse_id'
        sns.barplot(x=x_col, y='win_rate', data=top_win_horses)
        plt.title(f'勝率上位の馬（最低{min_races}レース出走）')
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/top_win_rate_horses.png')
        plt.close()
        
        # 連対率上位の馬
        top_place_horses = qualified_horses.sort_values('place_rate', ascending=False).head(20)
        
        # 馬名を結合（可能な場合）
        if horse_info_df is not None and 'horse_id' in horse_info_df.columns and 'name' in horse_info_df.columns:
            top_place_horses = top_place_horses.merge(
                horse_info_df[['horse_id', 'name']], 
                on='horse_id', 
                how='left'
            )
        
        # 連対率上位馬をグラフ化
        plt.figure(figsize=(12, 8))
        x_col = 'name' if 'name' in top_place_horses.columns else 'horse_id'
        sns.barplot(x=x_col, y='place_rate', data=top_place_horses)
        plt.title(f'連対率上位の馬（最低{min_races}レース出走）')
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/top_place_rate_horses.png')
        plt.close()
        
        # 詳細な馬の成績を保存
        if horse_info_df is not None and 'horse_id' in horse_info_df.columns:
            detailed_horse_stats = qualified_horses.merge(
                horse_info_df[['horse_id', 'name', 'father', 'mother', 'sex', 'birth_date']], 
                on='horse_id', 
                how='left'
            )
            detailed_horse_stats = detailed_horse_stats.sort_values('win_rate', ascending=False)
            detailed_horse_stats.to_csv(f'{output_dir}/horse_performance_stats.csv', index=False, encoding='utf-8-sig')
    
    print("=== 馬のパフォーマンス分析が完了 ===")

def analyze_jockey_trainer_performance(races_df):
    """騎手・調教師の分析"""
    print("=== 騎手・調教師のパフォーマンス分析を開始 ===")
    
    if races_df is None:
        print("レースデータがありません。")
        return
    
    # 騎手の成績分析
    if '騎手' in races_df.columns and '着順_数値' in races_df.columns:
        # 十分な騎乗回数のある騎手のみ対象
        min_rides = 50
        
        # 騎手ごとの成績を集計
        jockey_stats = races_df.groupby('騎手').agg(
            rides_count=('race_id', 'count'),
            win_count=('着順_数値', lambda x: sum(x == 1)),
            place_count=('着順_数値', lambda x: sum((x >= 1) & (x <= 3))),
            avg_rank=('着順_数値', 'mean')
        ).reset_index()
        
        # 勝率と連対率を計算
        jockey_stats['win_rate'] = jockey_stats['win_count'] / jockey_stats['rides_count'] * 100
        jockey_stats['place_rate'] = jockey_stats['place_count'] / jockey_stats['rides_count'] * 100
        
        # 十分な騎乗回数のある騎手のみをフィルタリング
        qualified_jockeys = jockey_stats[jockey_stats['rides_count'] >= min_rides]
        
        # 勝率上位の騎手
        top_jockeys = qualified_jockeys.sort_values('win_rate', ascending=False).head(20)
        print("\n勝率上位の騎手:")
        print(top_jockeys[['騎手', 'rides_count', 'win_count', 'win_rate']])
        
        # 勝率上位騎手をグラフ化
        plt.figure(figsize=(12, 8))
        sns.barplot(x='騎手', y='win_rate', data=top_jockeys)
        plt.title(f'勝率上位の騎手（最低{min_rides}回騎乗）')
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/top_jockeys.png')
        plt.close()
        
        # 騎手のコース別勝率
        if 'course_type' in races_df.columns:
            # 十分なデータがあるコース・騎手の組み合わせのみ対象
            min_course_rides = 30
            
            jockey_course_stats = races_df.groupby(['騎手', 'course_type']).agg(
                rides_count=('race_id', 'count'),
                win_count=('着順_数値', lambda x: sum(x == 1))
            ).reset_index()
            
            jockey_course_stats['win_rate'] = jockey_course_stats['win_count'] / jockey_course_stats['rides_count'] * 100
            
            # 十分な騎乗回数のある組み合わせのみをフィルタリング
            qualified_jockey_courses = jockey_course_stats[jockey_course_stats['rides_count'] >= min_course_rides]
            
            # 上位の騎手のみに絞る
            top_jockey_names = top_jockeys['騎手'].tolist()
            top_jockey_courses = qualified_jockey_courses[qualified_jockey_courses['騎手'].isin(top_jockey_names)]
            
            # コース別勝率をヒートマップで可視化
            if len(top_jockey_courses) > 0:
                pivot_data = top_jockey_courses.pivot(index='騎手', columns='course_type', values='win_rate')
                
                plt.figure(figsize=(10, 8))
                sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='YlGnBu')
                plt.title('騎手のコース別勝率(%)')
                plt.tight_layout()
                plt.savefig(f'{output_dir}/jockey_course_win_rates.png')
                plt.close()
        
        # 騎手の詳細な成績を保存
        qualified_jockeys.to_csv(f'{output_dir}/jockey_performance_stats.csv', index=False, encoding='utf-8-sig')
    
    # 調教師の成績分析（trainer列が存在する場合）
    if 'trainer' in races_df.columns and '着順_数値' in races_df.columns:
        # 十分な頭数のある調教師のみ対象
        min_horses = 30
        
        # 調教師ごとの成績を集計
        trainer_stats = races_df.groupby('trainer').agg(
            horses_count=('race_id', 'count'),
            win_count=('着順_数値', lambda x: sum(x == 1)),
            place_count=('着順_数値', lambda x: sum((x >= 1) & (x <= 3))),
            avg_rank=('着順_数値', 'mean')
        ).reset_index()
        
        # 勝率と連対率を計算
        trainer_stats['win_rate'] = trainer_stats['win_count'] / trainer_stats['horses_count'] * 100
        trainer_stats['place_rate'] = trainer_stats['place_count'] / trainer_stats['horses_count'] * 100
        
        # 十分な頭数のある調教師のみをフィルタリング
        qualified_trainers = trainer_stats[trainer_stats['horses_count'] >= min_horses]
        
        # 勝率上位の調教師
        top_trainers = qualified_trainers.sort_values('win_rate', ascending=False).head(20)
        print("\n勝率上位の調教師:")
        print(top_trainers[['trainer', 'horses_count', 'win_count', 'win_rate']])
        
        # 勝率上位調教師をグラフ化
        plt.figure(figsize=(12, 8))
        sns.barplot(x='trainer', y='win_rate', data=top_trainers)
        plt.title(f'勝率上位の調教師（最低{min_horses}頭）')
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/top_trainers.png')
        plt.close()
        
        # 調教師の詳細な成績を保存
        qualified_trainers.to_csv(f'{output_dir}/trainer_performance_stats.csv', index=False, encoding='utf-8-sig')
    
    print("=== 騎手・調教師のパフォーマンス分析が完了 ===")

def analyze_track_weather_season(races_df):
    """馬場・天候・季節の影響分析"""
    print("=== 馬場・天候・季節の影響分析を開始 ===")
    
    if races_df is None:
        print("レースデータがありません。")
        return
    
    # 馬場状態の影響分析
    if 'track_condition' in races_df.columns and 'タイム_秒' in races_df.columns:
        # コース種別ごとに馬場状態の影響を分析
        if 'course_type' in races_df.columns:
            plt.figure(figsize=(15, 10))
            
            course_types = races_df['course_type'].unique()
            
            for i, course in enumerate(course_types):
                if len(course_types) <= 3:
                    plt.subplot(len(course_types), 1, i+1)
                else:
                    # コースタイプが多い場合は2行に分ける
                    plt.subplot(2, (len(course_types) + 1) // 2, i+1)
                
                course_data = races_df[races_df['course_type'] == course]
                
                if len(course_data) > 0:  # データが存在する場合のみプロット
                    sns.boxplot(x='track_condition', y='タイム_秒', data=course_data)
                    
                    plt.title(f'{course}コースにおける馬場状態別のタイム分布')
                    plt.xlabel('馬場状態')
                    plt.ylabel('タイム (秒)')
                    plt.grid(True, linestyle='--', alpha=0.7)
            
            plt.tight_layout()
            plt.savefig(f'{output_dir}/track_condition_impact.png')
            plt.close()
            
            # 馬場状態別の詳細データを保存
            track_condition_stats = races_df.groupby(['course_type', 'track_condition']).agg({
                'タイム_秒': ['count', 'mean', 'std', 'min', 'max'],
                'race_id': 'nunique'
            }).reset_index()
            
            track_condition_stats.columns = ['コース種別', '馬場状態', 'サンプル数', '平均タイム', 'タイム標準偏差', '最速タイム', '最遅タイム', 'レース数']
            track_condition_stats.to_csv(f'{output_dir}/track_condition_stats.csv', index=False, encoding='utf-8-sig')
    
    # 天候の影響分析
    if 'weather' in races_df.columns and 'タイム_秒' in races_df.columns:
        # 天候別の平均タイム
        weather_times = races_df.groupby(['weather', 'distance'])['タイム_秒'].mean().reset_index()
        
        plt.figure(figsize=(12, 8))
        for weather in weather_times['weather'].unique():
            data = weather_times[weather_times['weather'] == weather]
            if len(data) > 0:  # データが存在する場合のみプロット
                plt.plot(data['distance'], data['タイム_秒'], 'o-', label=weather)
        
        plt.title('天候別の平均タイム（距離別）')
        plt.xlabel('距離 (m)')
        plt.ylabel('平均タイム (秒)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f'{output_dir}/weather_times.png')
        plt.close()
        
        # 天候別の詳細データを保存
        weather_stats = races_df.groupby(['weather']).agg({
            'タイム_秒': ['count', 'mean', 'std'],
            'race_id': 'nunique'
        }).reset_index()
        
        weather_stats.columns = ['天候', 'サンプル数', '平均タイム', 'タイム標準偏差', 'レース数']
        weather_stats.to_csv(f'{output_dir}/weather_stats.csv', index=False, encoding='utf-8-sig')
    
    # 季節の影響分析
    if 'race_date' in races_df.columns:
        # 日付型に変換
        if not pd.api.types.is_datetime64_dtype(races_df['race_date']):
            races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
        
        races_df['month'] = races_df['race_date'].dt.month
        
        # 月ごとのレース数
        monthly_races = races_df.groupby('month').size()
        
        plt.figure(figsize=(10, 6))
        monthly_races.plot(kind='bar')
        plt.title('月ごとのレース数')
        plt.xlabel('月')
        plt.ylabel('レース数')
        plt.xticks(range(12), ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'])
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/monthly_races.png')
        plt.close()
        
        # 月ごとの平均タイム（特定距離のレースのみ）
        if 'distance' in races_df.columns and 'タイム_秒' in races_df.columns:
            # 一般的な距離を選択（例: 1600m, 1800m, 2000m）
            common_distances = [1600, 1800, 2000]
            
            plt.figure(figsize=(15, 10))
            
            for i, dist in enumerate(common_distances):
                plt.subplot(len(common_distances), 1, i+1)
                
                dist_data = races_df[races_df['distance'] == dist]
                if len(dist_data) > 0:  # データが存在する場合のみプロット
                    monthly_times = dist_data.groupby('month')['タイム_秒'].mean()
                    
                    monthly_times.plot(kind='line', marker='o')
                    plt.title(f'{dist}mレースの月別平均タイム')
                    plt.xlabel('月')
                    plt.ylabel('平均タイム (秒)')
                    plt.xticks(range(1, 13))
                    plt.grid(True, linestyle='--', alpha=0.7)
            
            plt.tight_layout()
            plt.savefig(f'{output_dir}/monthly_average_times.png')
            plt.close()
            
            # 月別の詳細データを保存
            monthly_stats = races_df.groupby(['month']).agg({
                'タイム_秒': ['count', 'mean', 'std'],
                'race_id': 'nunique'
            }).reset_index()
            
            monthly_stats.columns = ['月', 'サンプル数', '平均タイム', 'タイム標準偏差', 'レース数']
            monthly_stats.to_csv(f'{output_dir}/monthly_stats.csv', index=False, encoding='utf-8-sig')
    
    print("=== 馬場・天候・季節の影響分析が完了 ===")

def main():
    """メイン実行関数"""
    import glob
    
    # 前処理済みデータの読み込み
    races_df, horse_info_df, horse_history_df = load_preprocessed_data()
    
    if races_df is None:
        print("レースデータが利用できないため、分析を中止します。")
        return
    
    # 1. レースデータの基本統計分析
    analyze_race_statistics(races_df)
    
    # 2. 馬のパフォーマンス分析
    analyze_horse_performance(races_df, horse_info_df)
    
    # 3. 騎手・調教師の分析
    analyze_jockey_trainer_performance(races_df)
    
    # 4. 馬場・天候・季節の影響分析
    analyze_track_weather_season(races_df)
    
    print(f"すべての分析結果は {output_dir} ディレクトリに保存されました。")

if __name__ == "__main__":
    main()
