import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
import json
import os
import glob
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse
import re

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='horse_scraping.log'
)
logger = logging.getLogger(__name__)

# 出力ディレクトリ
OUTPUT_DIR = 'horse_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# デバッグ用ディレクトリ
HORSE_DEBUG_DIR = f"{OUTPUT_DIR}/debug_html"
os.makedirs(HORSE_DEBUG_DIR, exist_ok=True)

# セッション管理とリトライ処理の設定
def create_session():
    """リトライ機能付きのセッションを作成"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # より詳細なヘッダー設定
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Referer': 'https://www.netkeiba.com/'
    })
    
    return session

# 馬の基本情報を取得
def scrape_horse_info(horse_id, session=None):
    """馬の基本情報をスクレイピングする関数"""
    if session is None:
        session = create_session()
    
    url = f"https://db.netkeiba.com/horse/{horse_id}"
    logger.info(f"Requesting horse info: {url}")
    
    try:
        response = session.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error: Status code {response.status_code} for {url}")
            return None
        
        # 明示的にデコードして処理
        html_content = response.content.decode("euc-jp", "ignore")
        
        # デバッグ用にHTMLを保存
        with open(f"{HORSE_DEBUG_DIR}/horse_{horse_id}.html", 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 馬の基本情報を抽出
        horse_info = {'horse_id': horse_id}
        
        # 馬名
        name_tag = soup.select_one('div.horse_title h1')
        if name_tag:
            horse_info['name'] = name_tag.text.strip()
        else:
            # 代替セレクタを試す
            alt_selectors = ['h1.tit', '#horse_title h1', '.horse_name']
            for selector in alt_selectors:
                name_elem = soup.select_one(selector)
                if name_elem:
                    horse_info['name'] = name_elem.text.strip()
                    break
        
        # プロフィールテーブルから情報抽出（複数のセレクタを試す）
        profile_tables = []
        selectors = [
            'div.db_prof_table_01',
            'div.db_prof_box',
            'div.horse_profile',
            'table.db_prof_table'
        ]
        
        for selector in selectors:
            tables = soup.select(f'{selector} table')
            if tables:
                profile_tables.extend(tables)
        
        if not profile_tables:
            # すべてのテーブルを検索
            all_tables = soup.find_all('table')
            for table in all_tables:
                if any(keyword in table.text for keyword in ['生年月日', '調教師', '馬主', '生産者']):
                    profile_tables.append(table)
        
        for table in profile_tables:
            rows = table.select('tr')
            for row in rows:
                cells = row.select('th, td')
                if len(cells) >= 2:
                    header = cells[0].text.strip()
                    value = cells[1].text.strip()
                    
                    if '生年月日' in header:
                        horse_info['birth_date'] = value
                    elif '調教師' in header:
                        horse_info['trainer'] = value
                    elif '馬主' in header:
                        horse_info['owner'] = value
                    elif '生産者' in header:
                        horse_info['breeder'] = value
                    elif '産地' in header:
                        horse_info['origin'] = value
                    elif '毛色' in header:
                        horse_info['color'] = value
                    elif '性別' in header or '性' in header:
                        horse_info['sex'] = value
                    elif '父' in header and '母' not in header:
                        horse_info['father'] = value
                    elif '母' in header and '父' not in header:
                        horse_info['mother'] = value
                    elif '母父' in header:
                        horse_info['maternal_grandfather'] = value
        
        # 血統情報
        blood_table = soup.select_one('table.blood_table, table.pedigree_table')
        if blood_table:
            # 3代血統表の解析（簡易版）
            horse_info['pedigree'] = {}
            blood_cells = blood_table.select('td')
            
            # 父方の祖父
            if len(blood_cells) > 0:
                horse_info['pedigree']['paternal_grandfather'] = blood_cells[0].text.strip()
            
            # 父方の祖母
            if len(blood_cells) > 2:
                horse_info['pedigree']['paternal_grandmother'] = blood_cells[2].text.strip()
            
            # 母方の祖父
            if len(blood_cells) > 8:
                horse_info['pedigree']['maternal_grandfather'] = blood_cells[8].text.strip()
            
            # 母方の祖母
            if len(blood_cells) > 10:
                horse_info['pedigree']['maternal_grandmother'] = blood_cells[10].text.strip()
        
        # 獲得賞金と成績情報（複数のセレクタを試す）
        performance_selectors = [
            'div.db_prof_area_02 table',
            'div.horse_performance table',
            'div.db_prof_box table'
        ]
        
        for selector in performance_selectors:
            tables = soup.select(selector)
            for table in tables:
                rows = table.select('tr')
                for row in rows:
                    cells = row.select('th, td')
                    if len(cells) >= 2:
                        header = cells[0].text.strip()
                        value = cells[1].text.strip()
                        
                        if '獲得賞金' in header or '収得賞金' in header:
                            horse_info['prize_money_text'] = value
                            # 獲得賞金を数値化（例: "5億4,321万円" → 543210000）
                            if '円' in value:
                                prize_text = value.replace('円', '')
                                try:
                                    if '億' in prize_text:
                                        parts = prize_text.split('億')
                                        oku = float(parts[0]) * 100000000
                                        if '万' in parts[1]:
                                            man_parts = parts[1].split('万')
                                            man = float(man_parts[0].replace(',', '')) * 10000
                                        else:
                                            man = 0
                                        horse_info['prize_money'] = oku + man
                                    elif '万' in prize_text:
                                        man = float(prize_text.split('万')[0].replace(',', '')) * 10000
                                        horse_info['prize_money'] = man
                                except:
                                    pass
                        
                        elif '通算成績' in header or '競走成績' in header:
                            horse_info['career_summary'] = value
                            
                            # レース数・勝利数などを抽出
                            match = re.search(r'(\d+)戦(\d+)勝', value)
                            if match:
                                horse_info['total_races'] = int(match.group(1))
                                horse_info['total_wins'] = int(match.group(2))
        
        if not horse_info.get('name'):
            logger.warning(f"Could not find basic information for horse {horse_id}")
            return None
            
        logger.info(f"Successfully collected info for horse {horse_info.get('name', horse_id)}")
        return horse_info
        
    except Exception as e:
        logger.error(f"Exception while scraping horse info {url}: {str(e)}")
        return None

# 馬の出走履歴を取得
def scrape_horse_history(horse_id, session=None):
    """馬の出走履歴をスクレイピングする関数"""
    if session is None:
        session = create_session()
    
    url = f"https://db.netkeiba.com/horse/{horse_id}/result/"
    logger.info(f"Requesting horse history: {url}")
    
    try:
        response = session.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error: Status code {response.status_code} for {url}")
            return None
        
        # 明示的にデコードして処理
        html_content = response.content.decode("euc-jp", "ignore")
        
        # デバッグ用にHTMLを保存
        with open(f"{HORSE_DEBUG_DIR}/horse_history_{horse_id}.html", 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 出走履歴テーブル（複数のセレクタを試す）
        history_table = None
        selectors = [
            'table.nk_tb_common.race_table_01',
            'table.race_table_01',
            'table.db_h_race_results',
            'div.horse_result table'
        ]
        
        for selector in selectors:
            table = soup.select_one(selector)
            if table:
                history_table = table
                break
        
        if not history_table:
            # すべてのテーブルを検索して正しいものを特定
            all_tables = soup.find_all('table')
            for table in all_tables:
                headers = [th.text.strip() for th in table.find_all('th')]
                if '日付' in ' '.join(headers) and '馬場' in ' '.join(headers):
                    history_table = table
                    break
        
        if not history_table:
            logger.warning(f"No history table found for horse {horse_id}")
            return None
        
        # pandasでテーブルを解析
        try:
            dfs = pd.read_html(str(history_table))
            if dfs:
                df = dfs[0]
                # 列名をクリーンアップ
                df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
                df['horse_id'] = horse_id
                
                # レースIDを抽出
                race_links = history_table.select('a[href*="/race/"]')
                race_ids = []
                
                for link in race_links:
                    href = link.get('href', '')
                    if '/race/' in href:
                        try:
                            race_id = href.split('/race/')[1].rstrip('/')
                            race_ids.append(race_id)
                        except:
                            race_ids.append(None)
                
                # レースIDをデータフレームに追加
                if race_ids and len(race_ids) == len(df):
                    df['race_id'] = race_ids
                
                logger.info(f"Successfully collected race history for horse {horse_id}: {len(df)} races")
                return df
            else:
                logger.warning(f"Failed to parse history table for horse {horse_id}")
        except Exception as e:
            logger.error(f"Error parsing history table for horse {horse_id}: {str(e)}")
        
        return None
        
    except Exception as e:
        logger.error(f"Exception while scraping horse history {url}: {str(e)}")
        return None

# 馬のトレーニング情報を取得
def scrape_horse_training(horse_id, session=None):
    """馬の調教情報をスクレイピングする関数"""
    if session is None:
        session = create_session()
    
    # 複数の調教情報URLを試す
    urls = [
        f"https://db.netkeiba.com/horse/{horse_id}/oikiri/",
        f"https://db.netkeiba.com/horse/{horse_id}/training/"
    ]
    
    for url in urls:
        logger.info(f"Requesting horse training: {url}")
        
        try:
            response = session.get(url)
            
            if response.status_code != 200:
                logger.error(f"Error: Status code {response.status_code} for {url}")
                continue
            
            # 明示的にデコードして処理
            html_content = response.content.decode("euc-jp", "ignore")
            
            # デバッグ用にHTMLを保存
            with open(f"{HORSE_DEBUG_DIR}/horse_training_{horse_id}_{url.split('/')[-2]}.html", 'w', encoding='utf-8') as f:
                f.write(html_content)
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 調教テーブル（複数のセレクタを試す）
            training_table = None
            selectors = [
                'table.nk_tb_common.race_table_01',
                'table.training_table',
                'div.horse_training table'
            ]
            
            for selector in selectors:
                table = soup.select_one(selector)
                if table:
                    training_table = table
                    break
            
            if not training_table:
                logger.warning(f"No training table found for horse {horse_id} at {url}")
                continue
            
            # pandasでテーブルを解析
            try:
                dfs = pd.read_html(str(training_table))
                if dfs:
                    df = dfs[0]
                    # 列名をクリーンアップ
                    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
                    df['horse_id'] = horse_id
                    logger.info(f"Successfully collected training data for horse {horse_id}")
                    return df
                else:
                    logger.warning(f"Failed to parse training table for horse {horse_id}")
            except Exception as e:
                logger.error(f"Error parsing training table for horse {horse_id}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Exception while scraping horse training {url}: {str(e)}")
    
    return None

# 既存の馬情報をロードする関数
def load_existing_horse_ids():
    """
    horse_data/horse_info_*.csv ファイルから既に取得済みの馬IDをロードする
    
    Returns:
        set: 取得済みの馬IDのセット
    """
    existing_horse_ids = set()
    
    # horse_info_*.csvファイルを検索
    info_files = glob.glob(os.path.join(OUTPUT_DIR, "horse_info_*.csv"))
    
    for file in info_files:
        try:
            df = pd.read_csv(file, encoding='utf-8-sig')
            if 'horse_id' in df.columns:
                horse_ids = df['horse_id'].astype(str).tolist()
                existing_horse_ids.update(horse_ids)
                logger.info(f"Loaded {len(horse_ids)} existing horse IDs from {file}")
        except Exception as e:
            logger.error(f"Error loading existing horse IDs from {file}: {str(e)}")
    
    logger.info(f"Total existing horse IDs loaded: {len(existing_horse_ids)}")
    return existing_horse_ids

# 複数の馬情報を収集
def scrape_multiple_horses(horse_ids, include_training=False, batch_size=3, pause_between_batches=45, max_retries=2, skip_existing=False):
    """
    複数の馬の情報を収集する関数
    
    Args:
        horse_ids: 収集対象の馬IDリスト
        include_training: 調教情報も収集するか
        batch_size: バッチサイズ
        pause_between_batches: バッチ間の待機時間（秒）
        max_retries: 最大リトライ回数
        skip_existing: 既存の馬データをスキップするか
    """
    all_horse_info = []
    all_horse_history = []
    all_horse_training = []
    session = create_session()
    
    # 処理済みの馬IDを記録
    processed_horses = set()
    
    # 既存の馬データをロード（オプション）
    existing_horse_ids = set()
    if skip_existing:
        existing_horse_ids = load_existing_horse_ids()
        logger.info(f"Will skip {len(existing_horse_ids)} existing horses")
    
    # スキップされた馬のカウント
    skipped_count = 0
    
    # バッチ処理
    for i in range(0, len(horse_ids), batch_size):
        batch = horse_ids[i:i+batch_size]
        logger.info(f"Processing horse batch {i//batch_size + 1}/{(len(horse_ids) + batch_size - 1)//batch_size}")
        
        for j, horse_id in enumerate(batch):
            # 既に処理済みの馬またはDBに存在する馬をスキップ
            if horse_id in processed_horses or (skip_existing and horse_id in existing_horse_ids):
                if horse_id in existing_horse_ids:
                    logger.info(f"Skipping existing horse in database {j+1}/{len(batch)}: {horse_id}")
                    skipped_count += 1
                else:
                    logger.info(f"Skipping already processed horse {j+1}/{len(batch)}: {horse_id}")
                continue
                
            logger.info(f"Scraping horse {j+1}/{len(batch)}: {horse_id}")
            
            # 馬の基本情報（リトライあり）
            horse_info = None
            retries = 0
            while horse_info is None and retries < max_retries:
                if retries > 0:
                    logger.info(f"Retrying horse info for {horse_id} (attempt {retries+1})")
                    # リトライの前に少し長めに待機
                    time.sleep(random.uniform(7, 15))
                
                horse_info = scrape_horse_info(horse_id, session)
                retries += 1
            
            if horse_info:
                all_horse_info.append(horse_info)
            
            # サーバー負荷軽減
            time.sleep(random.uniform(5, 10))
            
            # 馬の出走履歴（リトライあり）
            horse_history = None
            retries = 0
            while horse_history is None and retries < max_retries:
                if retries > 0:
                    logger.info(f"Retrying horse history for {horse_id} (attempt {retries+1})")
                    time.sleep(random.uniform(7, 15))
                
                horse_history = scrape_horse_history(horse_id, session)
                retries += 1
            
            if horse_history is not None:
                all_horse_history.append(horse_history)
            
            # サーバー負荷軽減
            time.sleep(random.uniform(5, 10))
            
            # 調教情報（オプション）
            if include_training:
                horse_training = scrape_horse_training(horse_id, session)
                if horse_training is not None:
                    all_horse_training.append(horse_training)
                
                # サーバー負荷軽減
                time.sleep(random.uniform(5, 10))
            
            # 処理済みとしてマーク
            processed_horses.add(horse_id)
        
        # 中間結果の保存
        if (i + batch_size) % (batch_size * 3) == 0:
            save_intermediate_horse_results(all_horse_info, all_horse_history, all_horse_training, i)
        
        # バッチ間の待機（より長めに）
        if i + batch_size < len(horse_ids):
            logger.info(f"Pausing for {pause_between_batches} seconds between batches")
            time.sleep(pause_between_batches)
    
    # スキップされた馬の数を表示
    if skip_existing:
        logger.info(f"Skipped {skipped_count} horses that already exist in the database")
    
    # 最終結果の保存
    return save_horse_results(all_horse_info, all_horse_history, all_horse_training)

# 中間の馬データ保存
def save_intermediate_horse_results(horse_info, horse_history, horse_training, batch_index):
    """馬データの中間結果を保存"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if horse_info:
        try:
            # 馬の基本情報をJSONとして保存
            with open(f"{OUTPUT_DIR}/intermediate_horse_info_{timestamp}_{batch_index}.json", 'w', encoding='utf-8') as f:
                json.dump(horse_info, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved intermediate horse info to intermediate_horse_info_{timestamp}_{batch_index}.json")
        except Exception as e:
            logger.error(f"Failed to save intermediate horse info: {str(e)}")
    
    if horse_history:
        try:
            # 馬の出走履歴をCSVとして保存
            combined_history = pd.concat(horse_history, ignore_index=True)
            combined_history.to_csv(f"{OUTPUT_DIR}/intermediate_horse_history_{timestamp}_{batch_index}.csv", 
                                   index=False, encoding='utf-8-sig')
            logger.info(f"Saved intermediate horse history to intermediate_horse_history_{timestamp}_{batch_index}.csv")
        except Exception as e:
            logger.error(f"Failed to save intermediate horse history: {str(e)}")
    
    if horse_training:
        try:
            # 馬の調教データをCSVとして保存
            combined_training = pd.concat(horse_training, ignore_index=True)
            combined_training.to_csv(f"{OUTPUT_DIR}/intermediate_horse_training_{timestamp}_{batch_index}.csv", 
                                    index=False, encoding='utf-8-sig')
            logger.info(f"Saved intermediate horse training to intermediate_horse_training_{timestamp}_{batch_index}.csv")
        except Exception as e:
            logger.error(f"Failed to save intermediate horse training: {str(e)}")

# 最終的な馬データ保存
def save_horse_results(horse_info, horse_history, horse_training):
    """最終的な馬データを保存"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    horse_info_df = None
    if horse_info:
        try:
            # 血統情報は別途処理（ネストしたデータ構造のため）
            processed_horse_info = []
            for horse in horse_info:
                horse_copy = horse.copy()
                if 'pedigree' in horse_copy:
                    for key, value in horse_copy['pedigree'].items():
                        horse_copy[key] = value
                    del horse_copy['pedigree']
                processed_horse_info.append(horse_copy)
            
            # CSVとJSONの両方で保存
            horse_info_df = pd.DataFrame(processed_horse_info)
            horse_info_df.to_csv(f"{OUTPUT_DIR}/horse_info_{timestamp}.csv", index=False, encoding='utf-8-sig')
            
            with open(f"{OUTPUT_DIR}/horse_info_{timestamp}.json", 'w', encoding='utf-8') as f:
                json.dump(horse_info, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved horse info to horse_info_{timestamp}.csv and .json")
        except Exception as e:
            logger.error(f"Failed to save horse info: {str(e)}")
    
    horse_history_df = None
    if horse_history:
        try:
            # 馬の出走履歴をCSVとして保存
            horse_history_df = pd.concat(horse_history, ignore_index=True)
            horse_history_df.to_csv(f"{OUTPUT_DIR}/horse_history_{timestamp}.csv", index=False, encoding='utf-8-sig')
            logger.info(f"Saved horse history to horse_history_{timestamp}.csv")
        except Exception as e:
            logger.error(f"Failed to save horse history: {str(e)}")
    
    horse_training_df = None
    if horse_training:
        try:
            # 馬の調教データをCSVとして保存
            horse_training_df = pd.concat(horse_training, ignore_index=True)
            horse_training_df.to_csv(f"{OUTPUT_DIR}/horse_training_{timestamp}.csv", index=False, encoding='utf-8-sig')
            logger.info(f"Saved horse training to horse_training_{timestamp}.csv")
        except Exception as e:
            logger.error(f"Failed to save horse training: {str(e)}")
    
    return horse_info_df, horse_history_df, horse_training_df

# CSVからの馬ID抽出（改良版）
def extract_horse_ids_from_file(file_path):
    """ファイルから馬IDを抽出する関数（複数のフォーマットに対応）"""
    try:
        if file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # JSONの形式に応じて処理
                if isinstance(data, list):
                    # リスト形式の場合
                    if all(isinstance(item, str) for item in data):
                        # 文字列のリスト
                        horse_ids = data
                    elif all(isinstance(item, dict) for item in data):
                        # 辞書のリスト
                        if 'horse_id' in data[0]:
                            horse_ids = [item['horse_id'] for item in data if 'horse_id' in item]
                        else:
                            # キーを推測
                            potential_keys = ['id', 'horse_id', 'horseid', 'horse']
                            for key in potential_keys:
                                if key in data[0]:
                                    horse_ids = [item[key] for item in data if key in item]
                                    break
                            else:
                                horse_ids = []
                else:
                    # 辞書形式の場合
                    horse_ids = []
            
            logger.info(f"Extracted {len(horse_ids)} horse IDs from JSON file {file_path}")
            return horse_ids
            
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            
            # 馬IDの列を探す
            id_columns = ['horse_id', 'horseid', 'id', 'horse']
            for col in id_columns:
                if col in df.columns:
                    horse_ids = df[col].dropna().astype(str).unique().tolist()
                    logger.info(f"Extracted {len(horse_ids)} horse IDs from column '{col}' in CSV file {file_path}")
                    return horse_ids
            
            # 明示的な列名がない場合はすべての列をチェック
            for col in df.columns:
                if 'id' in col.lower() or 'horse' in col.lower():
                    values = df[col].dropna().astype(str).tolist()
                    # 典型的な馬IDのパターン（数字8-10桁）
                    potential_ids = [val for val in values if re.match(r'^\d{8,10}$', val)]
                    if potential_ids:
                        logger.info(f"Extracted {len(potential_ids)} potential horse IDs from column '{col}' in CSV file {file_path}")
                        return potential_ids
            
            logger.error(f"No suitable horse_id column found in {file_path}")
            return []
            
        else:
            # テキストファイルとして処理
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                horse_ids = [line.strip() for line in lines if line.strip()]
            
            logger.info(f"Loaded {len(horse_ids)} horse IDs from text file {file_path}")
            return horse_ids
            
    except Exception as e:
        logger.error(f"Error extracting horse IDs from {file_path}: {str(e)}")
        return []

# 最近の活躍馬を収集（改良版）
def collect_recent_active_horses(years=[2022, 2023], session=None):
    """最近の活躍馬のIDを収集する関数（改良版）"""
    if session is None:
        session = create_session()
    
    horse_ids = set()
    
    # 方法1: 各年の重賞レース勝ち馬を収集
    for year in years:
        url = f"https://db.netkeiba.com/?pid=jra_grade_race&year={year}"
        logger.info(f"Fetching grade race winners for {year}: {url}")
        
        try:
            response = session.get(url)
            
            if response.status_code == 200:
                # 明示的にデコードして処理
                html_content = response.content.decode("euc-jp", "ignore")
                
                # デバッグ用にHTMLを保存
                with open(f"{HORSE_DEBUG_DIR}/grade_races_{year}.html", 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # 勝ち馬のリンクを抽出
                winner_links = soup.select('td.win a[href*="/horse/"]')
                
                for link in winner_links:
                    href = link.get('href', '')
                    if '/horse/' in href:
                        try:
                            horse_id = href.split('/horse/')[1].rstrip('/')
                            horse_ids.add(horse_id)
                        except:
                            continue
                
                logger.info(f"Collected {len(winner_links)} grade race winners from {year}")
            else:
                logger.error(f"Failed to get grade race winners for {year}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error collecting grade race winners for {year}: {str(e)}")
        
        time.sleep(random.uniform(5, 10))
    
    # 方法2: 人気馬ランキングページからも収集
    try:
        ranking_urls = [
            "https://db.netkeiba.com/ranking/",
            "https://db.netkeiba.com/ranking/?page=2"
        ]
        
        for url in ranking_urls:
            logger.info(f"Fetching popular horses from: {url}")
            
            response = session.get(url)
            
            if response.status_code == 200:
                # 明示的にデコードして処理
                html_content = response.content.decode("euc-jp", "ignore")
                
                # デバッグ用にHTMLを保存
                with open(f"{HORSE_DEBUG_DIR}/ranking_{url.split('=')[-1] if '=' in url else 'main'}.html", 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # 馬のリンクを抽出
                horse_links = soup.select('a[href*="/horse/"]')
                
                for link in horse_links:
                    href = link.get('href', '')
                    if '/horse/' in href:
                        try:
                            horse_id = href.split('/horse/')[1].rstrip('/')
                            # 数値のみの8-10桁のIDのみを対象
                            if re.match(r'^\d{8,10}$', horse_id):
                                horse_ids.add(horse_id)
                        except:
                            continue
                
                logger.info(f"Collected {len(horse_links)} horses from ranking page: {url}")
            else:
                logger.error(f"Failed to get ranking page: {response.status_code}")
                
            time.sleep(random.uniform(5, 10))
    except Exception as e:
        logger.error(f"Error collecting horses from ranking pages: {str(e)}")
    
    # 結果を返す
    horse_ids_list = list(horse_ids)
    logger.info(f"Total unique horses collected: {len(horse_ids_list)}")
    return horse_ids_list

# コマンドライン引数の解析
def parse_args():
    parser = argparse.ArgumentParser(description='Netkeiba Horse Data Scraper (Improved)')
    parser.add_argument('--source', choices=['file', 'recent', 'manual'], default='recent',
                        help='Source of horse IDs: file (from file), recent (active horses), or manual (command line list)')
    parser.add_argument('--file', type=str,
                        help='File path containing horse IDs (required if source is "file")')
    parser.add_argument('--years', type=int, nargs='+', default=[2022, 2023],
                        help='Years to collect recent active horses (used if source is "recent")')
    parser.add_argument('--horse_ids', type=str, nargs='+',
                        help='List of horse IDs to collect (required if source is "manual")')
    parser.add_argument('--include_training', action='store_true',
                        help='Include training data in collection')
    parser.add_argument('--batch_size', type=int, default=3,
                        help='Number of horses to process in a batch')
    parser.add_argument('--pause', type=int, default=45,
                        help='Pause time between batches in seconds')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit number of horses to collect (0 for all)')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Skip horses that already exist in horse_data/horse_info_*.csv files')
    
    return parser.parse_args()

# メイン実行関数
def main():
    args = parse_args()
    horse_ids = []
    
    # 馬IDの収集元
    if args.source == 'file':
        if not args.file:
            print("Error: --file parameter is required when source is 'file'")
            return
        horse_ids = extract_horse_ids_from_file(args.file)
    elif args.source == 'recent':
        print(f"Collecting IDs of recently active horses for years: {args.years}")
        horse_ids = collect_recent_active_horses(years=args.years)
    elif args.source == 'manual':
        if not args.horse_ids:
            print("Error: --horse_ids parameter is required when source is 'manual'")
            return
        horse_ids = args.horse_ids
    
    # 馬IDが取得できない場合は終了
    if not horse_ids:
        print("Error: No horse IDs found from the specified source")
        return
    
    # 指定された上限で絞り込み
    if args.limit > 0 and len(horse_ids) > args.limit:
        print(f"Limiting collection to {args.limit} horses from {len(horse_ids)} total")
        horse_ids = horse_ids[:args.limit]
    
    print(f"Starting data collection for {len(horse_ids)} horses")
    print(f"Settings: batch_size={args.batch_size}, pause={args.pause}s, include_training={args.include_training}, skip_existing={args.skip_existing}")
    
    # 馬情報の収集
    horse_info_df, horse_history_df, horse_training_df = scrape_multiple_horses(
        horse_ids, 
        include_training=args.include_training,
        batch_size=args.batch_size, 
        pause_between_batches=args.pause,
        skip_existing=args.skip_existing
    )
    
    if horse_info_df is not None:
        print(f"Successfully collected data for {len(horse_info_df)} horses")
        if horse_history_df is not None:
            total_races = len(horse_history_df)
            print(f"Total race histories collected: {total_races} entries")
        if horse_training_df is not None:
            total_training = len(horse_training_df)
            print(f"Total training data collected: {total_training} entries")
    else:
        print("Failed to collect horse data")
    
    print("Data collection completed!")

if __name__ == "__main__":
    main()
