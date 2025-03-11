import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
import json
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse
import re

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='direct_race_scraping.log'
)
logger = logging.getLogger(__name__)

# 出力ディレクトリ
OUTPUT_DIR = 'keiba_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# デバッグ用ディレクトリ
DEBUG_DIR = f"{OUTPUT_DIR}/debug_html"
os.makedirs(DEBUG_DIR, exist_ok=True)

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
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    })
    
    return session

# 場所コードと名前のマッピング
PLACE_DICT = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟',
    '05': '東京', '06': '中山', '07': '中京', '08': '京都',
    '09': '阪神', '10': '小倉'
}

# レースIDを生成してループする
def generate_race_ids(year, places=None):
    """
    指定された条件に基づいてレースIDを生成する
    
    Args:
        year: 対象年
        places: 対象競馬場コードのリスト（Noneの場合はすべての競馬場）
    
    Yields:
        str: 生成されたレースID
    """
    year_str = str(year)
    
    if places is None:
        places = list(PLACE_DICT.keys())  # すべての競馬場
    
    logger.info(f"Generating race IDs for {year} with places: {', '.join([PLACE_DICT.get(p, p) for p in places])}")
    
    for place_code in places:
        # 各開催回（1~6回）
        for kai in range(1, 7):
            # 各開催日（1~12日）
            for kaisai_day in range(1, 13):
                # 各レース（1~12R）
                for race_num in range(1, 13):
                    race_id = f"{year_str}{place_code}{str(kai).zfill(2)}{str(kaisai_day).zfill(2)}{str(race_num).zfill(2)}"
                    yield race_id

# レースの有効性をチェック
def is_valid_race(race_id, session=None):
    """
    レースIDが有効かどうかをチェックする
    
    Args:
        race_id: チェックするレースID
        session: リクエストセッション
    
    Returns:
        bool: レースが存在する場合はTrue
    """
    if session is None:
        session = create_session()
    
    url = f"https://db.netkeiba.com/race/{race_id}"
    
    try:
        # HEADリクエストで存在確認（高速）
        response = session.head(url, allow_redirects=False)
        
        # ステータスコード200はページが存在することを示す
        if response.status_code == 200:
            return True
        
        # 念のためGETリクエストで詳細確認
        if response.status_code in [301, 302]:
            response = session.get(url)
            html_content = response.content.decode("euc-jp", "ignore")
            
            # レースが存在しない場合のメッセージをチェック
            if "レース情報がありません" in html_content or "存在しないレースID" in html_content:
                return False
                
            # レース結果テーブルの存在を確認
            soup = BeautifulSoup(html_content, 'html.parser')
            race_table = soup.select_one('table.race_table_01')
            
            return race_table is not None
            
        return False
    except Exception as e:
        logger.error(f"Error checking race {race_id}: {str(e)}")
        return False

# レース結果ページをスクレイピング
def scrape_race_results(race_id, session=None):
    """レース結果をスクレイピングする関数"""
    if session is None:
        session = create_session()
    
    url = f"https://db.netkeiba.com/race/{race_id}"
    logger.info(f"Requesting: {url}")
    
    try:
        response = session.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error: Status code {response.status_code} for {url}")
            return None, {}
        
        # 明示的にデコードして処理
        html_content = response.content.decode("euc-jp", "ignore")
        
        # レスポンスのHTMLをファイルに保存（デバッグ用）
        with open(f"{DEBUG_DIR}/race_{race_id}.html", 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # レースの基本情報を取得
        race_info = extract_race_info(soup, race_id)
        
        # レース結果テーブルを取得（複数のセレクタを試す）
        table = None
        selectors = [
            'table.race_table_01',
            'table.Shutuba_table',
            'div.race_result_table table',
            '#contents_liquid table'
        ]
        
        for selector in selectors:
            table = soup.select_one(selector)
            if table:
                logger.info(f"Found race table with selector: {selector}")
                break
        
        if not table:
            # HTMLを詳細に分析してテーブルを探す
            all_tables = soup.find_all('table')
            if all_tables:
                logger.info(f"Found {len(all_tables)} tables on the page, trying to identify the right one")
                # テーブルの構造を確認し、レース結果テーブルを特定
                for i, tbl in enumerate(all_tables):
                    headers = [th.text.strip() for th in tbl.find_all('th')]
                    if '着順' in ' '.join(headers) or '馬名' in ' '.join(headers):
                        table = tbl
                        logger.info(f"Identified race table by headers")
                        break
            
            if not table:
                logger.warning(f"No race table found for {race_id}")
                return None, race_info
        
        # pandasでテーブルを解析
        try:
            dfs = pd.read_html(str(table))
            if dfs:
                # 最初のテーブルを取得
                df = dfs[0]
                
                # 列名をクリーンアップ（スペースの除去）
                df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
                
                # 結果を検証
                if '馬名' not in df.columns and '着順' not in df.columns:
                    logger.warning(f"The table doesn't seem to be a race result table: {df.columns}")
                    return None, race_info
                
                # レースIDを列として追加
                df['race_id'] = race_id
                
                # 馬IDを抽出
                horse_ids = extract_horse_ids(soup)
                if horse_ids and len(horse_ids) == len(df):
                    df['horse_id'] = horse_ids
                
                # 通過順、体重、体重変化、上がり、人気を追加
                passage_orders, weights, weight_diffs, last_3f, popularities = extract_horse_details(soup, len(df))
                
                if passage_orders and len(passage_orders) == len(df):
                    df['通過順'] = passage_orders
                
                if weights and len(weights) == len(df):
                    df['体重'] = weights
                
                if weight_diffs and len(weight_diffs) == len(df):
                    df['体重変化'] = weight_diffs
                
                if last_3f and len(last_3f) == len(df):
                    df['上がり'] = last_3f
                
                if popularities and len(popularities) == len(df):
                    df['人気'] = popularities
                
                # レース基本情報をデータフレームの各行に追加
                for key, value in race_info.items():
                    df[key] = value
                
                # レースIDから基本情報を追加
                try:
                    # レースIDから情報抽出
                    place_code = race_id[4:6]
                    kai = int(race_id[6:8])
                    day = int(race_id[8:10])
                    race_num = int(race_id[10:12])
                    
                    # 場所名
                    place_name = PLACE_DICT.get(place_code, '不明')
                    
                    df['place_code'] = place_code
                    df['place_name'] = place_name
                    df['kai'] = kai
                    df['day'] = day
                    df['race_number'] = race_num
                except:
                    pass
                
                # 天候と馬場状態が明示的に含まれていることを確認
                if 'weather' in race_info:
                    df['weather'] = race_info['weather']
                if 'track_condition' in race_info:
                    df['track_condition'] = race_info['track_condition']
                
                # データフレームに天候と馬場情報が確実に含まれるようにする（最終チェック）
                if 'weather' not in df.columns and 'weather' in race_info:
                    df['weather'] = race_info['weather']
                if 'track_condition' not in df.columns and 'track_condition' in race_info:
                    df['track_condition'] = race_info['track_condition']
                
                logger.info(f"Successfully scraped race {race_id}")
                return df, race_info
            else:
                logger.warning(f"No data found in race table for {race_id}")
        except Exception as e:
            logger.error(f"Error parsing table for {race_id}: {str(e)}")
        
        return None, race_info
        
    except Exception as e:
        logger.error(f"Exception while scraping {url}: {str(e)}")
        return None, {}

# 馬の詳細情報（通過順、体重、上がり、人気）を抽出
def extract_horse_details(soup, expected_horses):
    """レース結果ページから馬の詳細情報を抽出"""
    try:
        passage_orders = []
        weights = []
        weight_diffs = []
        last_3f = []
        popularities = []
        
        # 通過順の抽出
        nowrap_cells = soup.find_all("td", nowrap="nowrap", class_=None)
        for i in range(0, len(nowrap_cells), 3):  # 3つごとに処理
            if i+1 < len(nowrap_cells):
                # 通過順
                try:
                    passage_order = nowrap_cells[i].get_text(strip=True)
                    passage_orders.append(passage_order)
                except:
                    passage_orders.append('')
                
                # 体重と体重変化
                try:
                    weight_text = nowrap_cells[i+1].get_text(strip=True)
                    if '(' in weight_text and ')' in weight_text:
                        weight = int(weight_text.split('(')[0])
                        weight_diff = int(weight_text.split('(')[1].split(')')[0])
                    else:
                        weight = int(weight_text) if weight_text.isdigit() else 0
                        weight_diff = 0
                        
                    weights.append(weight)
                    weight_diffs.append(weight_diff)
                except:
                    weights.append(0)
                    weight_diffs.append(0)
        
        # 上がりタイムと人気の抽出
        txt_c_cells = soup.find_all("td", nowrap="nowrap", class_="txt_c")
        
        # 上がりタイム（3F）
        last_3f_cells = [cell for cell in txt_c_cells if cell.find('span', class_='F03')]
        for cell in last_3f_cells:
            try:
                last_time = cell.find('span', class_='F03').get_text(strip=True)
                last_3f.append(last_time)
            except:
                last_3f.append('')
        
        # 人気
        popularity_spans = soup.find_all("span", class_="Popularity")
        for span in popularity_spans:
            try:
                popularity = span.get_text(strip=True)
                popularities.append(popularity)
            except:
                popularities.append('')
        
        # 別の方法でも試す場合
        if not popularity_spans:
            try:
                pop_index = -1
                for i, col in enumerate(soup.find_all('th')):
                    if '人気' in col.get_text():
                        pop_index = i
                        break
                
                if pop_index >= 0:
                    rows = soup.find('table', class_='race_table_01').find_all('tr')[1:]  # ヘッダー行をスキップ
                    popularities = []
                    for row in rows:
                        cells = row.find_all('td')
                        if pop_index < len(cells):
                            popularities.append(cells[pop_index].get_text(strip=True))
                        else:
                            popularities.append('')
            except:
                pass
        
        # 結果のリストサイズをチェックして調整
        while len(passage_orders) < expected_horses:
            passage_orders.append('')
        while len(weights) < expected_horses:
            weights.append(0)
        while len(weight_diffs) < expected_horses:
            weight_diffs.append(0)
        while len(last_3f) < expected_horses:
            last_3f.append('')
        while len(popularities) < expected_horses:
            popularities.append('')
        
        return passage_orders[:expected_horses], weights[:expected_horses], weight_diffs[:expected_horses], last_3f[:expected_horses], popularities[:expected_horses]
    
    except Exception as e:
        logger.error(f"Error extracting horse details: {str(e)}")
        return [], [], [], [], []

# レース情報を抽出
def extract_race_info(soup, race_id):
    """HTMLからレース情報を抽出する"""
    race_info = {'race_id': race_id}
    
    # レース名
    race_name_elem = soup.select_one('.data_intro h1')
    if race_name_elem:
        race_info['race_name'] = race_name_elem.get_text(strip=True)
    else:
        # 代替セレクタを試す
        alt_selectors = ['.race_title', 'h1.tit', '#page_title h1']
        for selector in alt_selectors:
            elem = soup.select_one(selector)
            if elem:
                race_info['race_name'] = elem.get_text(strip=True)
                break
    
    # 日付・場所・コンディション等
    race_details_elem = soup.select_one('.data_intro .smalltxt')
    if race_details_elem:
        race_details = race_details_elem.get_text(strip=True)
        race_info['race_details'] = race_details
        
        # 日付を抽出
        if '日' in race_details:
            race_date_parts = race_details.split('日')[0].split()
            if race_date_parts:
                race_info['race_date'] = race_date_parts[0] + '日'
    else:
        # 代替セレクタを試す
        alt_selectors = ['.race_data', '.race_header_data', '.RaceData01', 'p.smalltxt']
        for selector in alt_selectors:
            elem = soup.select_one(selector)
            if elem:
                race_details = elem.get_text(strip=True)
                race_info['race_details'] = race_details
                
                # 日付を抽出
                if '日' in race_details:
                    race_date_parts = race_details.split('日')[0].split()
                    if race_date_parts:
                        race_info['race_date'] = race_date_parts[0] + '日'
                break
    
    # 直接RaceData01クラスから天候と馬場情報を抽出（修正版）
    race_data_elem = soup.select_one('.RaceData01')
    if race_data_elem:
        race_data_text = race_data_elem.get_text(strip=True)
        
        # 天候の抽出 - 正規表現パターンを修正
        weather_match = re.search(r'天候\s*[:：]\s*(\S+)', race_data_text)
        if weather_match:
            race_info['weather'] = weather_match.group(1)
        
        # 馬場状態の抽出 - 正規表現パターンを修正
        track_match = re.search(r'(芝|ダート)\s*[:：]\s*(\S+)', race_data_text)
        if track_match:
            race_info['track_condition'] = track_match.group(2)
    
    # バックアップの抽出方法: すべてのspanタグをチェック
    if 'weather' not in race_info or 'track_condition' not in race_info:
        span_elements = soup.find_all('span')
        for span in span_elements:
            span_text = span.get_text(strip=True)
            
            # 天候と馬場状態をチェック
            if 'weather' not in race_info:
                weather_match = re.search(r'天候\s*[:：]\s*(\S+)', span_text)
                if weather_match:
                    race_info['weather'] = weather_match.group(1)
            
            # 馬場状態 - コース種別に続く状態を検索
            if 'track_condition' not in race_info:
                track_match = re.search(r'(芝|ダート)\s*[:：]\s*(\S+)', span_text)
                if track_match:
                    race_info['track_condition'] = track_match.group(2)
    
    # より詳細なレース情報（クラス、コース種別、距離、馬場状態など）を抽出
    try:
        # レースの詳細情報は複数の場所に存在する可能性があるので複数のセレクタを試す
        race_data_spans = []
        selectors = ['span.race_type', 'span.Icon_GradeType', 'div.data_intro span']
        
        for selector in selectors:
            spans = soup.select(selector)
            if spans:
                race_data_spans.extend(spans)
        
        race_class = ''
        course_type = ''
        distance = ''
        course_direction = ''
        track_condition = ''
        weather = ''
        
        # 各spanからレース情報を抽出する
        for span in race_data_spans:
            span_text = span.get_text(strip=True)
            
            # レースのクラス（G1, G2, G3, 新馬, 未勝利など）
            if any(grade in span_text for grade in ['G1', 'G2', 'G3', 'G', 'オープン', '新馬', '未勝利']):
                race_class = span_text
            
            # コース情報を含むスパン
            if '芝' in span_text or 'ダ' in span_text:
                # コース種別（芝/ダート）
                if '芝' in span_text:
                    course_type = '芝'
                elif 'ダ' in span_text:
                    course_type = 'ダート'
                
                # 距離（メートル単位）
                distance_match = re.search(r'(\d+)m', span_text)
                if distance_match:
                    distance = distance_match.group(1)
                
                # コースの回り（右/左）
                if '右' in span_text:
                    course_direction = '右'
                elif '左' in span_text:
                    course_direction = '左'
                elif '直線' in span_text:
                    course_direction = '直線'
            
            # 馬場状態と天気
            if '馬場:' in span_text:
                track_parts = span_text.split('馬場:')
                if len(track_parts) > 1:
                    track_condition = track_parts[1].strip()
            
            if '天候:' in span_text:
                weather_parts = span_text.split('天候:')
                if len(weather_parts) > 1:
                    weather = weather_parts[1].strip()
        
        # 抽出した情報をrace_infoに追加
        if race_class:
            race_info['race_class'] = race_class
        if course_type:
            race_info['course_type'] = course_type
        if distance:
            race_info['distance'] = distance
        if course_direction:
            race_info['course_direction'] = course_direction
        if track_condition and 'track_condition' not in race_info:
            race_info['track_condition'] = track_condition
        if weather and 'weather' not in race_info:
            race_info['weather'] = weather
        
        # 別の方法でも詳細情報を取得する
        race_data_text = ''
        race_data_selectors = ['.RaceData', '.RaceList_Item', '.race_data_info', 'div.data_intro', '.RaceData01']
        
        for selector in race_data_selectors:
            race_data_elem = soup.select_one(selector)
            if race_data_elem:
                race_data_text = race_data_elem.get_text(strip=True)
                break
        
        if race_data_text:
            # 正規表現を使ってデータを抽出
            # コース種別と距離
            course_match = re.search(r'(芝|ダート)(\d+)m', race_data_text)
            if course_match:
                if not 'course_type' in race_info or not race_info['course_type']:
                    race_info['course_type'] = course_match.group(1)
                if not 'distance' in race_info or not race_info['distance']:
                    race_info['distance'] = course_match.group(2)
            
            # 馬場状態 - より広範なパターンに対応
            if 'track_condition' not in race_info:
                track_match = re.search(r'(芝|ダート)\s*[:：]\s*(\S+)', race_data_text)
                if track_match:
                    race_info['track_condition'] = track_match.group(2)
            
            # 天気 - より広範なパターンに対応
            if 'weather' not in race_info:
                weather_match = re.search(r'天候\s*[:：]\s*(\S+)', race_data_text)
                if weather_match:
                    race_info['weather'] = weather_match.group(1)
        
        # さらにバックアップ: race_detailsから抽出
        if ('race_details' in race_info) and ('weather' not in race_info or 'track_condition' not in race_info):
            details_text = race_info['race_details']
            
            if 'weather' not in race_info:
                weather_match = re.search(r'天候\s*[:：]\s*(\S+)', details_text)
                if weather_match:
                    race_info['weather'] = weather_match.group(1)
            
            if 'track_condition' not in race_info:
                track_match = re.search(r'(芝|ダート)\s*[:：]\s*(\S+)', details_text)
                if track_match:
                    race_info['track_condition'] = track_match.group(2)
        
    except Exception as e:
        logger.error(f"Error extracting detailed race info: {str(e)}")
    
    return race_info

# 馬のIDを抽出
def extract_horse_ids(soup):
    """レース結果ページから馬IDを抽出"""
    horse_ids = []
    horse_links = soup.select('table.race_table_01 td.horsename a, table.Shutuba_table td.horsename a')
    
    if not horse_links:
        # 代替の方法で馬リンクを探す
        horse_links = soup.select('a[href*="/horse/"]')
    
    for link in horse_links:
        href = link.get('href', '')
        if '/horse/' in href:
            try:
                horse_id = href.split('/horse/')[1].rstrip('/')
                horse_ids.append(horse_id)
            except:
                horse_ids.append(None)
    
    return horse_ids

# 複数レースのデータ収集
def scrape_races_by_id_pattern(year, places=None, max_races=None, batch_size=3, pause_between_batches=45):
    """
    レースIDパターンに基づいて複数レースの結果を収集する
    
    Args:
        year: 対象年
        places: 対象競馬場コードのリスト（Noneの場合はすべての競馬場）
        max_races: 最大収集レース数（Noneの場合は制限なし）
        batch_size: バッチあたりの処理レース数
        pause_between_batches: バッチ間の待機時間（秒）
    
    Returns:
        tuple: レース結果のDataFrame、レース情報リスト、馬IDリスト
    """
    all_results = []
    all_race_infos = []
    all_horse_ids = set()
    session = create_session()
    
    # 処理したレース数
    processed_count = 0
    # 有効なレース数
    valid_count = 0
    # バッチ制御用
    batch_count = 0
    
    # 進捗ファイル
    progress_file = f"{OUTPUT_DIR}/race_scraping_progress_{year}.txt"
    skip_ids = set()
    
    # 進捗ファイルが存在する場合は読み込む
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            skip_ids = set([line.strip() for line in f.readlines()])
        logger.info(f"Loaded {len(skip_ids)} processed race IDs from progress file")
    
    # 結果の中間保存用タイムスタンプ
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for race_id in generate_race_ids(year, places):
        # 既に処理済みのレースはスキップ
        if race_id in skip_ids:
            logger.debug(f"Skipping already processed race: {race_id}")
            continue
        
        processed_count += 1
        
        # ランダム間隔で待機（サーバー負荷軽減）
        if processed_count > 1 and processed_count % 10 == 0:
            time.sleep(random.uniform(3, 7))
        
        # 最初に高速チェック
        if not is_valid_race(race_id, session):
            # 無効なレースは処理しない
            with open(progress_file, 'a') as f:
                f.write(f"{race_id}\n")
            continue
        
        # レース結果を取得
        logger.info(f"Processing valid race: {race_id}")
        valid_count += 1
        batch_count += 1
        
        # レース結果を取得
        result, race_info = scrape_race_results(race_id, session=session)
        
        if result is not None:
            all_results.append(result)
            
            # 馬IDを収集
            if 'horse_id' in result.columns:
                horse_ids = result['horse_id'].dropna().unique()
                all_horse_ids.update(horse_ids)
        
        if race_info:
            all_race_infos.append(race_info)
        
        # 進捗ファイルに記録
        with open(progress_file, 'a') as f:
            f.write(f"{race_id}\n")
        
        # バッチが一定数に達したら中間結果を保存
        if batch_count >= batch_size:
            # 中間結果の保存
            if all_results:
                save_intermediate_results(all_results, all_race_infos, processed_count)
            
            # バッチ間の待機
            logger.info(f"Pausing for {pause_between_batches} seconds between batches")
            time.sleep(pause_between_batches)
            
            # バッチカウンタをリセット
            batch_count = 0
        
        # 最大レース数に達したら終了
        if max_races is not None and valid_count >= max_races:
            logger.info(f"Reached maximum number of races: {max_races}")
            break
    
    # 最終結果の保存
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        return combined_df, all_race_infos, list(all_horse_ids)
    
    return None, all_race_infos, list(all_horse_ids)

# 中間結果を保存
def save_intermediate_results(results, race_infos, count_index):
    """スクレイピング中の中間結果を保存"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if results:
        try:
            combined_df = pd.concat(results, ignore_index=True)
            combined_df.to_csv(f"{OUTPUT_DIR}/intermediate_races_{timestamp}_{count_index}.csv", 
                               index=False, encoding='utf-8-sig')
            logger.info(f"Saved intermediate results to intermediate_races_{timestamp}_{count_index}.csv")
        except Exception as e:
            logger.error(f"Failed to save intermediate race results: {str(e)}")
    
    if race_infos:
        try:
            with open(f"{OUTPUT_DIR}/intermediate_race_infos_{timestamp}_{count_index}.json", 'w', encoding='utf-8') as f:
                json.dump(race_infos, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved intermediate race info to intermediate_race_infos_{timestamp}_{count_index}.json")
        except Exception as e:
            logger.error(f"Failed to save intermediate race info: {str(e)}")

# コマンドライン引数の解析
def parse_args():
    parser = argparse.ArgumentParser(description='Netkeiba Race Data Scraper by Direct ID Pattern')
    parser.add_argument('--year', type=int, default=2024,
                        help='Year to scrape data for')
    parser.add_argument('--places', type=str, nargs='+',
                        help='Place codes to scrape (01-10, default: all places)')
    parser.add_argument('--batch_size', type=int, default=3,
                        help='Number of races to process in a batch')
    parser.add_argument('--pause', type=int, default=45,
                        help='Pause time between batches in seconds')
    parser.add_argument('--max_races', type=int, default=0,
                        help='Maximum number of races to collect (0 for no limit)')
    
    return parser.parse_args()

# メイン実行関数
def main():
    args = parse_args()
    year = args.year
    places = args.places  # None or list
    batch_size = args.batch_size
    pause_time = args.pause
    max_races = args.max_races if args.max_races > 0 else None
    
    print(f"Starting race data collection for {year}")
    
    # 競馬場が指定されている場合
    if places:
        place_names = [f"{p}({PLACE_DICT.get(p, 'Unknown')})" for p in places]
        print(f"Targeting race places: {', '.join(place_names)}")
    else:
        print(f"Targeting all race places")
    
    print(f"Settings: batch_size={batch_size}, pause={pause_time}s, max_races={max_races or 'unlimited'}")
    
    # レースデータ収集
    races_df, race_detailed_infos, horse_ids = scrape_races_by_id_pattern(
        year, places, max_races, batch_size, pause_time
    )
    
    # 結果の保存
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if races_df is not None:
        # 結果のファイル名を作成
        filename = f"races_{year}_{timestamp}.csv"
        
        # CSVを保存する前に天候と馬場情報が含まれているか確認
        if 'weather' not in races_df.columns:
            logger.warning("Weather information is missing in the dataframe")
        if 'track_condition' not in races_df.columns:
            logger.warning("Track condition information is missing in the dataframe")
        
        races_df.to_csv(f"{OUTPUT_DIR}/{filename}", index=False, encoding='utf-8-sig')
        print(f"Saved race data to {filename}")
        
        info_filename = f"race_infos_{year}_{timestamp}.json"
        with open(f"{OUTPUT_DIR}/{info_filename}", 'w', encoding='utf-8') as f:
            json.dump(race_detailed_infos, f, ensure_ascii=False, indent=2)
        print(f"Saved race info to {info_filename}")
        
        # 馬IDを保存
        horse_filename = f"horse_ids_{year}_{timestamp}.json"
        with open(f"{OUTPUT_DIR}/{horse_filename}", 'w', encoding='utf-8') as f:
            json.dump(horse_ids, f, ensure_ascii=False)
        print(f"Saved {len(horse_ids)} horse IDs to {horse_filename}")
    else:
        print("Failed to collect any race data")
    
    print("Race data collection completed!")

if __name__ == "__main__":
    main()
