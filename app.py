import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import gspread
from google.oauth2.service_account import Credentials
from io import BytesIO

# ページの設定
st.set_page_config(
    page_title="鑑定落ちリスト",
    layout="wide"
)

# ===================
# Googleスプレッドシート設定
# ===================
SPREADSHEET_ID = "1xJCrmUNqdAX0CNR_Mm7zenvgR-StP5d9VVRSe0CBnXM"
CREDENTIALS_FILE = "credentials.json"

@st.cache_resource
def get_google_sheet():
    """Googleスプレッドシートに接続"""
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        if os.path.exists(CREDENTIALS_FILE):
            credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        elif 'gcp_service_account' in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            st.error("認証情報が見つかりません")
            return None
        
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        return spreadsheet
    except Exception as e:
        st.error(f"Googleスプレッドシートへの接続に失敗しました: {e}")
        return None

def get_or_create_worksheet(spreadsheet, sheet_name):
    """ワークシートを取得、なければ作成"""
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=30)
    return worksheet

@st.cache_data(ttl=300)
def load_data_from_sheet(_spreadsheet):
    """スプレッドシートからデータを読み込み"""
    data = {"pig_details": {}, "repeat_breeding": {}, "week_comments": {}}
    
    try:
        ws_pig = get_or_create_worksheet(spreadsheet, "母豚詳細")
        records = ws_pig.get_all_records()
        for record in records:
            if record.get("key"):
                data["pig_details"][record["key"]] = {
                    "分娩舎": str(record.get("分娩舎", "")),
                    "ロット": str(record.get("ロット", "")),
                    "哺乳日数": str(record.get("哺乳日数", "")),
                    "P2値": str(record.get("P2値", "")),
                    "コメント": str(record.get("コメント", ""))
                }
        
        ws_repeat = get_or_create_worksheet(spreadsheet, "再発付け")
        records = ws_repeat.get_all_records()
        for record in records:
            farm = record.get("farm_name", "")
            week = record.get("week_id", "")
            if farm and week:
                key = f"{farm}_{week}"
                data["repeat_breeding"][key] = {
                    "種付": str(record.get("種付", "")),
                    "受胎": str(record.get("受胎", ""))
                }
        
        ws_comment = get_or_create_worksheet(spreadsheet, "週コメント")
        records = ws_comment.get_all_records()
        for record in records:
            farm = record.get("farm_name", "")
            week = record.get("week_id", "")
            if farm and week:
                key = f"{farm}_{week}"
                data["week_comments"][key] = str(record.get("コメント", ""))
    
    except Exception as e:
        st.warning(f"データ読み込み中にエラーが発生しました: {e}")
    
    return data

def parse_date_flexible(date_value, year=2025):
    """柔軟な日付パース（7月4日 や 2025-07-04 など）"""
    if pd.isna(date_value) or date_value == '':
        return None
    
    date_str = str(date_value)
    
    # すでにdatetime型の場合
    if isinstance(date_value, (pd.Timestamp, datetime)):
        return date_value.strftime('%Y-%m-%d')
    
    # 2025-07-04 形式
    if '-' in date_str and len(date_str) >= 10:
        return date_str[:10]
    
    # 7月4日 形式
    import re
    match = re.match(r'(\d+)月(\d+)日', date_str)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        return f"{year}-{month:02d}-{day:02d}"
    
    # 7/4 形式
    match = re.match(r'(\d+)/(\d+)', date_str)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        return f"{year}-{month:02d}-{day:02d}"
    
    return date_str


@st.cache_data(ttl=60)
def load_p2_data_from_sheet(_spreadsheet, farm_name, weaning_date):
    """スプレッドシートからP2値（経産）を読み込み"""
    try:
        ws = _spreadsheet.worksheet("P2値_経産")
        data = ws.get_all_records()
        
        # 検索対象の日付を正規化
        target_date = parse_date_flexible(weaning_date)
        if target_date is None:
            return None
        
        for record in data:
            if record.get("農場") == farm_name:
                record_date = parse_date_flexible(record.get("離乳日", ""))
                if record_date == target_date:
                    return record
        return None
    except Exception as e:
        return None


@st.cache_data(ttl=60)
def load_gilt_p2_data_from_sheet(_spreadsheet, farm_name, week_id):
    """スプレッドシートからP2値（初産）を読み込み"""
    try:
        ws = _spreadsheet.worksheet("P2値_初産")
        data = ws.get_all_records()
        
        # 検索対象の日付を正規化
        target_date = parse_date_flexible(week_id)
        if target_date is None:
            return None
        
        for record in data:
            if record.get("農場") == farm_name:
                record_date = parse_date_flexible(record.get("種付開始週", ""))
                if record_date == target_date:
                    return record
        return None
    except Exception as e:
        return None


@st.cache_data(ttl=60)
def load_semen_report_from_sheet(_spreadsheet, start_date):
    """スプレッドシートから採精レポートを読み込み"""
    try:
        ws = _spreadsheet.worksheet("採精レポート")
        data = ws.get_all_records()
        
        if not data:
            return None
        
        # 日付でフィルタリング（種付開始週の前日曜〜土曜）
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        
        days_since_monday = start_date.weekday()
        if days_since_monday == 0:
            previous_sunday = start_date - timedelta(days=1)
        else:
            previous_sunday = start_date - timedelta(days=days_since_monday + 1)
        
        days_until_saturday = 5 - start_date.weekday()
        if days_until_saturday < 0:
            days_until_saturday += 7
        saturday_of_week = start_date + timedelta(days=days_until_saturday)
        
        filtered_data = []
        for record in data:
            try:
                # 日付を正規化
                record_date_str = parse_date_flexible(record.get("採精日"))
                if record_date_str:
                    record_date = pd.to_datetime(record_date_str)
                    if previous_sunday <= record_date <= saturday_of_week:
                        record['採精日'] = record_date_str
                        filtered_data.append(record)
            except:
                pass
        
        if filtered_data:
            df = pd.DataFrame(filtered_data)
            return df
        return None
    except Exception as e:
        return None

@st.cache_data(ttl=60)
def load_all_breeding_records(_spreadsheet):
    """すべての種付記録を読み込み"""
    try:
        ws = _spreadsheet.worksheet("種付記録")
        data = ws.get_all_values()
        
        if len(data) <= 1:
            return None
        
        headers = data[0]
        rows = data[1:]
        
        if not rows:
            return None
        
        df = pd.DataFrame(rows, columns=headers)
        
        # farm_name列とweek_id列を除外
        if 'farm_name' in df.columns:
            df = df.drop(columns=['farm_name'])
        if 'week_id' in df.columns:
            df = df.drop(columns=['week_id'])
        
        return df
    except Exception as e:
        st.error(f"種付記録の読み込みに失敗しました: {e}")
        return None


def get_period_data(df, farm_name, period_type, year=None, month=None, start_date=None, end_date=None):
    """指定期間のデータをフィルタリング"""
    # 農場でフィルタ
    if '農場' in df.columns:
        df_filtered = df[df['農場'] == farm_name].copy()
    else:
        df_filtered = df.copy()
    
    # 種付日をdatetime型に変換
    df_filtered['種付日_dt'] = pd.to_datetime(df_filtered['種付日'], errors='coerce')
    
    # 期間でフィルタ
    if period_type == "月単位":
        df_filtered = df_filtered[
            (df_filtered['種付日_dt'].dt.year == year) & 
            (df_filtered['種付日_dt'].dt.month == month)
        ]
    elif period_type == "年単位":
        df_filtered = df_filtered[df_filtered['種付日_dt'].dt.year == year]
    elif period_type == "カスタム期間":
        df_filtered = df_filtered[
            (df_filtered['種付日_dt'] >= pd.to_datetime(start_date)) & 
            (df_filtered['種付日_dt'] <= pd.to_datetime(end_date))
        ]
    
    # 種付日_dt列を削除
    if '種付日_dt' in df_filtered.columns:
        df_filtered = df_filtered.drop(columns=['種付日_dt'])
    
    return df_filtered

def save_breeding_records(spreadsheet, df, week_id, farm_name):
    """種付記録をスプレッドシートに保存（一括処理）"""
    try:
        ws = get_or_create_worksheet(spreadsheet, "種付記録")
        
        # 既存データを取得
        existing_data = ws.get_all_values()
        
        # ヘッダー設定（farm_name + week_id + CSVの列名）
        csv_columns = df.columns.tolist()
        headers = ['farm_name', 'week_id'] + csv_columns
        
        # 新しいデータを準備
        new_rows = []
        for _, row in df.iterrows():
            row_data = [farm_name, week_id] + [str(v) if pd.notna(v) else '' for v in row.values]
            new_rows.append(row_data)
        
        if len(existing_data) <= 1 or existing_data[0][0] == '':
            # 新規または空のヘッダー：ヘッダー + データを一括書き込み
            all_data = [headers] + new_rows
        else:
            # 既存データから同じfarm_name + week_idの組み合わせ以外を残す
            all_data = [headers]
            for row in existing_data[1:]:
                if row and len(row) >= 2:
                    # 農場名とweek_idの両方が一致する場合は除外
                    if not (row[0] == farm_name and row[1] == week_id):
                        all_data.append(row)
            # 新しいデータを追加
            all_data.extend(new_rows)
        
        # シートをクリアして一括書き込み
        ws.clear()
        ws.update('A1', all_data)
        
        return True
    except Exception as e:
        st.error(f"種付記録の保存に失敗しました: {e}")
        return False
@st.cache_data(ttl=60)
def load_breeding_records(_spreadsheet, week_id, farm_name):
    """種付記録をスプレッドシートから読み込み"""
    try:
        ws = get_or_create_worksheet(_spreadsheet, "種付記録")
        data = ws.get_all_values()
        
        if len(data) <= 1:
            return None
        
        headers = data[0]
        # farm_nameとweek_idの両方が一致する行を取得
        rows = [row for row in data[1:] if row and len(row) >= 2 and row[0] == farm_name and row[1] == week_id]
        
        if not rows:
            return None
        
        # DataFrameを作成
        df = pd.DataFrame(rows, columns=headers)
        
        # farm_name列とweek_id列を除外
        if 'farm_name' in df.columns:
            df = df.drop(columns=['farm_name'])
        if 'week_id' in df.columns:
            df = df.drop(columns=['week_id'])
        
        return df
    except Exception as e:
        st.error(f"種付記録の読み込みに失敗しました: {e}")
        return None
    
@st.cache_data(ttl=60)
def get_saved_farms_and_weeks(_spreadsheet):
    """保存済みの農場と週一覧を取得"""
    try:
        ws = get_or_create_worksheet(_spreadsheet, "種付記録")
        data = ws.get_all_values()
        
        if len(data) <= 1:
            return {}, []
        
        # 農場ごとの週を取得
        farm_weeks = {}
        all_farms = set()
        
        for row in data[1:]:
            if row and len(row) >= 2 and row[0] and row[1]:
                farm_name = row[0]
                week_id = row[1]
                all_farms.add(farm_name)
                
                if farm_name not in farm_weeks:
                    farm_weeks[farm_name] = set()
                farm_weeks[farm_name].add(week_id)
        
        # セットをソートしたリストに変換
        for farm in farm_weeks:
            farm_weeks[farm] = sorted(list(farm_weeks[farm]), reverse=True)
        
        all_farms = sorted(list(all_farms))
        
        return farm_weeks, all_farms
    except Exception as e:
        st.error(f"データ一覧の取得に失敗しました: {e}")
        return {}, []

def save_data_to_sheet(spreadsheet, data, week_id, farm_name):
    """手入力データをスプレッドシートに保存（一括処理）"""
    try:
        # キーのプレフィックス（農場名_週ID）
        key_prefix = f"{farm_name}_{week_id}"
        
        # === 母豚詳細を保存 ===
        ws_pig = get_or_create_worksheet(spreadsheet, "母豚詳細")
        existing_data = ws_pig.get_all_values()
        
        headers = ["key", "farm_name", "week_id", "分娩舎", "ロット", "哺乳日数", "P2値", "コメント"]
        
        if len(existing_data) == 0:
            new_data = [headers]
        else:
            # 既存データから同じfarm_name + week_id以外を残す
            new_data = [headers]
            for row in existing_data[1:]:
                if row and len(row) >= 3:
                    if not (row[1] == farm_name and row[2] == week_id):
                        new_data.append(row)
        
        # 新しいデータを追加
        for key, details in data["pig_details"].items():
            if key.startswith(key_prefix):
                row_data = [key, farm_name, week_id, details.get("分娩舎", ""), details.get("ロット", ""), 
                           details.get("哺乳日数", ""), details.get("P2値", ""), details.get("コメント", "")]
                new_data.append(row_data)
        
        ws_pig.clear()
        if new_data:
            ws_pig.update('A1', new_data)
        
        # === 再発付けを保存 ===
        ws_repeat = get_or_create_worksheet(spreadsheet, "再発付け")
        existing_data = ws_repeat.get_all_values()
        
        headers = ["farm_name", "week_id", "種付", "受胎"]
        
        if len(existing_data) == 0:
            new_data = [headers]
        else:
            new_data = [headers]
            for row in existing_data[1:]:
                if row and len(row) >= 2:
                    if not (row[0] == farm_name and row[1] == week_id):
                        new_data.append(row)
        
        repeat_key = f"{farm_name}_{week_id}"
        if repeat_key in data["repeat_breeding"]:
            repeat_data = data["repeat_breeding"][repeat_key]
            row_data = [farm_name, week_id, repeat_data.get("種付", ""), repeat_data.get("受胎", "")]
            new_data.append(row_data)
        
        ws_repeat.clear()
        if new_data:
            ws_repeat.update('A1', new_data)
        
        # === 週コメントを保存 ===
        ws_comment = get_or_create_worksheet(spreadsheet, "週コメント")
        existing_data = ws_comment.get_all_values()
        
        headers = ["farm_name", "week_id", "コメント"]
        
        if len(existing_data) == 0:
            new_data = [headers]
        else:
            new_data = [headers]
            for row in existing_data[1:]:
                if row and len(row) >= 2:
                    if not (row[0] == farm_name and row[1] == week_id):
                        new_data.append(row)
        
        comment_key = f"{farm_name}_{week_id}"
        if comment_key in data["week_comments"]:
            row_data = [farm_name, week_id, data["week_comments"][comment_key]]
            new_data.append(row_data)
        
        ws_comment.clear()
        if new_data:
            ws_comment.update('A1', new_data)
        
        return True
    except Exception as e:
        st.error(f"データ保存中にエラーが発生しました: {e}")
        return False

# ===================
# カスタムCSS
# ===================
st.markdown("""
<style>
    /* テーブル中央揃え */
    table { width: 100%; }
    th { text-align: center !important; }
    td { text-align: center !important; }
    
    /* セレクトボックスのカーソルを指に変更 */
    [data-testid="stSelectbox"] > div > div {
        cursor: pointer !important;
    }
    
    [data-testid="stSelectbox"] input {
        cursor: pointer !important;
    }
    
    /* サイドバーのセレクトボックスのドロップダウンを前面に表示 */
    [data-testid="stSidebar"] [data-testid="stSelectbox"] > div > div > div {
        z-index: 9999 !important;
    }
    
    /* ドロップダウンリストを前面に表示 */
    [data-baseweb="popover"] {
        z-index: 9999 !important;
    }
    
    /* サイドバー内の要素の重なり順を調整 */
    [data-testid="stSidebar"] [data-testid="stExpander"] {
        z-index: 1 !important;
    }
    
    /* ファイルアップローダーの重なり順を調整 */
    [data-testid="stFileUploader"] {
        z-index: 1 !important;
    }
</style>
""", unsafe_allow_html=True)

# ===================
# ユーティリティ関数
# ===================
def to_halfwidth(text):
    """全角英数字を半角に変換"""
    if not text:
        return text
    halfwidth = str.maketrans(
        'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９',
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    )
    return text.translate(halfwidth)

def display_centered_table(df, height=None):
    """DataFrameをHTML形式で中央揃え表示"""
    html = df.to_html(index=False, escape=False)
    html = html.replace('<table', '<table style="width:100%; border-collapse:collapse; background-color:#ffffff;"')
    html = html.replace('<th>', '<th style="text-align:center; background-color:#f0f2f6; color:#333333; padding:10px; border:1px solid #dddddd; font-weight:bold;">')
    html = html.replace('<td>', '<td style="text-align:center; background-color:#ffffff; color:#333333; padding:10px; border:1px solid #dddddd;">')
    
    if height:
        st.markdown(f'<div style="height:{height}px; overflow-y:auto;">{html}</div>', unsafe_allow_html=True)
    else:
        st.markdown(html, unsafe_allow_html=True)

@st.fragment
def pig_details_input_form(df_not_pregnant, farm_name, week_id, comments_data):
    """不受胎母豚の詳細入力フォーム（フラグメント化で再実行を防止）"""
    
    if 'temp_pig_details' not in st.session_state:
        st.session_state.temp_pig_details = {}
    
    st.write("**不受胎母豚の詳細情報を入力**")
    
    for idx, row in df_not_pregnant.iterrows():
        pig_id = str(row['母豚番号'])
        detail_key = f"{farm_name}_{week_id}_{pig_id}"
        saved_details = comments_data["pig_details"].get(detail_key, {})
        
        # セッションステートから既存の入力値を取得（あれば）
        if detail_key in st.session_state.temp_pig_details:
            saved_details = st.session_state.temp_pig_details[detail_key]
    
        with st.expander(f"{pig_id}（{row['産次']}産 / {row['雄豚・精液・あて雄']}）", expanded=False):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                bunben = st.text_input("分娩舎", value=saved_details.get("分娩舎", ""), key=f"bunben_{detail_key}", placeholder="例: 1号")
            with col2:
                lot = st.text_input("ロット", value=saved_details.get("ロット", ""), key=f"lot_{detail_key}", placeholder="例: 2-3")
            with col3:
                honyugs = st.text_input("哺乳日数", value=saved_details.get("哺乳日数", ""), key=f"honyu_{detail_key}", placeholder="例: 21")
            with col4:
                p2_value = st.text_input("P2値", value=saved_details.get("P2値", ""), key=f"p2_{detail_key}", placeholder="例: 12")
            
            comment = st.text_input("コメント", value=saved_details.get("コメント", ""), key=f"comment_{detail_key}", placeholder="廃用理由、治療歴、助産歴など")
            
            st.session_state.temp_pig_details[detail_key] = {
                "分娩舎": to_halfwidth(bunben),
                "ロット": to_halfwidth(lot),
                "哺乳日数": to_halfwidth(honyugs),
                "P2値": to_halfwidth(p2_value),
                "コメント": comment
            }

def generate_print_html(df, week_id, farm_name, start_date, end_date, comments_data, 
                        df_parity, semen_stats, df_not_pregnant, week_comment,
                        p2_data=None, gilt_p2_data=None, semen_report=None):
    """印刷用HTMLを生成"""
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')
    import base64
    from io import BytesIO
    
   # 日本語フォント設定
    import matplotlib
    matplotlib.use('Agg')
    
    # 利用可能な日本語フォントを探す
    from matplotlib import font_manager
    
    # フォント候補リスト
    font_candidates = [
        'Hiragino Sans',
        'Hiragino Kaku Gothic ProN', 
        'Yu Gothic',
        'Meiryo',
        'MS Gothic',
        'Noto Sans CJK JP',
        'IPAGothic',
        'IPAPGothic',
        'VL Gothic',
        'Takao Gothic',
        'DejaVu Sans'
    ]
    
    # 利用可能なフォントを探す
    available_fonts = [f.name for f in font_manager.fontManager.ttflist]
    selected_font = None
    for font in font_candidates:
        if font in available_fonts:
            selected_font = font
            break
    
    if selected_font:
        plt.rcParams['font.family'] = selected_font
    else:
        # フォントが見つからない場合は英語で表示
        plt.rcParams['font.family'] = 'DejaVu Sans'
    
    # 受胎率計算
    total = len(df)
    pregnant = df['受胎'].sum()
    fertility_rate = pregnant / total * 100
    
    df_sow = df[df['産次'].astype(int) >= 2]
    sow_rate = df_sow['受胎'].sum() / len(df_sow) * 100 if len(df_sow) > 0 else 0
    
    df_gilt = df[df['産次'].astype(int) == 1]
    gilt_rate = df_gilt['受胎'].sum() / len(df_gilt) * 100 if len(df_gilt) > 0 else 0
    
    # 不受胎リストデータ準備
    not_pregnant_html = ""
    if len(df_not_pregnant) > 0:
        display_data = []
        for idx, row in df_not_pregnant.iterrows():
            pig_id = str(row['母豚番号'])
            detail_key = f"{farm_name}_{week_id}_{pig_id}"
            details = comments_data["pig_details"].get(detail_key, {})
            
            display_data.append({
                '種付日': row['種付日'],
                '母豚番号': pig_id,
                '精液': row['雄豚・精液・あて雄'],
                '産次': row['産次'],
                '分娩舎': details.get('分娩舎', ''),
                'ロット': details.get('ロット', ''),
                '哺乳日数': details.get('哺乳日数', ''),
                'P2値': details.get('P2値', ''),
                'コメント': details.get('コメント', '')
            })
        df_display = pd.DataFrame(display_data)
        not_pregnant_html = df_display.to_html(index=False)
    else:
        not_pregnant_html = "<p>不受胎なし</p>"
    
    # グラフ生成関数
    def create_bar_chart_base64(data_df, title, color, x_col='P2値(mm)', y_col='頭数'):
        """棒グラフを生成してBase64エンコードした画像を返す"""
        fig, ax = plt.subplots(figsize=(8, 4))
        
        x_values = data_df[x_col].astype(str).tolist()
        y_values = data_df[y_col].tolist()
        
        bars = ax.bar(x_values, y_values, color=color, edgecolor='white')
        
        # 英語ラベルを使用（文字化け防止）
        ax.set_xlabel('P2 (mm)')
        ax.set_ylabel('Count')
        
        # タイトルを英語に変換
        if '経産' in title:
            ax.set_title('P2 Distribution (Sow)')
        elif '初産' in title:
            ax.set_title('P2 Distribution (Gilt)')
        else:
            ax.set_title('P2 Distribution')
        
        # 値をバーの上に表示
        for bar, val in zip(bars, y_values):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                       str(int(val)), ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        
        # Base64エンコード
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
        buffer.seek(0)
        img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        plt.close(fig)
        
        return img_base64
    
    # P2値（経産）HTML
    p2_html = ""
    if p2_data:
        try:
            chart_base64 = create_bar_chart_base64(
                p2_data['table'], 
                '離乳時P2値分布（経産）', 
                '#1f77b4'
            )
            p2_html = f"""
            <h2>【離乳時P2値分布（経産）】</h2>
            <p>離乳日: {p2_data['weaning_date']} / ロット: {p2_data['lot']} / 平均P2値: {p2_data['average']:.1f}mm</p>
            <div class="chart-container">
                <img src="data:image/png;base64,{chart_base64}" alt="P2値分布（経産）" style="max-width: 500px; width: 65%;">
                <div class="table-side">
                    {p2_data['table'].to_html(index=False)}
                </div>
            </div>
            """
        except Exception as e:
            p2_html = f"""
            <h2>【離乳時P2値分布（経産）】</h2>
            <p>離乳日: {p2_data['weaning_date']} / ロット: {p2_data['lot']} / 平均P2値: {p2_data['average']:.1f}mm</p>
            {p2_data['table'].to_html(index=False)}
            """
    
    # P2値（初産）HTML
    gilt_p2_html = ""
    if gilt_p2_data:
        try:
            chart_base64 = create_bar_chart_base64(
                gilt_p2_data['table'], 
                '種付時P2値分布（初産）', 
                '#ff7f0e'
            )
            gilt_p2_html = f"""
            <h2>【種付時P2値分布（初産）】</h2>
            <p>種付開始週: {week_id} / 平均P2値: {gilt_p2_data['average']:.1f}mm</p>
            <div class="chart-container">
                <img src="data:image/png;base64,{chart_base64}" alt="P2値分布（初産）" style="max-width: 500px; width: 65%;">
                    {gilt_p2_data['table'].to_html(index=False)}
                </div>
            </div>
            """
        except Exception as e:
            gilt_p2_html = f"""
            <h2>【種付時P2値分布（初産）】</h2>
            <p>種付開始週: {week_id} / 平均P2値: {gilt_p2_data['average']:.1f}mm</p>
            {gilt_p2_data['table'].to_html(index=False)}
            """
    
    # 採精レポートHTML
    semen_html = ""
    if semen_report is not None and len(semen_report) > 0:
        semen_html = f"""
        <h2>【採精レポート】</h2>
        {semen_report.to_html(index=False)}
        """
    
    # 週コメントHTML
    comment_html = ""
    if week_comment:
        comment_html = f"""
        <h2>【週のコメント】</h2>
        <div class="comment-box">{week_comment.replace(chr(10), '<br>')}</div>
        """
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>鑑定落ちリスト_{farm_name}_{week_id}</title>
        <style>
            @media print {{
                body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
            }}
            body {{
                font-family: "Hiragino Sans", "Hiragino Kaku Gothic ProN", "Noto Sans JP", "メイリオ", sans-serif;
                font-size: 11px;
                line-height: 1.4;
                color: #333;
                max-width: 1000px;
                margin: 0 auto;
                padding: 20px;
            }}
            h1 {{
                font-size: 20px;
                text-align: center;
                margin-bottom: 5px;
                color: #1f77b4;
            }}
            h2 {{
                font-size: 14px;
                margin-top: 20px;
                margin-bottom: 10px;
                padding-bottom: 3px;
                border-bottom: 2px solid #1f77b4;
            }}
            .header-info {{
                text-align: center;
                margin-bottom: 20px;
            }}
            .summary-container {{
                display: flex;
                justify-content: center;
                gap: 30px;
                margin: 20px 0;
            }}
            .summary-item {{
                text-align: center;
                padding: 15px 25px;
                background-color: #f0f2f6;
                border-radius: 10px;
            }}
            .summary-item .label {{ font-size: 12px; color: #666; }}
            .summary-item .rate {{ font-size: 28px; font-weight: bold; }}
            .summary-item .count {{ font-size: 14px; color: #333; }}
            .rate-total {{ color: #1f77b4; }}
            .rate-sow {{ color: #2ca02c; }}
            .rate-gilt {{ color: #ff7f0e; }}
            .two-column {{
                display: flex;
                gap: 30px;
            }}
            .two-column > div {{ flex: 1; }}
            .chart-container {{
                display: flex;
                gap: 15px;
                align-items: flex-start;
                margin: 10px 0;
            }}
            .chart-container img {{
                flex-shrink: 0;
                max-width: 500px;
                width: 65%;
            }}
            .table-side {{
                flex: 1;
                font-size: 9px;
            }}
            .table-side table {{
                font-size: 9px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 10px 0;
                font-size: 10px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 6px;
                text-align: center;
            }}
            th {{
                background-color: #f0f2f6;
                font-weight: bold;
            }}
            .comment-box {{
                background-color: #f9f9f9;
                border: 1px solid #ddd;
                border-radius: 5px;
                padding: 15px;
                margin-top: 10px;
                white-space: pre-wrap;
            }}
            .print-button {{
                position: fixed;
                top: 10px;
                right: 10px;
                padding: 10px 20px;
                background-color: #1f77b4;
                color: white;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                font-size: 14px;
            }}
            .print-button:hover {{ background-color: #1565a0; }}
            @media print {{
                .print-button {{ display: none; }}
            }}
        </style>
    </head>
    <body>
        <button class="print-button" onclick="window.print()">🖨️ 印刷 / PDF保存</button>
        
        <h1>鑑定落ちリスト</h1>
        
        <div class="header-info">
            <p><strong>種付期間:</strong> {start_date.strftime('%Y-%m-%d')} ～ {end_date.strftime('%Y-%m-%d')}</p>
            <p><strong>農場:</strong> {farm_name}</p>
            <p><strong>作成日:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        </div>
        
        <h2>【受胎率サマリー】</h2>
        <div class="summary-container">
            <div class="summary-item">
                <div class="label">合計</div>
                <div class="rate rate-total">{fertility_rate:.1f}%</div>
                <div class="count">{int(pregnant)} / {total} 頭</div>
            </div>
            <div class="summary-item">
                <div class="label">経産</div>
                <div class="rate rate-sow">{sow_rate:.1f}%</div>
                <div class="count">{int(df_sow['受胎'].sum())} / {len(df_sow)} 頭</div>
            </div>
            <div class="summary-item">
                <div class="label">初産(Gilt)</div>
                <div class="rate rate-gilt">{gilt_rate:.1f}%</div>
                <div class="count">{int(df_gilt['受胎'].sum())} / {len(df_gilt)} 頭</div>
            </div>
        </div>
        
        <div class="two-column">
            <div>
                <h2>【産次別受胎率】</h2>
                {df_parity.to_html(index=False)}
            </div>
            <div>
                <h2>【精液別受胎率】</h2>
                {semen_stats.to_html(index=False)}
            </div>
        </div>
        
        <h2>【不受胎リスト】</h2>
        {not_pregnant_html}
        
        {p2_html}
        {gilt_p2_html}
        {semen_html}
        {comment_html}
        
    </body>
    </html>
    """
    return html

# ===================
# スプレッドシート接続
# ===================
spreadsheet = get_google_sheet()

if spreadsheet:
    st.sidebar.success("✅ Googleスプレッドシート接続済み")
    with st.spinner("保存データを読み込み中..."):
        comments_data = load_data_from_sheet(spreadsheet)
        farm_weeks, all_farms = get_saved_farms_and_weeks(spreadsheet)
else:
    st.sidebar.warning("⚠️ オフラインモード")
    comments_data = {"pig_details": {}, "repeat_breeding": {}, "week_comments": {}}
    farm_weeks = {}
    all_farms = []

# タイトル
st.title("鑑定落ちリスト")

# ===================
# サイドバー
# ===================
# セッション状態の初期化
if 'temp_pig_details' not in st.session_state:
    st.session_state.temp_pig_details = {}
if 'temp_repeat_breeding' not in st.session_state:
    st.session_state.temp_repeat_breeding = {}
if 'temp_week_comment' not in st.session_state:
    st.session_state.temp_week_comment = ""

st.sidebar.header("📁 データ選択")

# データソースの選択肢を設定
data_sources = ["CSVをアップロード", "過去データから選択", "期間別レポート"]

data_source = st.sidebar.radio(
    "データの読み込み方法",
    data_sources,
    index=0
)

# 編集モードの管理
if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False

# データソースが変わったら編集モードをリセット
if 'previous_data_source' not in st.session_state:
    st.session_state.previous_data_source = data_source
elif st.session_state.previous_data_source != data_source:
    st.session_state.edit_mode = False
    st.session_state.previous_data_source = data_source

# 過去データ以外は常に編集モード
if data_source != "過去データから選択":
    st.session_state.edit_mode = True

df = None
week_id = None
farm_name = None

if data_source == "CSVをアップロード":
    uploaded_csv = st.sidebar.file_uploader(
        "種付記録CSV（Porker出力）",
        type=['csv']
    )
    
    if uploaded_csv is not None:
        df = pd.read_csv(uploaded_csv, encoding='utf-8-sig')
        df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'
        start_date = pd.to_datetime(df['種付日'].min())
        week_id = start_date.strftime('%Y-%m-%d')
        
        # 農場名を取得
        if '農場' in df.columns:
            farm_name = df['農場'].iloc[0]
        else:
            farm_name = "不明"

elif data_source == "過去データから選択":
    if all_farms:
        selected_farm = st.sidebar.selectbox(
            "農場を選択",
            all_farms
        )
        
        if selected_farm and selected_farm in farm_weeks:
            weeks_for_farm = farm_weeks[selected_farm]
            
            if weeks_for_farm:
                selected_week = st.sidebar.selectbox(
                    "週を選択",
                    weeks_for_farm,
                    format_func=lambda x: f"{x} 週"
                )
                
                if selected_week:
                    farm_name = selected_farm
                    week_id = selected_week
                    with st.spinner("📂 データを読み込み中..."):
                        df = load_breeding_records(spreadsheet, week_id, farm_name)
                        if df is not None:
                            df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'
                        # 編集モード切り替えボタン
                    if not st.session_state.edit_mode:
                        if st.sidebar.button("編集する"):
                            st.session_state.edit_mode = True
                            st.rerun()
                    else:
                        if st.sidebar.button("閲覧モードに戻る"):
                            st.session_state.edit_mode = False
                            st.cache_data.clear()
                            st.rerun()
            else:
                st.sidebar.info("この農場の保存データがありません")
    else:
        st.sidebar.info("保存済みのデータがありません")

elif data_source == "期間別レポート":
    st.session_state.edit_mode = False  # 期間別レポートは閲覧のみ
    
    if all_farms:
        selected_farm = st.sidebar.selectbox(
            "農場を選択",
            all_farms,
            key="period_farm"
        )
        
        period_type = st.sidebar.radio(
            "期間の種類",
            ["月単位", "年単位", "カスタム期間"]
        )
        
        # 期間の選択
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        if period_type == "月単位":
            col_y, col_m = st.sidebar.columns(2)
            with col_y:
                selected_year = st.selectbox(
                    "年",
                    range(current_year, current_year - 5, -1),
                    key="period_year_month"
                )
            with col_m:
                selected_month = st.selectbox(
                    "月",
                    range(1, 13),
                    index=current_month - 1,
                    key="period_month"
                )
        elif period_type == "年単位":
            selected_year = st.sidebar.selectbox(
                "年",
                range(current_year, current_year - 5, -1),
                key="period_year"
            )
            selected_month = None
        else:  # カスタム期間
            col_start, col_end = st.sidebar.columns(2)
            with col_start:
                custom_start = st.date_input(
                    "開始日",
                    value=datetime(current_year, current_month, 1),
                    key="period_start"
                )
            with col_end:
                custom_end = st.date_input(
                    "終了日",
                    value=datetime.now(),
                    key="period_end"
                )
            selected_year = None
            selected_month = None
        
        if st.sidebar.button("レポートを表示"):
            with st.spinner("データを集計中..."):
                # すべての種付記録を読み込み
                df_all = load_all_breeding_records(spreadsheet)
                
                if df_all is not None and len(df_all) > 0:
                    # 期間でフィルタリング
                    if period_type == "カスタム期間":
                        df = get_period_data(
                            df_all, selected_farm, period_type,
                            start_date=custom_start, end_date=custom_end
                        )
                    else:
                        df = get_period_data(
                            df_all, selected_farm, period_type,
                            year=selected_year, month=selected_month
                        )
                    
                    if len(df) > 0:
                        df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'
                        farm_name = selected_farm
                        
                        # 期間情報をセッションに保存
                        st.session_state['period_df'] = df
                        st.session_state['period_farm_name'] = farm_name
                        st.session_state['period_type'] = period_type
                        if period_type == "月単位":
                            st.session_state['period_label'] = f"{selected_year}年{selected_month}月"
                        elif period_type == "年単位":
                            st.session_state['period_label'] = f"{selected_year}年"
                        else:
                            st.session_state['period_label'] = f"{custom_start} ～ {custom_end}"
                        
                        st.rerun()
                    else:
                        st.sidebar.warning("指定期間のデータがありません")
                else:
                    st.sidebar.warning("種付記録データがありません")
    else:
        st.sidebar.info("保存済みのデータがありません")
    
    # セッションステートからデータを復元
    if 'period_df' in st.session_state:
        df = st.session_state['period_df']
        farm_name = st.session_state['period_farm_name']

# ===================
# メインコンテンツ
# ===================
if df is not None and (week_id is not None or data_source == "期間別レポート"):
    # 期間別レポートの場合は別のヘッダー
    if data_source == "期間別レポート":
        period_label = st.session_state.get('period_label', '')
        st.header(f"期間別受胎率レポート")
        st.subheader(f"農場: {farm_name}")
        st.subheader(f"期間: {period_label}")
        st.caption(f"作成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
    else:
        start_date = pd.to_datetime(df['種付日'].min())
        end_date = pd.to_datetime(df['種付日'].max())
        
    start_date = pd.to_datetime(df['種付日'].min())
    end_date = pd.to_datetime(df['種付日'].max())
    
    # ヘッダー情報
    st.header(f"種付期間: {start_date.strftime('%Y-%m-%d')} ～ {end_date.strftime('%Y-%m-%d')}")
    st.subheader(f"農場: {farm_name}")
    st.caption(f"作成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # ===================
    # 受胎率サマリー
    # ===================
    st.subheader("【受胎率サマリー】")
    
    total = len(df)
    pregnant = df['受胎'].sum()
    fertility_rate = pregnant / total * 100
    
    df_sow = df[df['産次'].astype(int) >= 2]
    sow_rate = df_sow['受胎'].sum() / len(df_sow) * 100 if len(df_sow) > 0 else 0
    
    df_gilt = df[df['産次'].astype(int) == 1]
    gilt_rate = df_gilt['受胎'].sum() / len(df_gilt) * 100 if len(df_gilt) > 0 else 0
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background-color: #f0f2f6; border-radius: 10px;">
            <p style="margin: 0; font-size: 16px; color: #666;">合計</p>
            <p style="margin: 0; font-size: 36px; font-weight: bold; color: #1f77b4;">{fertility_rate:.1f}%</p>
            <p style="margin: 0; font-size: 18px; color: #333;">{int(pregnant)} / {total} 頭</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background-color: #f0f2f6; border-radius: 10px;">
            <p style="margin: 0; font-size: 16px; color: #666;">経産</p>
            <p style="margin: 0; font-size: 36px; font-weight: bold; color: #2ca02c;">{sow_rate:.1f}%</p>
            <p style="margin: 0; font-size: 18px; color: #333;">{int(df_sow['受胎'].sum())} / {len(df_sow)} 頭</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background-color: #f0f2f6; border-radius: 10px;">
            <p style="margin: 0; font-size: 16px; color: #666;">初産(Gilt)</p>
            <p style="margin: 0; font-size: 36px; font-weight: bold; color: #ff7f0e;">{gilt_rate:.1f}%</p>
            <p style="margin: 0; font-size: 18px; color: #333;">{int(df_gilt['受胎'].sum())} / {len(df_gilt)} 頭</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.write("")
    
    # ===================
    # 産次別 & 精液別
    # ===================
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("【産次別受胎率】")
        
        parity_data = []
        for parity in sorted(df['産次'].astype(int).unique()):
            df_p = df[df['産次'].astype(int) == parity]
            p_total = len(df_p)
            p_pregnant = df_p['受胎'].sum()
            p_rate = p_pregnant / p_total * 100 if p_total > 0 else 0
            parity_data.append({
                '産次': f"{parity}産",
                '受胎': int(p_pregnant),
                '種付': p_total,
                '受胎率': f"{p_rate:.1f}%"
            })
        
        # 再発付けデータ
        repeat_key = f"{farm_name}_{week_id}"
        saved_repeat = comments_data["repeat_breeding"].get(repeat_key, {"種付": "", "受胎": ""})
        
        if 'temp_repeat_breeding' not in st.session_state:
            st.session_state.temp_repeat_breeding = saved_repeat
        
        repeat_total = st.session_state.temp_repeat_breeding.get("種付", "")
        repeat_pregnant = st.session_state.temp_repeat_breeding.get("受胎", "")
        
        if repeat_total and repeat_pregnant:
            try:
                rt = int(repeat_total)
                rp = int(repeat_pregnant)
                r_rate = rp / rt * 100 if rt > 0 else 0
                parity_data.append({
                    '産次': '再発付',
                    '受胎': rp,
                    '種付': rt,
                    '受胎率': f"{r_rate:.1f}%"
                })
            except ValueError:
                pass
        
        df_parity = pd.DataFrame(parity_data)
        display_centered_table(df_parity)
        
       # 再発付け入力フォーム（編集モード時のみ）
        if st.session_state.edit_mode:
            st.write("**再発付けの入力**")
            col_r1, col_r2 = st.columns(2)
            with col_r1:
                repeat_total_input = st.text_input(
                    "再発付け種付頭数",
                    value=saved_repeat.get("種付", ""),
                    key="repeat_total",
                    placeholder="例: 5"
                )
            with col_r2:
                repeat_pregnant_input = st.text_input(
                    "再発付け受胎頭数",
                    value=saved_repeat.get("受胎", ""),
                    key="repeat_pregnant",
                    placeholder="例: 4"
                )
            
            st.session_state.temp_repeat_breeding = {
                "種付": to_halfwidth(repeat_total_input),
                "受胎": to_halfwidth(repeat_pregnant_input)
            }
    
    with col_right:
        st.subheader("【精液別受胎率】")
        
        semen_stats = df.groupby('雄豚・精液・あて雄').agg(
            種付=('受胎', 'count'),
            受胎=('受胎', 'sum')
        ).reset_index()
        semen_stats['受胎率'] = (semen_stats['受胎'] / semen_stats['種付'] * 100).round(1).astype(str) + '%'
        semen_stats.columns = ['精液', '種付', '受胎', '受胎率']
        semen_stats = semen_stats.sort_values('種付', ascending=False)
        
        display_centered_table(semen_stats)
    
   # 期間別レポートの場合はここで終了（不受胎リスト、P2値、採精レポートは表示しない）
    if data_source == "期間別レポート":
        st.divider()
        
        # ===================
        # 週ごとの受胎率推移
        # ===================
        st.subheader("【週ごとの受胎率推移】")
        
        # 種付日から週の開始日を計算
        df['種付日_dt'] = pd.to_datetime(df['種付日'])
        df['週開始日'] = df['種付日_dt'].apply(lambda x: x - timedelta(days=x.weekday()))
        
        # 週ごとに集計
        weekly_stats = df.groupby('週開始日').agg(
            種付頭数=('受胎', 'count'),
            受胎頭数=('受胎', 'sum')
        ).reset_index()
        
        weekly_stats['受胎率'] = (weekly_stats['受胎頭数'] / weekly_stats['種付頭数'] * 100).round(1)
        weekly_stats['週開始日'] = weekly_stats['週開始日'].dt.strftime('%Y-%m-%d')
        
        # 経産・初産別の週ごと集計
        df_sow = df[df['産次'].astype(int) >= 2]
        df_gilt = df[df['産次'].astype(int) == 1]
        
        weekly_sow = df_sow.groupby('週開始日').agg(
            経産_種付=('受胎', 'count'),
            経産_受胎=('受胎', 'sum')
        ).reset_index()
        weekly_sow['経産_受胎率'] = (weekly_sow['経産_受胎'] / weekly_sow['経産_種付'] * 100).round(1)
        
        weekly_gilt = df_gilt.groupby('週開始日').agg(
            初産_種付=('受胎', 'count'),
            初産_受胎=('受胎', 'sum')
        ).reset_index()
        weekly_gilt['初産_受胎率'] = (weekly_gilt['初産_受胎'] / weekly_gilt['初産_種付'] * 100).round(1)
        
        # データを結合
        weekly_stats['週開始日_dt'] = pd.to_datetime(weekly_stats['週開始日'])
        weekly_sow['週開始日_dt'] = weekly_sow['週開始日']
        weekly_gilt['週開始日_dt'] = weekly_gilt['週開始日']
        
        weekly_merged = weekly_stats.copy()
        weekly_merged = weekly_merged.merge(
            weekly_sow[['週開始日_dt', '経産_種付', '経産_受胎', '経産_受胎率']], 
            on='週開始日_dt', how='left'
        )
        weekly_merged = weekly_merged.merge(
            weekly_gilt[['週開始日_dt', '初産_種付', '初産_受胎', '初産_受胎率']], 
            on='週開始日_dt', how='left'
        )
        
        # 表示用に整形
        weekly_display = weekly_merged[['週開始日', '種付頭数', '受胎頭数', '受胎率', 
                                         '経産_種付', '経産_受胎', '経産_受胎率',
                                         '初産_種付', '初産_受胎', '初産_受胎率']].copy()
        weekly_display.columns = ['週開始日', '種付', '受胎', '受胎率(%)', 
                                   '経産種付', '経産受胎', '経産率(%)',
                                   '初産種付', '初産受胎', '初産率(%)']
        
        # NaNを0に置換
        weekly_display = weekly_display.fillna(0)
        for col in ['経産種付', '経産受胎', '初産種付', '初産受胎']:
            weekly_display[col] = weekly_display[col].astype(int)
        
        # 受胎率に%を追加
        weekly_display['受胎率(%)'] = weekly_display['受胎率(%)'].astype(str) + '%'
        weekly_display['経産率(%)'] = weekly_display['経産率(%)'].astype(str) + '%'
        weekly_display['初産率(%)'] = weekly_display['初産率(%)'].astype(str) + '%'
        
        # 表を表示
        display_centered_table(weekly_display)
        
        # 折れ線グラフ
        st.subheader("【受胎率推移グラフ】")
        
        import altair as alt
        
        # グラフ用データを準備
        chart_data = weekly_merged[['週開始日', '受胎率', '経産_受胎率', '初産_受胎率']].copy()
        chart_data.columns = ['週開始日', '合計', '経産', '初産']
        
        # 縦持ちに変換
        chart_melted = chart_data.melt(
            id_vars=['週開始日'],
            value_vars=['合計', '経産', '初産'],
            var_name='区分',
            value_name='受胎率'
        )
        
        # NaNを除外
        chart_melted = chart_melted.dropna()
        
        # 折れ線グラフを作成
        line_chart = alt.Chart(chart_melted).mark_line(point=True).encode(
            x=alt.X('週開始日:N', title='週開始日', sort=None),
            y=alt.Y('受胎率:Q', title='受胎率 (%)', scale=alt.Scale(domain=[0, 100])),
            color=alt.Color('区分:N', title='区分', 
                           scale=alt.Scale(domain=['合計', '経産', '初産'],
                                          range=['#1f77b4', '#2ca02c', '#ff7f0e'])),
            tooltip=['週開始日', '区分', alt.Tooltip('受胎率:Q', format='.1f')]
        ).properties(
            height=400
        )
        
        st.altair_chart(line_chart, use_container_width=True)
        
        st.divider()
        st.success(f"集計対象: {len(df)}頭のデータを集計しました")
        st.stop()
    
    # ===================
    # 不受胎リスト
    # ===================
    st.subheader("【不受胎リスト】")
    
    df_not_pregnant = df[df['受胎'] == False].copy()
    
    if len(df_not_pregnant) > 0:
        # 詳細入力フォーム（編集モード時のみ）
        if st.session_state.edit_mode:
            pig_details_input_form(df_not_pregnant, farm_name, week_id, comments_data)
        
        st.write("**不受胎一覧表**")
        if st.session_state.edit_mode:
            st.caption("💡 入力内容は「データを保存」後に反映されます")
        
        display_data = []
        for idx, row in df_not_pregnant.iterrows():
            pig_id = str(row['母豚番号'])
            detail_key = f"{farm_name}_{week_id}_{pig_id}"
            
            # 保存済みデータを表示（入力中のデータは保存後に反映される）
            details = comments_data["pig_details"].get(detail_key, {})
            
            hormone = row['投与ホルモン'] if pd.notna(row.get('投与ホルモン')) else ''
            days_after_weaning = row['離乳後交配日数'] if pd.notna(row.get('離乳後交配日数')) else ''
            if days_after_weaning != '':
                try:
                    days_after_weaning = int(float(days_after_weaning))
                except:
                    pass
            
            display_data.append({
                '種付日': row['種付日'],
                '母豚番号': pig_id,
                '精液': row['雄豚・精液・あて雄'],
                '分娩予定日': row.get('分娩予定日', ''),
                '産次': row['産次'],
                '投与ホルモン': hormone,
                '離乳後交配日数': days_after_weaning,
                '分娩舎': details.get('分娩舎', ''),
                'ロット': details.get('ロット', ''),
                '哺乳日数': details.get('哺乳日数', ''),
                'P2値': details.get('P2値', ''),
                'コメント': details.get('コメント', '')
            })
        
        df_display = pd.DataFrame(display_data)
        display_centered_table(df_display)
    else:
        st.success("不受胎なし")
    
   # ===================
    # P2値分布（経産）
    # ===================
    st.subheader("【離乳時P2値分布（経産）】")
    
    # スプレッドシートまたはアップロードファイルからデータを取得
    p2_row = None
    most_common_weaning = None
    
    df_sow_for_p2 = df[df['産次'].astype(int) >= 2]
    if len(df_sow_for_p2) > 0 and df_sow_for_p2['前回離乳日'].notna().any():
        most_common_weaning = df_sow_for_p2['前回離乳日'].value_counts().idxmax()
        
        # まずスプレッドシートから読み込み
        if spreadsheet:
            p2_record = load_p2_data_from_sheet(spreadsheet, farm_name, most_common_weaning)
            if p2_record:
                p2_row = p2_record
    
    if p2_row and most_common_weaning:
        lot_value = p2_row.get('離乳ロット', '')
        st.write(f"**離乳日:** {most_common_weaning} / **ロット:** {lot_value}")
        
        p2_columns = [str(i) for i in range(4, 21)]
        p2_data = []
        total_count = 0
        weighted_sum = 0
        
        for p2 in p2_columns:
            if p2 in p2_row:
                try:
                    count = int(p2_row[p2])
                    total_count += count
                    weighted_sum += int(p2) * count
                    p2_data.append({'P2値(mm)': int(p2), '頭数': count})
                except:
                    pass
        
        if total_count > 0:
            average_p2 = weighted_sum / total_count
            
            col_chart, col_table = st.columns(2)
            
            with col_chart:
                import altair as alt
                df_p2_chart = pd.DataFrame(p2_data)
                df_p2_chart = df_p2_chart.sort_values('P2値(mm)')
                df_p2_chart['P2値'] = df_p2_chart['P2値(mm)'].astype(str) + 'mm'
                
                chart = alt.Chart(df_p2_chart).mark_bar().encode(
                    x=alt.X('P2値:N', sort=df_p2_chart['P2値'].tolist(), title='P2値'),
                    y=alt.Y('頭数:Q', title='頭数'),
                    tooltip=['P2値', '頭数']
                ).properties(height=300)
                st.altair_chart(chart, use_container_width=True)
            
            with col_table:
                df_p2_table = pd.DataFrame(p2_data)
                df_p2_table = df_p2_table[df_p2_table['頭数'] > 0]
                df_p2_table = df_p2_table.sort_values('P2値(mm)')
                df_p2_table['P2値(mm)'] = df_p2_table['P2値(mm)'].astype(str) + 'mm'
                display_centered_table(df_p2_table, height=300)
            
            st.write(f"**合計:** {total_count}頭 / **平均P2値:** {average_p2:.1f}mm")
        else:
            st.info("P2値データがありません")
    else:
        if most_common_weaning:
            st.info(f"離乳日 {most_common_weaning} に対応するP2値データがスプレッドシートに登録されていません")
        else:
            st.info("経産豚の離乳データがありません")
    
    # ===================
    # P2値分布（初産）
    # ===================
    st.subheader("【種付時P2値分布（初産）】")
    
    # スプレッドシートまたはアップロードファイルからデータを取得
    gilt_p2_row = None
    
    # まずスプレッドシートから読み込み
    if spreadsheet:
        gilt_p2_record = load_gilt_p2_data_from_sheet(spreadsheet, farm_name, week_id)
        if gilt_p2_record:
            gilt_p2_row = gilt_p2_record
    
    if gilt_p2_row:
        st.write(f"**種付開始週:** {week_id}")
        
        p2_columns = [str(i) for i in range(4, 21)]
        gilt_p2_data = []
        gilt_total_count = 0
        gilt_weighted_sum = 0
        
        for p2 in p2_columns:
            if p2 in gilt_p2_row:
                try:
                    count = int(gilt_p2_row[p2])
                    gilt_total_count += count
                    gilt_weighted_sum += int(p2) * count
                    gilt_p2_data.append({'P2値(mm)': int(p2), '頭数': count})
                except:
                    pass
        
        if gilt_total_count > 0:
            gilt_average_p2 = gilt_weighted_sum / gilt_total_count
            
            col_chart_gilt, col_table_gilt = st.columns(2)
            
            with col_chart_gilt:
                import altair as alt
                df_gilt_p2_chart = pd.DataFrame(gilt_p2_data)
                df_gilt_p2_chart = df_gilt_p2_chart.sort_values('P2値(mm)')
                df_gilt_p2_chart['P2値'] = df_gilt_p2_chart['P2値(mm)'].astype(str) + 'mm'
                
                chart_gilt = alt.Chart(df_gilt_p2_chart).mark_bar(color='#ff7f0e').encode(
                    x=alt.X('P2値:N', sort=df_gilt_p2_chart['P2値'].tolist(), title='P2値'),
                    y=alt.Y('頭数:Q', title='頭数'),
                    tooltip=['P2値', '頭数']
                ).properties(height=300)
                st.altair_chart(chart_gilt, use_container_width=True)
            
            with col_table_gilt:
                df_gilt_p2_table = pd.DataFrame(gilt_p2_data)
                df_gilt_p2_table = df_gilt_p2_table[df_gilt_p2_table['頭数'] > 0]
                df_gilt_p2_table = df_gilt_p2_table.sort_values('P2値(mm)')
                df_gilt_p2_table['P2値(mm)'] = df_gilt_p2_table['P2値(mm)'].astype(str) + 'mm'
                display_centered_table(df_gilt_p2_table, height=300)
            
            st.write(f"**合計:** {gilt_total_count}頭 / **平均P2値:** {gilt_average_p2:.1f}mm")
        else:
            st.info("初産P2値データがありません")
    else:
        st.info(f"種付開始週 {week_id} に対応する初産P2値データがスプレッドシートに登録されていません")
    
    # ===================
    # 採精レポート（花泉1号・花泉2号のみ）
    # ===================
    if farm_name in ["花泉1号", "花泉2号"]:
        st.subheader("【採精レポート】")
        
        # スプレッドシートまたはアップロードファイルからデータを取得
        df_semen_week = None
        
        # まずスプレッドシートから読み込み
        if spreadsheet:
            df_semen_week = load_semen_report_from_sheet(spreadsheet, start_date)
        
        if df_semen_week is not None and len(df_semen_week) > 0:
            # 対象期間を計算
            days_since_monday = start_date.weekday()
            if days_since_monday == 0:
                previous_sunday = start_date - timedelta(days=1)
            else:
                previous_sunday = start_date - timedelta(days=days_since_monday + 1)
            
            days_until_saturday = 5 - start_date.weekday()
            if days_until_saturday < 0:
                days_until_saturday += 7
            saturday_of_week = start_date + timedelta(days=days_until_saturday)
            
            st.write(f"**対象期間:** {previous_sunday.strftime('%Y-%m-%d')} ～ {saturday_of_week.strftime('%Y-%m-%d')}")
            
            # 表示用に整形
            display_cols = ['採精日', '個体番号', '採精量', '精子数', '備考']
            available_cols = [col for col in display_cols if col in df_semen_week.columns]
            df_semen_display = df_semen_week[available_cols].copy()
            
            # 採精日を文字列に変換
            if '採精日' in df_semen_display.columns:
                df_semen_display['採精日'] = pd.to_datetime(df_semen_display['採精日']).dt.strftime('%Y-%m-%d')
            
            # 備考のNaNを空文字に
            if '備考' in df_semen_display.columns:
                df_semen_display['備考'] = df_semen_display['備考'].fillna('').astype(str)
            
            # 列名を変更
            df_semen_display.columns = ['採精日', '個体番号', '採精量(ml)', '精子数(億)', '備考'][:len(available_cols)]
            
            display_centered_table(df_semen_display)
        else:
            st.info("この週の採精レポートがスプレッドシートに登録されていません")
    
    # ===================
    # 週全体のコメント
    # ===================
    st.subheader("【週のコメント】")
    
    comment_key = f"{farm_name}_{week_id}"
    saved_week_comment = comments_data["week_comments"].get(comment_key, "")
    
    if st.session_state.edit_mode:
        # 編集モード：入力欄を表示
        week_comment = st.text_area(
            "この週の鑑定落ちリストに対するコメント",
            value=saved_week_comment,
            height=150,
            placeholder="必要妊豚在庫の確保状況、不受胎の原因分析、今後の対応など",
            key="week_comment_input"
        )
        st.session_state.temp_week_comment = week_comment
    else:
        # 閲覧モード：保存済みコメントを表示
        week_comment = saved_week_comment
        if week_comment:
            # コメントを表形式で表示（左詰め）
            st.markdown(f"""
            <div style="border: 1px solid #ddd; padding: 15px; background-color: white; color: #333; border-radius: 5px; margin: 10px 0; text-align: left; white-space: pre-wrap; font-size: 14px;">
{week_comment}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("コメントはありません")
    
    # ===================
    # 保存ボタン
    # ===================
    st.divider()
    
    if st.session_state.edit_mode:
        col_save, col_pdf, col_status = st.columns([1, 1, 2])
    else:
        col_pdf, col_status = st.columns([1, 3])
    
    if st.session_state.edit_mode:
        with col_save:
            if st.button("💾 データを保存", type="primary"):
                with st.spinner("💾 データを保存中...しばらくお待ちください"):
                    # 種付記録を保存
                    save_breeding_records(spreadsheet, df.drop(columns=['受胎']), week_id, farm_name)
                    
                    # キーのプレフィックス
                    key_prefix = f"{farm_name}_{week_id}"
                    
                    # 週コメントをセッションステートから取得
                    current_week_comment = st.session_state.get('temp_week_comment', '')
                    
                    # 手入力データを保存
                    save_data = {
                        "pig_details": st.session_state.temp_pig_details if 'temp_pig_details' in st.session_state else {},
                        "repeat_breeding": {key_prefix: st.session_state.temp_repeat_breeding} if 'temp_repeat_breeding' in st.session_state else {},
                        "week_comments": {key_prefix: current_week_comment}
                    }
                    
                    success = save_data_to_sheet(spreadsheet, save_data, week_id, farm_name)
                    
                    if success:
                        st.success("✅ データを保存しました！")
                        st.cache_resource.clear()
                        st.cache_data.clear()
                    else:
                        st.error("データの保存に失敗しました")

    with col_pdf:
        # P2値データの準備（スプレッドシートから取得）
        p2_data = None
        gilt_p2_data = None
        semen_report = None
        
        # 経産P2値
        try:
            df_sow_for_p2 = df[df['産次'].astype(int) >= 2]
            if len(df_sow_for_p2) > 0 and df_sow_for_p2['前回離乳日'].notna().any():
                most_common_weaning = df_sow_for_p2['前回離乳日'].value_counts().idxmax()
                
                if spreadsheet:
                    p2_record = load_p2_data_from_sheet(spreadsheet, farm_name, most_common_weaning)
                    if p2_record:
                        p2_columns = [str(i) for i in range(4, 21)]
                        p2_table_data = []
                        total_count = 0
                        weighted_sum = 0
                        for p2 in p2_columns:
                            if p2 in p2_record:
                                try:
                                    count = int(p2_record[p2])
                                    if count > 0:
                                        total_count += count
                                        weighted_sum += int(p2) * count
                                        p2_table_data.append({'P2値(mm)': f"{p2}mm", '頭数': count})
                                except:
                                    pass
                        if total_count > 0:
                            p2_data = {
                                'weaning_date': most_common_weaning,
                                'lot': p2_record.get('離乳ロット', ''),
                                'average': weighted_sum / total_count,
                                'table': pd.DataFrame(p2_table_data)
                            }
        except:
            pass
        
        # 初産P2値
        try:
            if spreadsheet:
                gilt_p2_record = load_gilt_p2_data_from_sheet(spreadsheet, farm_name, week_id)
                if gilt_p2_record:
                    p2_columns = [str(i) for i in range(4, 21)]
                    gilt_p2_table_data = []
                    gilt_total_count = 0
                    gilt_weighted_sum = 0
                    for p2 in p2_columns:
                        if p2 in gilt_p2_record:
                            try:
                                count = int(gilt_p2_record[p2])
                                if count > 0:
                                    gilt_total_count += count
                                    gilt_weighted_sum += int(p2) * count
                                    gilt_p2_table_data.append({'P2値(mm)': f"{p2}mm", '頭数': count})
                            except:
                                pass
                    if gilt_total_count > 0:
                        gilt_p2_data = {
                            'average': gilt_weighted_sum / gilt_total_count,
                            'table': pd.DataFrame(gilt_p2_table_data)
                        }
        except:
            pass
        
        # 採精レポート（花泉1号・花泉2号のみ）
        if farm_name in ["花泉1号", "花泉2号"]:
            try:
                if spreadsheet:
                    df_semen_week = load_semen_report_from_sheet(spreadsheet, start_date)
                    if df_semen_week is not None and len(df_semen_week) > 0:
                        display_cols = ['採精日', '個体番号', '採精量', '精子数', '備考']
                        available_cols = [col for col in display_cols if col in df_semen_week.columns]
                        semen_report = df_semen_week[available_cols].copy()
                        if '採精日' in semen_report.columns:
                            semen_report['採精日'] = pd.to_datetime(semen_report['採精日']).dt.strftime('%Y-%m-%d')
                        if '備考' in semen_report.columns:
                            semen_report['備考'] = semen_report['備考'].fillna('').astype(str)
                        semen_report.columns = ['採精日', '個体番号', '採精量(ml)', '精子数(億)', '備考'][:len(available_cols)]
            except:
                pass
        
        # 印刷用HTML生成
        print_html = generate_print_html(
            df=df,
            week_id=week_id,
            farm_name=farm_name,
            start_date=start_date,
            end_date=end_date,
            comments_data=comments_data,
            df_parity=df_parity,
            semen_stats=semen_stats,
            df_not_pregnant=df_not_pregnant,
            week_comment=week_comment,
            p2_data=p2_data,
            gilt_p2_data=gilt_p2_data,
            semen_report=semen_report
        )
        
        # HTMLダウンロードボタン
        st.download_button(
            label="印刷用ページ",
            data=print_html,
            file_name=f"鑑定落ちリスト_{farm_name}_{week_id}.html",
            mime="text/html",
            help="ダウンロード後、ブラウザで開いて印刷（Cmd+P）でPDF保存できます"
        )
    
    with col_status:
        is_saved = farm_name in farm_weeks and week_id in farm_weeks.get(farm_name, [])
        if is_saved:
            st.caption(f"✅ この週のデータは保存済みです")
        else:
            st.caption(f"⚠️ この週のデータはまだ保存されていません")

else:
    st.info("👈 サイドバーからデータを選択してください")
    
    st.markdown("""
    ### 使い方
    
    **方法1: 新しいCSVをアップロード**
    1. サイドバーで「CSVをアップロード」を選択
    2. 種付記録CSVをアップロード
    4. 必定な情報を手入力
    3. レポートを確認し、「データを保存」をクリック
    
    **方法2: 過去データを閲覧**
    1. サイドバーで「過去データから選択」を選択
    2. 閲覧したい農場・週を選ぶ
    3. レポートが表示されます
    4. 編集が必要であれば「編集する」を選択し編集
    5. レポートを確認し、「データを保存」をクリック
    
    **PDFレポート出力方法**
    1. 方法1または方法2のいずれかでレポートを作成後、最下部にある「印刷用ページ」を選択
    2. HTML形式でレポートがダウンロードされます
    3. ダウンロードされたレポートをブラウザで開く
    4. 「印刷/PDF」を選択しPDFで保存する
    
    """)
    
    if all_farms:
        st.write("**保存済みのデータ:**")
        for farm in all_farms[:3]:
            weeks = farm_weeks.get(farm, [])
            st.write(f"- {farm}: {len(weeks)}週分")
        if len(all_farms) > 3:
            st.write(f"...他 {len(all_farms) - 3} 農場")