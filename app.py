import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import gspread
from google.oauth2.service_account import Credentials

# ページの設定
st.set_page_config(
    page_title="鑑定落ちリスト",
    page_icon="🐷",
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
        
        # Streamlit Cloud の場合は Secrets から認証情報を取得
        if 'gcp_service_account' in st.secrets:
            # Secretsの内容を辞書に変換
            creds_dict = dict(st.secrets["gcp_service_account"])
            credentials = Credentials.from_service_account_info(
                creds_dict,
                scopes=scopes
            )
        # ローカルの場合は credentials.json を使用
        elif os.path.exists(CREDENTIALS_FILE):
            credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
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
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
    return worksheet

def load_data_from_sheet(spreadsheet):
    """スプレッドシートからデータを読み込み"""
    data = {"pig_details": {}, "repeat_breeding": {}, "week_comments": {}}
    
    try:
        # 母豚詳細
        ws_pig = get_or_create_worksheet(spreadsheet, "母豚詳細")
        records = ws_pig.get_all_records()
        for record in records:
            if record.get("key"):
                data["pig_details"][record["key"]] = {
                    "分娩舎": record.get("分娩舎", ""),
                    "ロット": record.get("ロット", ""),
                    "哺乳日数": record.get("哺乳日数", ""),
                    "P2値": record.get("P2値", ""),
                    "コメント": record.get("コメント", "")
                }
        
        # 再発付け
        ws_repeat = get_or_create_worksheet(spreadsheet, "再発付け")
        records = ws_repeat.get_all_records()
        for record in records:
            if record.get("week_id"):
                data["repeat_breeding"][record["week_id"]] = {
                    "種付": str(record.get("種付", "")),
                    "受胎": str(record.get("受胎", ""))
                }
        
        # 週コメント
        ws_comment = get_or_create_worksheet(spreadsheet, "週コメント")
        records = ws_comment.get_all_records()
        for record in records:
            if record.get("week_id"):
                data["week_comments"][record["week_id"]] = record.get("コメント", "")
    
    except Exception as e:
        st.warning(f"データ読み込み中にエラーが発生しました: {e}")
    
    return data

def save_data_to_sheet(spreadsheet, data, week_id):
    """スプレッドシートにデータを保存"""
    try:
        # 母豚詳細を保存
        ws_pig = get_or_create_worksheet(spreadsheet, "母豚詳細")
        existing_records = ws_pig.get_all_records()
        existing_keys = [r.get("key") for r in existing_records]
        
        # ヘッダーがなければ追加
        if not existing_records:
            ws_pig.update('A1:F1', [["key", "分娩舎", "ロット", "哺乳日数", "P2値", "コメント"]])
        
        for key, details in data["pig_details"].items():
            if key.startswith(week_id):
                row_data = [key, details.get("分娩舎", ""), details.get("ロット", ""), 
                           details.get("哺乳日数", ""), details.get("P2値", ""), details.get("コメント", "")]
                if key in existing_keys:
                    # 既存行を更新
                    row_index = existing_keys.index(key) + 2
                    ws_pig.update(f'A{row_index}:F{row_index}', [row_data])
                else:
                    # 新規行を追加
                    ws_pig.append_row(row_data)
        
        # 再発付けを保存
        ws_repeat = get_or_create_worksheet(spreadsheet, "再発付け")
        existing_records = ws_repeat.get_all_records()
        existing_weeks = [r.get("week_id") for r in existing_records]
        
        if not existing_records:
            ws_repeat.update('A1:C1', [["week_id", "種付", "受胎"]])
        
        if week_id in data["repeat_breeding"]:
            repeat_data = data["repeat_breeding"][week_id]
            row_data = [week_id, repeat_data.get("種付", ""), repeat_data.get("受胎", "")]
            if week_id in existing_weeks:
                row_index = existing_weeks.index(week_id) + 2
                ws_repeat.update(f'A{row_index}:C{row_index}', [row_data])
            else:
                ws_repeat.append_row(row_data)
        
        # 週コメントを保存
        ws_comment = get_or_create_worksheet(spreadsheet, "週コメント")
        existing_records = ws_comment.get_all_records()
        existing_weeks = [r.get("week_id") for r in existing_records]
        
        if not existing_records:
            ws_comment.update('A1:B1', [["week_id", "コメント"]])
        
        if week_id in data["week_comments"]:
            row_data = [week_id, data["week_comments"][week_id]]
            if week_id in existing_weeks:
                row_index = existing_weeks.index(week_id) + 2
                ws_comment.update(f'A{row_index}:B{row_index}', [row_data])
            else:
                ws_comment.append_row(row_data)
        
        return True
    
    except Exception as e:
        st.error(f"データ保存中にエラーが発生しました: {e}")
        return False

# ===================
# カスタムCSS
# ===================
st.markdown("""
<style>
    table { width: 100%; }
    th { text-align: center !important; }
    td { text-align: center !important; }
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

# ===================
# スプレッドシート接続
# ===================
spreadsheet = get_google_sheet()

if spreadsheet:
    st.sidebar.success("✅ Googleスプレッドシート接続済み")
    comments_data = load_data_from_sheet(spreadsheet)
else:
    st.sidebar.warning("⚠️ ローカル保存モードで動作中")
    # ローカルファイルから読み込み（フォールバック）
    COMMENTS_FILE = "comments_data.json"
    if os.path.exists(COMMENTS_FILE):
        with open(COMMENTS_FILE, 'r', encoding='utf-8') as f:
            comments_data = json.load(f)
    else:
        comments_data = {"pig_details": {}, "repeat_breeding": {}, "week_comments": {}}

if "pig_details" not in comments_data:
    comments_data["pig_details"] = {}
if "repeat_breeding" not in comments_data:
    comments_data["repeat_breeding"] = {}
if "week_comments" not in comments_data:
    comments_data["week_comments"] = {}

# タイトル
st.title("🐷 鑑定落ちリスト")
st.write("養豚場の受胎率管理システム")

# サイドバー：ファイルアップロード
st.sidebar.header("📁 データアップロード")

uploaded_csv = st.sidebar.file_uploader(
    "種付記録CSV（Porker出力）",
    type=['csv']
)

uploaded_p2 = st.sidebar.file_uploader(
    "P2値集計表（経産・Excel）",
    type=['xlsx']
)

uploaded_gilt_p2 = st.sidebar.file_uploader(
    "P2値集計表（初産・Excel）",
    type=['xlsx']
)

uploaded_semen = st.sidebar.file_uploader(
    "採精レポート（Excel）",
    type=['xlsx']
)

# メインコンテンツ
if uploaded_csv is not None:
    # ===================
    # データ読み込み
    # ===================
    df = pd.read_csv(uploaded_csv, encoding='utf-8-sig')
    df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'
    
    start_date = pd.to_datetime(df['種付日'].min())
    end_date = pd.to_datetime(df['種付日'].max())
    week_id = start_date.strftime('%Y-%m-%d')
    
    # ===================
    # ヘッダー情報
    # ===================
    st.header(f"📅 種付期間: {start_date.strftime('%Y-%m-%d')} ～ {end_date.strftime('%Y-%m-%d')}")
    st.caption(f"作成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # ===================
    # 受胎率サマリー
    # ===================
    st.subheader("【受胎率サマリー】")
    
    total = len(df)
    pregnant = df['受胎'].sum()
    fertility_rate = pregnant / total * 100
    
    df_sow = df[df['産次'] >= 2]
    sow_rate = df_sow['受胎'].sum() / len(df_sow) * 100 if len(df_sow) > 0 else 0
    
    df_gilt = df[df['産次'] == 1]
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
    # 2列レイアウト：産次別 & 精液別
    # ===================
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("【産次別受胎率】")
        
        parity_data = []
        for parity in sorted(df['産次'].unique()):
            df_p = df[df['産次'] == parity]
            p_total = len(df_p)
            p_pregnant = df_p['受胎'].sum()
            p_rate = p_pregnant / p_total * 100 if p_total > 0 else 0
            parity_data.append({
                '産次': f"{parity}産",
                '受胎': int(p_pregnant),
                '種付': p_total,
                '受胎率': f"{p_rate:.1f}%"
            })
        
        # 再発付けデータを取得
        saved_repeat = comments_data["repeat_breeding"].get(week_id, {"種付": "", "受胎": ""})
        
        # セッションステートの初期化
        if 'temp_repeat_breeding' not in st.session_state:
            st.session_state.temp_repeat_breeding = saved_repeat
        
        # 再発付けの受胎率を計算して追加
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
        
        # 再発付け入力フォーム
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
    
    # ===================
    # 不受胎リスト
    # ===================
    st.subheader("【不受胎リスト】")
    
    df_not_pregnant = df[df['受胎'] == False].copy()
    
    if len(df_not_pregnant) > 0:
        if 'temp_pig_details' not in st.session_state:
            st.session_state.temp_pig_details = {}
        
        st.write("**不受胎母豚の詳細情報を入力**")
        
        for idx, row in df_not_pregnant.iterrows():
            pig_id = str(row['母豚番号'])
            detail_key = f"{week_id}_{pig_id}"
            
            saved_details = comments_data["pig_details"].get(detail_key, {})
            
            with st.expander(f"🐷 {pig_id}（{row['産次']}産 / {row['雄豚・精液・あて雄']}）", expanded=False):
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    bunben = st.text_input(
                        "分娩舎",
                        value=saved_details.get("分娩舎", ""),
                        key=f"bunben_{detail_key}",
                        placeholder="例: 1号"
                    )
                
                with col2:
                    lot = st.text_input(
                        "ロット",
                        value=saved_details.get("ロット", ""),
                        key=f"lot_{detail_key}",
                        placeholder="例: 2-3"
                    )
                
                with col3:
                    honyugs = st.text_input(
                        "哺乳日数",
                        value=saved_details.get("哺乳日数", ""),
                        key=f"honyu_{detail_key}",
                        placeholder="例: 21"
                    )
                
                with col4:
                    p2_value = st.text_input(
                        "P2値",
                        value=saved_details.get("P2値", ""),
                        key=f"p2_{detail_key}",
                        placeholder="例: 12"
                    )
                
                comment = st.text_input(
                    "コメント",
                    value=saved_details.get("コメント", ""),
                    key=f"comment_{detail_key}",
                    placeholder="廃用理由、治療歴、助産歴など"
                )
                
                st.session_state.temp_pig_details[detail_key] = {
                    "分娩舎": to_halfwidth(bunben),
                    "ロット": to_halfwidth(lot),
                    "哺乳日数": to_halfwidth(honyugs),
                    "P2値": to_halfwidth(p2_value),
                    "コメント": comment
                }
        
        st.write("**不受胎一覧表**")
        
        display_data = []
        for idx, row in df_not_pregnant.iterrows():
            pig_id = str(row['母豚番号'])
            detail_key = f"{week_id}_{pig_id}"
            
            details = st.session_state.temp_pig_details.get(
                detail_key, 
                comments_data["pig_details"].get(detail_key, {})
            )
            
            hormone = row['投与ホルモン'] if pd.notna(row['投与ホルモン']) else ''
            days_after_weaning = row['離乳後交配日数'] if pd.notna(row['離乳後交配日数']) else ''
            if days_after_weaning != '':
                days_after_weaning = int(days_after_weaning)
            
            display_data.append({
                '種付日': row['種付日'],
                '母豚番号': pig_id,
                '精液': row['雄豚・精液・あて雄'],
                '分娩予定日': row['分娩予定日'],
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
    if uploaded_p2 is not None:
        st.subheader("【離乳時P2値分布（経産）】")
        
        df_p2 = pd.read_excel(uploaded_p2, header=1)
        df_p2['離乳日_str'] = df_p2['離乳日'].astype(str).str[:10]
        
        df_sow_for_p2 = df[df['産次'] >= 2]
        if len(df_sow_for_p2) > 0 and df_sow_for_p2['前回離乳日'].notna().any():
            most_common_weaning = df_sow_for_p2['前回離乳日'].value_counts().idxmax()
            matched_p2 = df_p2[df_p2['離乳日_str'] == most_common_weaning]
            
            if len(matched_p2) > 0:
                p2_row = matched_p2.iloc[0]
                
                st.write(f"**離乳日:** {most_common_weaning} / **ロット:** {p2_row['離乳ロット']}")
                
                p2_columns = [str(i) for i in range(4, 21)]
                p2_data = []
                total_count = 0
                weighted_sum = 0
                
                for p2 in p2_columns:
                    if p2 in p2_row.index:
                        count = int(p2_row[p2])
                        total_count += count
                        weighted_sum += int(p2) * count
                        p2_data.append({'P2値(mm)': int(p2), '頭数': count})
                
                average_p2 = weighted_sum / total_count if total_count > 0 else 0
                
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
                    ).properties(
                        height=300
                    )
                    
                    st.altair_chart(chart, use_container_width=True)
                
                with col_table:
                    df_p2_table = pd.DataFrame(p2_data)
                    df_p2_table = df_p2_table[df_p2_table['頭数'] > 0]
                    df_p2_table = df_p2_table.sort_values('P2値(mm)')
                    df_p2_table['P2値(mm)'] = df_p2_table['P2値(mm)'].astype(str) + 'mm'
                    
                    display_centered_table(df_p2_table, height=300)
                
                st.write(f"**合計:** {total_count}頭 / **平均P2値:** {average_p2:.1f}mm")
            else:
                st.warning(f"離乳日 {most_common_weaning} に対応するP2値データが見つかりません")
        else:
            st.warning("経産豚の離乳データがありません")
    
    # ===================
    # P2値分布（初産）
    # ===================
    if uploaded_gilt_p2 is not None:
        st.subheader("【種付時P2値分布（初産）】")
        
        df_gilt_p2 = pd.read_excel(uploaded_gilt_p2, header=1)
        df_gilt_p2['種付開始週_str'] = df_gilt_p2['種付開始週'].astype(str).str[:10]
        
        matched_gilt_p2 = df_gilt_p2[df_gilt_p2['種付開始週_str'] == week_id]
        
        if len(matched_gilt_p2) > 0:
            gilt_p2_row = matched_gilt_p2.iloc[0]
            
            st.write(f"**種付開始週:** {week_id}")
            
            p2_columns = [str(i) for i in range(4, 21)]
            gilt_p2_data = []
            gilt_total_count = 0
            gilt_weighted_sum = 0
            
            for p2 in p2_columns:
                if p2 in gilt_p2_row.index:
                    count = int(gilt_p2_row[p2])
                    gilt_total_count += count
                    gilt_weighted_sum += int(p2) * count
                    gilt_p2_data.append({'P2値(mm)': int(p2), '頭数': count})
            
            gilt_average_p2 = gilt_weighted_sum / gilt_total_count if gilt_total_count > 0 else 0
            
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
                ).properties(
                    height=300
                )
                
                st.altair_chart(chart_gilt, use_container_width=True)
            
            with col_table_gilt:
                df_gilt_p2_table = pd.DataFrame(gilt_p2_data)
                df_gilt_p2_table = df_gilt_p2_table[df_gilt_p2_table['頭数'] > 0]
                df_gilt_p2_table = df_gilt_p2_table.sort_values('P2値(mm)')
                df_gilt_p2_table['P2値(mm)'] = df_gilt_p2_table['P2値(mm)'].astype(str) + 'mm'
                
                display_centered_table(df_gilt_p2_table, height=300)
            
            st.write(f"**合計:** {gilt_total_count}頭 / **平均P2値:** {gilt_average_p2:.1f}mm")
        else:
            st.warning(f"種付開始週 {week_id} に対応する初産P2値データが見つかりません")
    
    # ===================
    # 採精レポート
    # ===================
    if uploaded_semen is not None:
        st.subheader("【採精レポート】")
        
        df_semen = pd.read_excel(uploaded_semen, header=2)
        df_semen['採精日'] = pd.to_datetime(df_semen['採精日'])
        
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
        
        df_semen_week = df_semen[
            (df_semen['採精日'] >= previous_sunday) & 
            (df_semen['採精日'] <= saturday_of_week)
        ]
        
        if len(df_semen_week) > 0:
            display_cols = ['採精日', '個体番号', '採精量', '精子数', '備考']
            df_semen_display = df_semen_week[display_cols].copy()
            df_semen_display['採精日'] = df_semen_display['採精日'].dt.strftime('%Y-%m-%d')
            df_semen_display['備考'] = df_semen_display['備考'].fillna('').astype(str)
            df_semen_display.columns = ['採精日', '個体番号', '採精量(ml)', '精子数(億)', '備考']
            
            display_centered_table(df_semen_display)
        else:
            st.info("対象期間の採精データがありません")
    
    # ===================
    # 週全体のコメント
    # ===================
    st.subheader("【週のコメント】")
    
    saved_week_comment = comments_data["week_comments"].get(week_id, "")
    
    if 'temp_week_comment' not in st.session_state:
        st.session_state.temp_week_comment = saved_week_comment
    
    week_comment = st.text_area(
        "この週の鑑定落ちリストに対するコメント",
        value=st.session_state.temp_week_comment,
        height=150,
        placeholder="必要妊豚在庫の確保状況、不受胎の原因分析、今後の対応など",
        key="week_comment_input"
    )
    st.session_state.temp_week_comment = week_comment
    
    # ===================
    # 保存ボタン
    # ===================
    st.divider()
    
    col_save, col_status = st.columns([1, 3])
    
    with col_save:
        if st.button("💾 データを保存", type="primary"):
            # データを準備
            save_data = {
                "pig_details": st.session_state.temp_pig_details,
                "repeat_breeding": {week_id: st.session_state.temp_repeat_breeding},
                "week_comments": {week_id: week_comment}
            }
            
            if spreadsheet:
                # Googleスプレッドシートに保存
                success = save_data_to_sheet(spreadsheet, save_data, week_id)
                if success:
                    st.success("✅ Googleスプレッドシートに保存しました！")
            else:
                # ローカルファイルに保存（フォールバック）
                for key, value in st.session_state.temp_pig_details.items():
                    if any(v for v in value.values()):
                        comments_data["pig_details"][key] = value
                
                if st.session_state.temp_repeat_breeding.get("種付") or st.session_state.temp_repeat_breeding.get("受胎"):
                    comments_data["repeat_breeding"][week_id] = st.session_state.temp_repeat_breeding
                
                if week_comment:
                    comments_data["week_comments"][week_id] = week_comment
                
                with open("comments_data.json", 'w', encoding='utf-8') as f:
                    json.dump(comments_data, f, ensure_ascii=False, indent=2)
                st.success("✅ ローカルに保存しました！")
    
    with col_status:
        detail_count = sum(1 for k in comments_data["pig_details"].keys() if k.startswith(week_id))
        week_comment_exists = week_id in comments_data["week_comments"]
        repeat_exists = week_id in comments_data["repeat_breeding"]
        st.caption(f"この週の保存済み: 母豚詳細 {detail_count}件 / 週コメント {'あり' if week_comment_exists else 'なし'} / 再発付け {'あり' if repeat_exists else 'なし'}")

else:
    st.info("👈 サイドバーから種付記録CSVをアップロードしてください")
    
    st.markdown("""
    ### 使い方
    1. **種付記録CSV**: Porkerからダウンロードした種付記録をアップロード
    2. **P2値集計表（経産）**: 離乳時P2値の集計表をアップロード（任意）
    3. **P2値集計表（初産）**: 種付時P2値の集計表をアップロード（任意）
    4. **採精レポート**: 採精情報をアップロード（任意）
    
    アップロードすると、自動で受胎率レポートが生成されます。
    """)
    
    # 保存データの確認
    if spreadsheet:
        st.sidebar.divider()
        st.sidebar.subheader("📊 保存データ")
        st.sidebar.write(f"保存済み週数: {len(comments_data['week_comments'])}週")
        st.sidebar.write(f"保存済み母豚詳細: {len(comments_data['pig_details'])}件")