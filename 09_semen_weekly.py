import pandas as pd
from datetime import datetime, timedelta

# ===================
# 1. データ読み込み
# ===================
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')
df_semen = pd.read_excel('data/採精レポート.xlsx', header=2)

# 受胎判定
df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'

# ===================
# 2. 種付期間を特定
# ===================
start_date = pd.to_datetime(df['種付日'].min())
end_date = pd.to_datetime(df['種付日'].max())

print(f"種付期間: {start_date.strftime('%Y-%m-%d')} ～ {end_date.strftime('%Y-%m-%d')}")

# ===================
# 3. 採精対象期間を計算
# ===================
# 直前の日曜日を計算
days_since_monday = start_date.weekday()  # 月曜=0, 日曜=6
if days_since_monday == 0:
    previous_sunday = start_date - timedelta(days=1)
else:
    previous_sunday = start_date - timedelta(days=days_since_monday + 1)

# 週末の土曜日を計算（日曜日は翌週分なので除外）
# 種付開始日の週の土曜日
days_until_saturday = 5 - start_date.weekday()  # 土曜=5
if days_until_saturday < 0:
    days_until_saturday += 7
saturday_of_week = start_date + timedelta(days=days_until_saturday)

print(f"直前の日曜日: {previous_sunday.strftime('%Y-%m-%d')}")
print(f"週末の土曜日: {saturday_of_week.strftime('%Y-%m-%d')}")
print(f"対象採精期間: {previous_sunday.strftime('%Y-%m-%d')} ～ {saturday_of_week.strftime('%Y-%m-%d')}")

# ===================
# 4. 採精日をdatetime型に変換
# ===================
df_semen['採精日'] = pd.to_datetime(df_semen['採精日'])

# ===================
# 5. 対象期間の採精データを抽出（日曜日は除外）
# ===================
df_semen_week = df_semen[
    (df_semen['採精日'] >= previous_sunday) & 
    (df_semen['採精日'] <= saturday_of_week)
]

print(f"\n対象期間の採精データ: {len(df_semen_week)}件")

# ===================
# 6. 使用された精液を特定
# ===================
used_semen = df['雄豚・精液・あて雄'].unique()
print(f"使用された精液: {used_semen}")

# ===================
# 7. 精液別の採精情報を表示
# ===================
print("\n" + "=" * 60)
print("【種付週の採精レポート】")
print("=" * 60)

for semen in used_semen:
    # その精液の対象期間の採精データを取得
    semen_data = df_semen_week[df_semen_week['個体番号'] == semen]
    
    # 受胎率も計算
    df_this_semen = df[df['雄豚・精液・あて雄'] == semen]
    semen_total = len(df_this_semen)
    semen_pregnant = df_this_semen['受胎'].sum()
    semen_rate = semen_pregnant / semen_total * 100 if semen_total > 0 else 0
    
    print(f"\n【{semen}】受胎率: {semen_pregnant}/{semen_total} = {semen_rate:.1f}%")
    print("-" * 50)
    
    if len(semen_data) > 0:
        for _, row in semen_data.iterrows():
            date_str = row['採精日'].strftime('%Y-%m-%d')
            print(f"  採精日: {date_str}")
            print(f"    採精量: {row['採精量']}ml")
            print(f"    精子数: {row['精子数']}億")
            print(f"    容量: {row['容量']}ml")
            print(f"    分注数: 1号={row['1号']}本, 2号={row['2号']}本")
            if pd.notna(row['備考']) and row['備考'] != '':
                print(f"    備考: {row['備考']}")
            print()
    else:
        if semen == 'D':
            print("  ※ 自家製精液（採精レポート対象外）")
        else:
            print("  ※ 対象期間の採精データなし")

# ===================
# 8. 採精データ一覧表
# ===================
print("\n" + "=" * 60)
print("【採精データ一覧】")
print("=" * 60)

if len(df_semen_week) > 0:
    display_cols = ['採精日', '個体番号', '採精量', '精子数', '備考']
    df_display = df_semen_week[display_cols].copy()
    df_display['採精日'] = df_display['採精日'].dt.strftime('%Y-%m-%d')
    # 修正：astype(str)で変換してから置換
    df_display['備考'] = df_display['備考'].fillna('').astype(str)
    df_display = df_display.fillna('')
    print(df_display.to_string(index=False))
else:
    print("対象期間の採精データがありません")