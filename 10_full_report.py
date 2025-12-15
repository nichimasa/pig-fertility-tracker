import pandas as pd
from datetime import datetime, timedelta

# ===================
# データ読み込み
# ===================
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')
df_p2 = pd.read_excel('data/P2値集計表.xlsx', header=1)
df_semen = pd.read_excel('data/採精レポート.xlsx', header=2)

# 受胎判定
df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'

# 日付変換
df_p2['離乳日_str'] = df_p2['離乳日'].astype(str).str[:10]
df_semen['採精日'] = pd.to_datetime(df_semen['採精日'])

# 種付期間
start_date = pd.to_datetime(df['種付日'].min())
end_date = pd.to_datetime(df['種付日'].max())

# 採精対象期間
days_since_monday = start_date.weekday()
if days_since_monday == 0:
    previous_sunday = start_date - timedelta(days=1)
else:
    previous_sunday = start_date - timedelta(days=days_since_monday + 1)

days_until_saturday = 5 - start_date.weekday()
if days_until_saturday < 0:
    days_until_saturday += 7
saturday_of_week = start_date + timedelta(days=days_until_saturday)

# ===================
# レポート出力
# ===================
print("=" * 70)
print("                    鑑 定 落 ち リ ス ト")
print("=" * 70)
print(f"種付期間: {start_date.strftime('%Y-%m-%d')} ～ {end_date.strftime('%Y-%m-%d')}")
print(f"作成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ----- 1. 受胎率サマリー -----
total = len(df)
pregnant = df['受胎'].sum()
fertility_rate = pregnant / total * 100

df_gilt = df[df['産次'] == 1]
df_sow = df[df['産次'] >= 2]
gilt_rate = df_gilt['受胎'].sum() / len(df_gilt) * 100 if len(df_gilt) > 0 else 0
sow_rate = df_sow['受胎'].sum() / len(df_sow) * 100 if len(df_sow) > 0 else 0

print("\n" + "-" * 70)
print("【受胎率サマリー】")
print("-" * 70)
print(f"  {'区分':<10} {'受胎':>6} / {'種付':>6} = {'受胎率':>8}")
print(f"  {'-'*40}")
print(f"  {'合計':<10} {int(pregnant):>6} / {total:>6} = {fertility_rate:>7.1f}%")
print(f"  {'経産':<10} {int(df_sow['受胎'].sum()):>6} / {len(df_sow):>6} = {sow_rate:>7.1f}%")
print(f"  {'初産(Gilt)':<10} {int(df_gilt['受胎'].sum()):>6} / {len(df_gilt):>6} = {gilt_rate:>7.1f}%")

# ----- 2. 産次別受胎率 -----
print("\n" + "-" * 70)
print("【産次別受胎率】")
print("-" * 70)
print(f"  {'産次':<6} {'受胎':>6} / {'種付':>6} = {'受胎率':>8}")
print(f"  {'-'*40}")

for parity in sorted(df['産次'].unique()):
    df_p = df[df['産次'] == parity]
    p_total = len(df_p)
    p_pregnant = df_p['受胎'].sum()
    p_rate = p_pregnant / p_total * 100 if p_total > 0 else 0
    print(f"  {parity}産{'':<4} {int(p_pregnant):>6} / {p_total:>6} = {p_rate:>7.1f}%")

# ----- 3. 精液別受胎率 -----
print("\n" + "-" * 70)
print("【精液別受胎率】")
print("-" * 70)
print(f"  {'精液':<8} {'受胎':>6} / {'種付':>6} = {'受胎率':>8}")
print(f"  {'-'*40}")

semen_stats = df.groupby('雄豚・精液・あて雄').agg(
    種付頭数=('受胎', 'count'),
    受胎頭数=('受胎', 'sum')
).reset_index()
semen_stats = semen_stats.sort_values('種付頭数', ascending=False)

for _, row in semen_stats.iterrows():
    s_rate = row['受胎頭数'] / row['種付頭数'] * 100
    print(f"  {row['雄豚・精液・あて雄']:<8} {int(row['受胎頭数']):>6} / {int(row['種付頭数']):>6} = {s_rate:>7.1f}%")

# ----- 4. 不受胎リスト -----
print("\n" + "-" * 70)
print("【不受胎リスト】")
print("-" * 70)

df_not_pregnant = df[df['受胎'] == False].copy()

if len(df_not_pregnant) > 0:
    display_cols = ['種付日', '母豚番号', '雄豚・精液・あて雄', '産次', '再発日', '流産日', '母豚廃用日']
    df_display = df_not_pregnant[display_cols].copy()
    df_display.columns = ['種付日', '母豚番号', '精液', '産次', '再発日', '流産日', '廃用日']
    df_display = df_display.fillna('')
    print(df_display.to_string(index=False))
else:
    print("  不受胎なし")

# ----- 5. P2値分布 -----
print("\n" + "-" * 70)
print("【離乳時P2値分布】")
print("-" * 70)

df_sow_for_p2 = df[df['産次'] >= 2]
if len(df_sow_for_p2) > 0 and df_sow_for_p2['前回離乳日'].notna().any():
    most_common_weaning = df_sow_for_p2['前回離乳日'].value_counts().idxmax()
    matched_p2 = df_p2[df_p2['離乳日_str'] == most_common_weaning]
    
    if len(matched_p2) > 0:
        p2_row = matched_p2.iloc[0]
        print(f"  離乳日: {most_common_weaning} / ロット: {p2_row['離乳ロット']}")
        print()
        
        p2_columns = [str(i) for i in range(4, 21)]
        total_count = 0
        weighted_sum = 0
        
        print(f"  {'P2値':<6} {'頭数':>4}  分布")
        print(f"  {'-'*50}")
        
        for p2 in p2_columns:
            if p2 in p2_row.index:
                count = int(p2_row[p2])
                total_count += count
                weighted_sum += int(p2) * count
                if count > 0:
                    bar = "█" * min(count, 30)
                    print(f"  {p2:>4}mm {count:>4}  {bar}")
        
        average_p2 = weighted_sum / total_count if total_count > 0 else 0
        print(f"  {'-'*50}")
        print(f"  合計: {total_count}頭 / 平均P2値: {average_p2:.1f}mm")
    else:
        print("  対応するP2値データが見つかりません")
else:
    print("  経産豚の離乳データがありません")

# ----- 6. 採精レポート -----
print("\n" + "-" * 70)
print("【採精レポート】")
print(f"  対象期間: {previous_sunday.strftime('%Y-%m-%d')} ～ {saturday_of_week.strftime('%Y-%m-%d')}")
print("-" * 70)

df_semen_week = df_semen[
    (df_semen['採精日'] >= previous_sunday) & 
    (df_semen['採精日'] <= saturday_of_week)
]

if len(df_semen_week) > 0:
    print(f"  {'採精日':<12} {'個体':<6} {'採精量':>8} {'精子数':>8}  備考")
    print(f"  {'-'*60}")
    
    for _, row in df_semen_week.iterrows():
        date_str = row['採精日'].strftime('%Y-%m-%d')
        remark = str(row['備考']) if pd.notna(row['備考']) else ''
        print(f"  {date_str:<12} {row['個体番号']:<6} {row['採精量']:>6}ml {row['精子数']:>6}億  {remark}")
else:
    print("  対象期間の採精データがありません")

print("\n" + "=" * 70)
print("                      レポート終了")
print("=" * 70)