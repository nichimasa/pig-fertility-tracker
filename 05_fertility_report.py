import pandas as pd
from datetime import datetime

# CSVファイルを読み込む
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')

# 受胎判定
df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'

# ===================
# レポートのヘッダー
# ===================
print("=" * 60)
print("           鑑 定 落 ち リ ス ト")
print("=" * 60)

# 種付期間
start_date = df['種付日'].min()
end_date = df['種付日'].max()
print(f"\n種付期間: {start_date} ～ {end_date}")

# ===================
# 1. 全体の受胎率
# ===================
total = len(df)
pregnant = df['受胎'].sum()
not_pregnant = total - pregnant
fertility_rate = pregnant / total * 100

print("\n" + "-" * 40)
print("【全体の受胎率】")
print("-" * 40)
print(f"  合計: {pregnant}/{total} = {fertility_rate:.1f}%")

# 経産・初産別
df_gilt = df[df['産次'] == 1]
df_sow = df[df['産次'] >= 2]

gilt_rate = df_gilt['受胎'].sum() / len(df_gilt) * 100 if len(df_gilt) > 0 else 0
sow_rate = df_sow['受胎'].sum() / len(df_sow) * 100 if len(df_sow) > 0 else 0

print(f"  経産: {df_sow['受胎'].sum()}/{len(df_sow)} = {sow_rate:.1f}%")
print(f"  初産: {df_gilt['受胎'].sum()}/{len(df_gilt)} = {gilt_rate:.1f}%")

# ===================
# 2. 産次別の受胎率
# ===================
print("\n" + "-" * 40)
print("【産次別の受胎率】")
print("-" * 40)

for parity in sorted(df['産次'].unique()):
    df_p = df[df['産次'] == parity]
    p_total = len(df_p)
    p_pregnant = df_p['受胎'].sum()
    p_rate = p_pregnant / p_total * 100 if p_total > 0 else 0
    print(f"  {parity}産: {p_pregnant}/{p_total} = {p_rate:.1f}%")

# ===================
# 3. 精液別の受胎率
# ===================
print("\n" + "-" * 40)
print("【精液別の受胎率】")
print("-" * 40)

semen_stats = df.groupby('雄豚・精液・あて雄').agg(
    種付頭数=('受胎', 'count'),
    受胎頭数=('受胎', 'sum')
).reset_index()
semen_stats['受胎率'] = semen_stats['受胎頭数'] / semen_stats['種付頭数'] * 100
semen_stats = semen_stats.sort_values('種付頭数', ascending=False)

for _, row in semen_stats.iterrows():
    print(f"  {row['雄豚・精液・あて雄']}: {int(row['受胎頭数'])}/{int(row['種付頭数'])} = {row['受胎率']:.1f}%")

# ===================
# 4. 不受胎リスト
# ===================
print("\n" + "-" * 40)
print("【不受胎リスト】")
print("-" * 40)

df_not_pregnant = df[df['受胎'] == False].copy()

if len(df_not_pregnant) > 0:
    # 表示用に列を選択・整形
    display_cols = ['種付日', '母豚番号', '雄豚・精液・あて雄', '産次', '再発日', '流産日', '母豚廃用日']
    df_display = df_not_pregnant[display_cols].copy()
    df_display.columns = ['種付日', '母豚番号', '精液', '産次', '再発日', '流産日', '廃用日']
    
    # NaNを空文字に変換して見やすく
    df_display = df_display.fillna('')
    
    print(df_display.to_string(index=False))
else:
    print("  不受胎なし")

# ===================
# 5. 産次別頭数
# ===================
print("\n" + "-" * 40)
print("【産次別頭数】")
print("-" * 40)

parity_counts = df['産次'].value_counts().sort_index()
for parity, count in parity_counts.items():
    print(f"  {parity}産: {count}頭")

print("\n" + "=" * 60)
print("                レポート終了")
print("=" * 60)