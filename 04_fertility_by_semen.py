import pandas as pd

# CSVファイルを読み込む
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')

# 受胎判定
def is_pregnant(row):
    return row['妊娠鑑定結果'] == '受胎確定'

df['受胎'] = df.apply(is_pregnant, axis=1)

# ===================
# 精液別の受胎率
# ===================
print("=== 精液別の受胎率 ===")
print()

# 精液ごとにグループ化して集計
semen_stats = df.groupby('雄豚・精液・あて雄').agg(
    種付頭数=('受胎', 'count'),
    受胎頭数=('受胎', 'sum')
).reset_index()

# 受胎率を計算
semen_stats['受胎率'] = semen_stats['受胎頭数'] / semen_stats['種付頭数'] * 100

# 種付頭数の多い順にソート
semen_stats = semen_stats.sort_values('種付頭数', ascending=False)

# 表示
for _, row in semen_stats.iterrows():
    semen = row['雄豚・精液・あて雄']
    total = int(row['種付頭数'])
    pregnant = int(row['受胎頭数'])
    rate = row['受胎率']
    print(f"  {semen}: {pregnant}/{total} = {rate:.1f}%")

# ===================
# 見やすい表形式でも表示
# ===================
print("\n=== 表形式 ===")
semen_stats['受胎率'] = semen_stats['受胎率'].round(1).astype(str) + '%'
semen_stats.columns = ['精液', '種付頭数', '受胎頭数', '受胎率']
print(semen_stats.to_string(index=False))