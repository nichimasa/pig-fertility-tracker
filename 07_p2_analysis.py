import pandas as pd

# ===================
# 1. データ読み込み
# ===================
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')
df_p2 = pd.read_excel('data/P2値集計表.xlsx', header=1)

# 離乳日を文字列に変換
df_p2['離乳日_str'] = df_p2['離乳日'].astype(str).str[:10]

# ===================
# 2. 最も多い離乳日を特定
# ===================
# 経産豚のみ（初産は前回離乳日がない）
df_sow = df[df['産次'] >= 2]
most_common_weaning_date = df_sow['前回離乳日'].value_counts().idxmax()
weaning_count = df_sow['前回離乳日'].value_counts().max()

print(f"対象離乳日: {most_common_weaning_date}（{weaning_count}頭）")

# ===================
# 3. P2値データを取得
# ===================
matched_p2 = df_p2[df_p2['離乳日_str'] == most_common_weaning_date]

if len(matched_p2) == 0:
    print("対応するP2値データが見つかりませんでした")
    exit()

# P2値の列（4〜20）を取得
p2_columns = [str(i) for i in range(4, 21)]
p2_row = matched_p2.iloc[0]

# ===================
# 4. P2値分布の集計
# ===================
print("\n" + "=" * 50)
print("【離乳時P2値分布】")
print("=" * 50)
print(f"離乳日: {most_common_weaning_date}")
print(f"離乳ロット: {p2_row['離乳ロット']}")
print()

# 分布を表示
print("P2値  | 頭数 | グラフ")
print("-" * 40)

total_count = 0
weighted_sum = 0

for p2 in p2_columns:
    count = int(p2_row[p2])
    total_count += count
    weighted_sum += int(p2) * count
    
    # 頭数がある場合のみ表示
    if count > 0:
        bar = "█" * count
        print(f"  {p2:>2}  |  {count:>2}  | {bar}")

# ===================
# 5. 平均P2値を計算
# ===================
average_p2 = weighted_sum / total_count if total_count > 0 else 0

print("-" * 40)
print(f"合計: {total_count}頭")
print(f"平均P2値: {average_p2:.1f}")

# ===================
# 6. 分布のサマリー
# ===================
print("\n" + "=" * 50)
print("【P2値サマリー】")
print("=" * 50)

# P2値を範囲でグループ化
low = sum(int(p2_row[str(i)]) for i in range(4, 9))      # 4-8: 低い
medium = sum(int(p2_row[str(i)]) for i in range(9, 13))  # 9-12: 適正
high = sum(int(p2_row[str(i)]) for i in range(13, 21))   # 13-20: 高い

print(f"  低い (4-8mm):   {low}頭 ({low/total_count*100:.1f}%)")
print(f"  適正 (9-12mm):  {medium}頭 ({medium/total_count*100:.1f}%)")
print(f"  高い (13-20mm): {high}頭 ({high/total_count*100:.1f}%)")