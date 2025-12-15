import pandas as pd

# ===================
# 1. 種付記録CSVを読み込む
# ===================
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')

# 受胎判定
df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'

# ===================
# 2. 前回離乳日の分布を確認
# ===================
print("=== 前回離乳日の分布 ===")
print(df['前回離乳日'].value_counts())

# 最も多い離乳日を取得
most_common_weaning_date = df['前回離乳日'].value_counts().idxmax()
print(f"\n最も多い離乳日: {most_common_weaning_date}")

# ===================
# 3. P2値集計表を読み込む
# ===================
df_p2 = pd.read_excel('data/P2値集計表.xlsx', header=1)

print("\n=== P2値集計表の列名 ===")
print(df_p2.columns.tolist())

print("\n=== P2値集計表の内容 ===")
print(df_p2.head())

# ===================
# 4. 離乳日でマッチング
# ===================
# 離乳日を文字列に変換して比較しやすくする
df_p2['離乳日_str'] = df_p2['離乳日'].astype(str).str[:10]

print(f"\n=== P2値集計表の離乳日一覧 ===")
print(df_p2['離乳日_str'].tolist())

# 最も多い離乳日に対応するP2値データを取得
matched_p2 = df_p2[df_p2['離乳日_str'] == most_common_weaning_date]

if len(matched_p2) > 0:
    print(f"\n=== {most_common_weaning_date} のP2値分布 ===")
    print(matched_p2)
else:
    print(f"\n※ {most_common_weaning_date} に対応するP2値データが見つかりませんでした")
    print("P2値集計表の離乳日と種付記録の前回離乳日の形式を確認してください")