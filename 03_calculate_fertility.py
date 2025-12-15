import pandas as pd

# CSVファイルを読み込む
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')

# ===================
# 受胎判定のロジック
# ===================
# 妊娠鑑定結果が「受胎確定」→ 受胎
# それ以外（不受胎、空白）→ 不受胎としてカウント

def is_pregnant(row):
    """受胎かどうかを判定する関数"""
    if row['妊娠鑑定結果'] == '受胎確定':
        return True
    else:
        return False

# 新しい列「受胎」を追加
df['受胎'] = df.apply(is_pregnant, axis=1)

# ===================
# 基本の受胎率計算
# ===================
total = len(df)
pregnant = df['受胎'].sum()  # Trueの数をカウント
fertility_rate = pregnant / total * 100

print("=== 全体の受胎率 ===")
print(f"種付頭数: {total}頭")
print(f"受胎確定: {pregnant}頭")
print(f"不受胎: {total - pregnant}頭")
print(f"受胎率: {fertility_rate:.1f}%")

# ===================
# 経産・初産別の受胎率
# ===================
print("\n=== 経産・初産別の受胎率 ===")

# 初産（産次 = 1）
df_gilt = df[df['産次'] == 1]
gilt_total = len(df_gilt)
gilt_pregnant = df_gilt['受胎'].sum()
gilt_rate = gilt_pregnant / gilt_total * 100 if gilt_total > 0 else 0

print(f"初産: {gilt_pregnant}/{gilt_total} = {gilt_rate:.1f}%")

# 経産（産次 >= 2）
df_sow = df[df['産次'] >= 2]
sow_total = len(df_sow)
sow_pregnant = df_sow['受胎'].sum()
sow_rate = sow_pregnant / sow_total * 100 if sow_total > 0 else 0

print(f"経産: {sow_pregnant}/{sow_total} = {sow_rate:.1f}%")

# ===================
# 産次別の受胎率
# ===================
print("\n=== 産次別の受胎率 ===")

for parity in sorted(df['産次'].unique()):
    df_parity = df[df['産次'] == parity]
    parity_total = len(df_parity)
    parity_pregnant = df_parity['受胎'].sum()
    parity_rate = parity_pregnant / parity_total * 100 if parity_total > 0 else 0
    print(f"  {parity}産: {parity_pregnant}/{parity_total} = {parity_rate:.1f}%")

# ===================
# 不受胎の詳細を確認
# ===================
print("\n=== 不受胎の内訳 ===")
df_not_pregnant = df[df['受胎'] == False]
print(df_not_pregnant[['種付日', '母豚番号', '産次', '妊娠鑑定結果', '再発日', '流産日', '母豚廃用日']].to_string(index=False))