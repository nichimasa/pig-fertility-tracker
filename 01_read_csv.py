# 必要なライブラリをインポート
import pandas as pd

# CSVファイルを読み込む
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')

# データの最初の5行を表示
print("=== データの最初の5行 ===")
print(df.head())

# データの基本情報を表示
print("\n=== データの基本情報 ===")
print(f"行数: {len(df)}")
print(f"列数: {len(df.columns)}")

# 列名を表示
print("\n=== 列名一覧 ===")
for col in df.columns:
    print(f"  - {col}")