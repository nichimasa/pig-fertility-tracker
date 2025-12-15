import pandas as pd

# ===================
# 1. データ読み込み
# ===================
df = pd.read_csv('data/種付記録一覧_20251123150908.csv', encoding='utf-8-sig')
df_semen = pd.read_excel('data/採精レポート.xlsx', header=2)

# 受胎判定
df['受胎'] = df['妊娠鑑定結果'] == '受胎確定'

print("=== 採精レポートの列名 ===")
print(df_semen.columns.tolist())

print("\n=== 採精レポートの内容（最初の10行） ===")
print(df_semen.head(10))

# ===================
# 2. 種付期間を特定
# ===================
start_date = df['種付日'].min()
end_date = df['種付日'].max()
print(f"\n種付期間: {start_date} ～ {end_date}")

# ===================
# 3. 使用された精液を特定
# ===================
used_semen = df['雄豚・精液・あて雄'].unique()
print(f"\n使用された精液: {used_semen}")

# ===================
# 4. 採精日を文字列に変換
# ===================
df_semen['採精日_str'] = df_semen['採精日'].astype(str).str[:10]

print("\n=== 採精日一覧 ===")
print(df_semen['採精日_str'].unique())

# ===================
# 5. 種付期間前後の採精データを抽出
# ===================
# 種付日の1週間前から種付期間中の採精データを対象とする
print("\n=== 使用精液の採精情報 ===")
print("-" * 60)

for semen in used_semen:
    # その精液の採精データを取得
    semen_data = df_semen[df_semen['個体番号'] == semen]
    
    if len(semen_data) > 0:
        print(f"\n【{semen}】")
        # 最新の採精データを表示
        latest = semen_data.sort_values('採精日', ascending=False).iloc[0]
        print(f"  最新採精日: {str(latest['採精日'])[:10]}")
        print(f"  採精量: {latest['採精量']}ml")
        print(f"  精子数: {latest['精子数']}億")
        if pd.notna(latest['備考']) and latest['備考'] != '':
            print(f"  備考: {latest['備考']}")
    else:
        print(f"\n【{semen}】")
        print("  採精データなし")