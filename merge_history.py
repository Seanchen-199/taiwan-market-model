"""
台股多因子模型 - 合併所有年份歷史資料
執行：python merge_history.py
輸入：data/history/YYYY.csv（各年份）
輸出：data/raw_data.csv（完整歷史）
"""

import subprocess, sys, os, warnings
def install(pkg):
    subprocess.check_call([sys.executable,'-m','pip','install',pkg,'-q'])
install('pandas'); install('numpy')

import pandas as pd
import numpy as np
warnings.filterwarnings('ignore')

print("合併歷史資料...")

# 讀取所有年份 CSV
hist_dir = 'data/history'
all_files = sorted([f for f in os.listdir(hist_dir) if f.endswith('.csv') and f != 'progress.json'])
print(f"找到 {len(all_files)} 個年份檔案：{[f.replace('.csv','') for f in all_files]}")

dfs = []
for fname in all_files:
    try:
        df = pd.read_csv(os.path.join(hist_dir, fname), index_col='date', parse_dates=True)
        dfs.append(df)
        print(f"  ✅ {fname}: {len(df)} 列")
    except Exception as e:
        print(f"  ❌ {fname}: {e}")

if not dfs:
    print("❌ 沒有找到任何資料")
    sys.exit(1)

master = pd.concat(dfs).sort_index()
master = master[~master.index.duplicated(keep='last')]

# ── 計算衍生欄位 ─────────────────────────────────────────────
print("\n計算衍生指標...")

if 'TWII' in master.columns:
    p = master['TWII']
    master['MA5']  = p.rolling(5).mean()
    master['MA20'] = p.rolling(20).mean()
    master['MA60'] = p.rolling(60).mean()
    delta = p.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, float('nan'))
    master['RSI14']       = 100 - (100/(1+rs))
    ema12 = p.ewm(span=12,adjust=False).mean()
    ema26 = p.ewm(span=26,adjust=False).mean()
    master['MACD']        = ema12 - ema26
    master['MACD_signal'] = master['MACD'].ewm(span=9,adjust=False).mean()
    master['MACD_hist']   = master['MACD'] - master['MACD_signal']

if 'margin_balance' in master.columns:
    master['margin_chg5'] = master['margin_balance'].pct_change(5)

if 'ETF0050' in master.columns:
    master['target_5d_return'] = master['ETF0050'].pct_change(5).shift(-5)
    master['target_signal']    = (master['target_5d_return'] > 0).astype(int)

# 前向填補
master = master.ffill(limit=5)

# 只保留有核心資料的列
core = [c for c in ['TWII','SOX','DXY','USDTWD'] if c in master.columns]
master = master.dropna(subset=core)

# ── 輸出統計 ─────────────────────────────────────────────────
print(f"\n最終資料：{master.shape[0]} 列 × {master.shape[1]} 欄")
print(f"日期範圍：{master.index[0].date()} ～ {master.index[-1].date()}")
print(f"\n欄位缺值率：")
for col in master.columns:
    pct = master[col].isna().mean() * 100
    st  = "✅" if pct < 20 else "⚠️" if pct < 60 else "❌"
    print(f"  {st} {col:30s} {pct:.1f}%")

master.index.name = 'date'
master.to_csv('data/raw_data.csv')
print(f"\n✅ 已儲存：data/raw_data.csv")
print(f"下一步：執行 python build_model.py")
