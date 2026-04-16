"""
台股多因子模型 - 歷史資料收集腳本
在本機執行：python collect_history.py
會產生 data/raw_data.csv 供後續模型使用
"""

import subprocess, sys

def install(pkg):
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg, '-q'])

print("安裝必要套件...")
for pkg in ['yfinance', 'pandas', 'requests', 'scipy']:
    install(pkg)

import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import warnings
warnings.filterwarnings('ignore')

os.makedirs('data', exist_ok=True)

START = '2019-01-01'
END   = datetime.date.today().strftime('%Y-%m-%d')
print(f"\n資料區間：{START} ～ {END}\n")

# ════════════════════════════════════════════════════
# 1. Yahoo Finance 可直接抓的因子
# ════════════════════════════════════════════════════
print("=" * 50)
print("【1】抓取 Yahoo Finance 資料")
print("=" * 50)

YAHOO_SYMBOLS = {
    '^SOX':      'SOX',          # 費城半導體
    '^TWII':     'TWII',         # 台灣加權指數
    'DX-Y.NYB':  'DXY',          # 美元指數
    'USDTWD=X':  'USDTWD',       # 台幣匯率
    '^TNX':      'US10Y',        # 美國10年期公債殖利率
    '^VIX':      'VIX',          # VIX 恐慌指數
    '^IXIC':     'NASDAQ',       # 那斯達克
    '0050.TW':   'ETF0050',      # 元大台灣50
    '006208.TW': 'ETF006208',    # 富邦台灣50
}

yahoo_frames = {}
for sym, name in YAHOO_SYMBOLS.items():
    try:
        df = yf.download(sym, start=START, end=END, progress=False, auto_adjust=True)
        if df.empty:
            print(f"  [警告] {name}（{sym}）無資料")
            continue
        # 只取收盤價
        close = df['Close'].copy()
        if hasattr(close, 'squeeze'):
            close = close.squeeze()
        close.name = name
        yahoo_frames[name] = close
        print(f"  ✅ {name:12s} {len(close)} 筆  ({close.index[0].date()} ～ {close.index[-1].date()})")
    except Exception as e:
        print(f"  ❌ {name}（{sym}）失敗：{e}")

# 合併成一個 DataFrame，對齊日期
yahoo_df = pd.DataFrame(yahoo_frames)
yahoo_df.index = pd.to_datetime(yahoo_df.index)
yahoo_df = yahoo_df.sort_index()
print(f"\n  Yahoo 合計：{yahoo_df.shape[0]} 列 × {yahoo_df.shape[1]} 欄")

# ════════════════════════════════════════════════════
# 2. TWSE 三大法人（逐月抓取）
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【2】抓取 TWSE 三大法人歷史資料")
print("=" * 50)

def fetch_twse_institutional_month(year, month):
    """抓取單月三大法人資料"""
    date_str = f"{year}{month:02d}01"
    url = (
        f"https://www.twse.com.tw/rwd/zh/fund/T86"
        f"?response=json&date={date_str}&selectType=ALL"
    )
    try:
        resp = requests.get(url, timeout=20, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.twse.com.tw/'
        })
        data = resp.json()
        if data.get('stat') != 'OK' or not data.get('data'):
            return []

        rows = []
        for row in data['data']:
            try:
                # 欄位：日期, ..., 外資買超, ..., 投信買超, ..., 自營商買超, ...
                date_raw = str(row[0]).strip()
                # 日期格式：民國年/月/日 → 西元
                parts = date_raw.split('/')
                if len(parts) == 3:
                    roc_y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                    real_year = roc_y + 1911
                    date = datetime.date(real_year, m, d)
                else:
                    continue

                def pn(s):
                    try: return int(str(s).replace(',','').replace(' ',''))
                    except: return 0

                foreign_net = pn(row[4])
                invest_net  = pn(row[7])
                dealer_net  = pn(row[10])

                rows.append({
                    'date':        date,
                    'foreign_net': foreign_net,
                    'invest_net':  invest_net,
                    'dealer_net':  dealer_net,
                })
            except:
                continue
        return rows
    except Exception as e:
        return []

# 逐月抓取
import time
all_institutional = []
start_year, start_month = 2019, 1
now = datetime.date.today()

total_months = (now.year - start_year) * 12 + now.month - start_month + 1
done = 0

y, m = start_year, start_month
while (y, m) <= (now.year, now.month):
    rows = fetch_twse_institutional_month(y, m)
    all_institutional.extend(rows)
    done += 1
    if done % 6 == 0 or done == total_months:
        print(f"  進度：{done}/{total_months} 個月，已取 {len(all_institutional)} 筆")
    # 換月
    m += 1
    if m > 12:
        m = 1
        y += 1
    time.sleep(0.4)

if all_institutional:
    inst_df = pd.DataFrame(all_institutional)
    inst_df['date'] = pd.to_datetime(inst_df['date'])
    inst_df = inst_df.set_index('date').sort_index()
    # 換算成億元（原始單位：千元）
    inst_df['foreign_net_bil'] = inst_df['foreign_net'] / 100_000
    inst_df['invest_net_bil']  = inst_df['invest_net']  / 100_000
    inst_df['dealer_net_bil']  = inst_df['dealer_net']  / 100_000
    print(f"  ✅ 三大法人：{len(inst_df)} 筆 ({inst_df.index[0].date()} ～ {inst_df.index[-1].date()})")
else:
    inst_df = pd.DataFrame()
    print("  ❌ 三大法人資料抓取失敗")

# ════════════════════════════════════════════════════
# 3. TAIFEX 外資期貨未平倉（逐月）
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【3】抓取 TAIFEX 外資期貨歷史資料")
print("=" * 50)

def fetch_taifex_futures_month(year, month):
    """抓取單月外資台指期貨淨多單"""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    start_str = f"{year}/{month:02d}/01"
    end_str   = f"{year}/{month:02d}/{last_day:02d}"

    url = 'https://www.taifex.com.tw/cht/3/futContractsDateDown'
    try:
        resp = requests.post(url, data={
            'queryStartDate': start_str,
            'queryEndDate':   end_str,
            'commodityId':    'TXF',
        }, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.taifex.com.tw/'
        }, timeout=20)

        lines = resp.text.strip().split('\n')
        rows = []
        current_date = None

        for line in lines:
            line = line.strip()
            if not line:
                continue
            cols = [c.strip().strip('"') for c in line.split(',')]

            # 嘗試解析日期（第一欄）
            if len(cols) > 0:
                date_str_raw = cols[0]
                try:
                    parts = date_str_raw.split('/')
                    if len(parts) == 3 and len(parts[0]) == 3:
                        # 民國年
                        d = datetime.date(int(parts[0])+1911, int(parts[1]), int(parts[2]))
                        current_date = d
                    elif len(parts) == 3:
                        d = datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
                        current_date = d
                except:
                    pass

            # 找外資行
            if current_date and ('外資' in line or 'Foreign' in line):
                nums = []
                for c in cols:
                    try: nums.append(int(c.replace(',','')))
                    except: pass
                if len(nums) >= 5:
                    net_long = nums[4]
                    rows.append({
                        'date': current_date,
                        'futures_foreign_net': net_long,
                    })

        return rows
    except Exception as e:
        return []

all_futures = []
y, m = start_year, start_month
done = 0
while (y, m) <= (now.year, now.month):
    rows = fetch_taifex_futures_month(y, m)
    all_futures.extend(rows)
    done += 1
    if done % 6 == 0 or done == total_months:
        print(f"  進度：{done}/{total_months} 個月，已取 {len(all_futures)} 筆")
    m += 1
    if m > 12:
        m = 1
        y += 1
    time.sleep(0.4)

if all_futures:
    fut_df = pd.DataFrame(all_futures)
    fut_df['date'] = pd.to_datetime(fut_df['date'])
    fut_df = fut_df.set_index('date').sort_index()
    # 去除重複（同日可能有多行）
    fut_df = fut_df[~fut_df.index.duplicated(keep='last')]
    print(f"  ✅ 外資期貨：{len(fut_df)} 筆 ({fut_df.index[0].date()} ～ {fut_df.index[-1].date()})")
else:
    fut_df = pd.DataFrame()
    print("  ❌ 外資期貨資料抓取失敗")

# ════════════════════════════════════════════════════
# 4. 合併所有資料
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【4】合併資料")
print("=" * 50)

# 以 Yahoo 資料為主軸（週一到週五）
master = yahoo_df.copy()

# 合併三大法人
if not inst_df.empty:
    for col in ['foreign_net_bil', 'invest_net_bil', 'dealer_net_bil']:
        master = master.join(inst_df[[col]], how='left')

# 合併外資期貨
if not fut_df.empty:
    master = master.join(fut_df[['futures_foreign_net']], how='left')

# ── 技術指標（次要因子）────────────────────────────
print("  計算技術指標...")
if 'TWII' in master.columns:
    p = master['TWII']
    master['MA5']   = p.rolling(5).mean()
    master['MA20']  = p.rolling(20).mean()
    master['MA60']  = p.rolling(60).mean()

    # RSI(14)
    delta = p.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, float('nan'))
    master['RSI14'] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = p.ewm(span=12, adjust=False).mean()
    ema26 = p.ewm(span=26, adjust=False).mean()
    master['MACD']        = ema12 - ema26
    master['MACD_signal'] = master['MACD'].ewm(span=9, adjust=False).mean()
    master['MACD_hist']   = master['MACD'] - master['MACD_signal']

# ── 目標變數：0050 未來 5 日報酬 ──────────────────
if 'ETF0050' in master.columns:
    master['target_5d_return'] = master['ETF0050'].pct_change(5).shift(-5)
    master['target_signal']    = (master['target_5d_return'] > 0).astype(int)
    print("  ✅ 目標變數（未來5日報酬）已建立")

# ── 填補缺值（前向填補，最多5天）──────────────────
master = master.ffill(limit=5)

# ── 僅保留有完整資料的列 ───────────────────────────
# 核心欄位至少需要有值
core_cols = [c for c in ['TWII', 'SOX', 'DXY', 'USDTWD'] if c in master.columns]
master = master.dropna(subset=core_cols)

print(f"\n  最終資料：{master.shape[0]} 列 × {master.shape[1]} 欄")
print(f"  日期範圍：{master.index[0].date()} ～ {master.index[-1].date()}")
print(f"\n  欄位清單：")
for i, col in enumerate(master.columns):
    null_pct = master[col].isna().mean() * 100
    print(f"    {col:25s} 缺值率：{null_pct:.1f}%")

# ── 儲存 ───────────────────────────────────────────
master.index.name = 'date'
master.to_csv('data/raw_data.csv')
print(f"\n✅ 已儲存至 data/raw_data.csv")
print("   下一步：執行 python build_model.py")
