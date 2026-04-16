"""
台股多因子模型 - 歷史資料收集（GitHub Actions 版）
每次執行抓取指定年份的資料，存入 data/history/YYYY.csv
由 GitHub Actions 分批執行
"""

import subprocess, sys, os, json, time, datetime, calendar, warnings
import argparse

def install(pkg):
    subprocess.check_call([sys.executable,'-m','pip','install',pkg,'-q'])

install('yfinance'); install('pandas'); install('requests')

import pandas as pd
import yfinance as yf
import requests
warnings.filterwarnings('ignore')

os.makedirs('data/history', exist_ok=True)

# ── 參數解析 ─────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True, help='要抓取的年份')
args = parser.parse_args()

YEAR  = args.year
START = f'{YEAR}-01-01'
END   = f'{YEAR}-12-31'
print(f"\n{'='*50}")
print(f"抓取年份：{YEAR}")
print(f"{'='*50}\n")

HEADERS_TWSE   = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}
HEADERS_TAIFEX = {'User-Agent':'Mozilla/5.0','Referer':'https://www.taifex.com.tw/'}

def roc_to_date(s):
    try:
        p = str(s).strip().split('/')
        if len(p) == 3:
            y = int(p[0])
            if y < 1000: y += 1911
            return datetime.date(y, int(p[1]), int(p[2]))
    except: pass
    return None

def pn(s):
    try: return int(str(s).replace(',','').replace(' ',''))
    except: return 0

months = [(YEAR, m) for m in range(1, 13)]
frames = {}

# ════════════════════════════════════════════════════
# 1. Yahoo Finance（只抓這一年）
# ════════════════════════════════════════════════════
print("【1】Yahoo Finance")
YAHOO = {
    '^SOX':'SOX', '^TWII':'TWII', 'DX-Y.NYB':'DXY',
    'USDTWD=X':'USDTWD', '^TNX':'US10Y', '^VIX':'VIX',
    '^IXIC':'NASDAQ', '0050.TW':'ETF0050',
}
yahoo_frames = {}
for sym, name in YAHOO.items():
    try:
        df = yf.download(sym, start=START, end=END, progress=False, auto_adjust=True)
        if df.empty: continue
        s = df['Close'].squeeze()
        s.name = name
        yahoo_frames[name] = s
        print(f"  ✅ {name}: {len(s)} 筆")
    except Exception as e:
        print(f"  ❌ {name}: {e}")

if yahoo_frames:
    ydf = pd.DataFrame(yahoo_frames)
    ydf.index = pd.to_datetime(ydf.index)
    frames['yahoo'] = ydf

# ════════════════════════════════════════════════════
# 2. TWSE 三大法人
# ════════════════════════════════════════════════════
print("\n【2】TWSE 三大法人")
inst_rows = []
for y, m in months:
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={y}{m:02d}01&selectType=ALL"
    try:
        data = requests.get(url, timeout=25, headers=HEADERS_TWSE).json()
        if data.get('stat') == 'OK' and data.get('data'):
            for row in data['data']:
                d = roc_to_date(row[0])
                if not d: continue
                inst_rows.append({
                    'date': d,
                    'foreign_net': pn(row[4]),
                    'invest_net':  pn(row[7]),
                    'dealer_net':  pn(row[10]),
                })
    except Exception as e:
        print(f"  [警告] {y}/{m:02d}: {e}")
    time.sleep(0.5)

if inst_rows:
    idf = pd.DataFrame(inst_rows)
    idf['date'] = pd.to_datetime(idf['date'])
    idf = idf.set_index('date').sort_index()
    for c in ['foreign_net','invest_net','dealer_net']:
        idf[c+'_bil'] = idf[c] / 100_000
    idf = idf.drop(columns=['foreign_net','invest_net','dealer_net'])
    frames['inst'] = idf
    print(f"  ✅ {len(idf)} 筆")
else:
    print("  ❌ 失敗")

# ════════════════════════════════════════════════════
# 3. TWSE 融資餘額
# ════════════════════════════════════════════════════
print("\n【3】TWSE 融資餘額")
margin_rows = []
for y, m in months:
    url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json&date={y}{m:02d}01&selectType=CS"
    try:
        data = requests.get(url, timeout=25, headers=HEADERS_TWSE).json()
        tables = data.get('tables', [])
        if tables:
            for row in tables[0].get('data', []):
                d = roc_to_date(row[0]) if len(row) > 0 else None
                if not d: continue
                margin_rows.append({
                    'date':           d,
                    'margin_balance': pn(row[1]) if len(row) > 1 else 0,
                    'short_balance':  pn(row[4]) if len(row) > 4 else 0,
                })
    except Exception as e:
        print(f"  [警告] {y}/{m:02d}: {e}")
    time.sleep(0.5)

if margin_rows:
    mdf = pd.DataFrame(margin_rows)
    mdf['date'] = pd.to_datetime(mdf['date'])
    mdf = mdf.set_index('date').sort_index()
    mdf = mdf[~mdf.index.duplicated(keep='last')]
    frames['margin'] = mdf
    print(f"  ✅ {len(mdf)} 筆")
else:
    print("  ❌ 失敗")

# ════════════════════════════════════════════════════
# 4. TAIFEX 期貨（TXF / MTX / MXF）
# ════════════════════════════════════════════════════
def fetch_taifex_futures_month(y, m, commodity):
    last = calendar.monthrange(y, m)[1]
    try:
        resp = requests.post(
            'https://www.taifex.com.tw/cht/3/futContractsDateDown',
            data={'queryStartDate':f"{y}/{m:02d}/01",
                  'queryEndDate':  f"{y}/{m:02d}/{last:02d}",
                  'commodityId':   commodity},
            headers=HEADERS_TAIFEX, timeout=25)
        rows = []; cur_date = None
        for line in resp.text.strip().split('\n'):
            cols = [c.strip().strip('"') for c in line.split(',')]
            d = roc_to_date(cols[0]) if cols else None
            if d: cur_date = d
            if cur_date and ('外資' in line or 'Foreign' in line):
                nums = []
                for c in cols:
                    try: nums.append(int(c.replace(',','')))
                    except: pass
                if len(nums) >= 5:
                    rows.append({'date': cur_date, f'{commodity}_net': nums[4]})
        return rows
    except: return []

for commodity in ['TXF', 'MTX', 'MXF']:
    if commodity == 'MXF' and YEAR < 2017:
        continue
    print(f"\n【期貨】{commodity}")
    rows = []
    for y, m in months:
        rows.extend(fetch_taifex_futures_month(y, m, commodity))
        time.sleep(0.4)
    if rows:
        fdf = pd.DataFrame(rows)
        fdf['date'] = pd.to_datetime(fdf['date'])
        fdf = fdf.set_index('date').sort_index()
        fdf = fdf[~fdf.index.duplicated(keep='last')]
        frames[commodity] = fdf
        print(f"  ✅ {len(fdf)} 筆")
    else:
        print(f"  ❌ 失敗")

# ════════════════════════════════════════════════════
# 5. TAIFEX 前五大/十大交易人留倉
# ════════════════════════════════════════════════════
print("\n【5】TAIFEX 前五大/十大交易人留倉")
lt_rows = []
for y, m in months:
    last = calendar.monthrange(y, m)[1]
    try:
        resp = requests.post(
            'https://www.taifex.com.tw/cht/3/largeTraderFutDown',
            data={'queryStartDate':f"{y}/{m:02d}/01",
                  'queryEndDate':  f"{y}/{m:02d}/{last:02d}",
                  'commodityId':   'TX'},
            headers=HEADERS_TAIFEX, timeout=25)
        cur_date = None
        for line in resp.text.strip().split('\n'):
            cols = [c.strip().strip('"') for c in line.split(',')]
            d = roc_to_date(cols[0]) if cols else None
            if d: cur_date = d
            if not cur_date: continue
            nums = []
            for c in cols:
                try: nums.append(int(c.replace(',','')))
                except: pass
            if len(nums) >= 6:
                lt_rows.append({
                    'date':     cur_date,
                    'top5_net': nums[0] - nums[1],
                    'top10_net':nums[4] - nums[5] if len(nums) > 5 else 0,
                })
    except Exception as e:
        print(f"  [警告] {y}/{m:02d}: {e}")
    time.sleep(0.4)

if lt_rows:
    ltdf = pd.DataFrame(lt_rows)
    ltdf['date'] = pd.to_datetime(ltdf['date'])
    ltdf = ltdf.set_index('date').sort_index()
    ltdf = ltdf[~ltdf.index.duplicated(keep='last')]
    frames['largetrader'] = ltdf
    print(f"  ✅ {len(ltdf)} 筆")
else:
    print("  ❌ 失敗")

# ════════════════════════════════════════════════════
# 6. TAIFEX 外資選擇權
# ════════════════════════════════════════════════════
if YEAR >= 2007:
    print("\n【6】TAIFEX 外資選擇權")
    opt_rows = []
    for y, m in months:
        last = calendar.monthrange(y, m)[1]
        try:
            resp = requests.post(
                'https://www.taifex.com.tw/cht/3/callsAndPutsDateDown',
                data={'queryStartDate':f"{y}/{m:02d}/01",
                      'queryEndDate':  f"{y}/{m:02d}/{last:02d}",
                      'commodityId':   'TXO'},
                headers=HEADERS_TAIFEX, timeout=25)
            cur_date = None
            for line in resp.text.strip().split('\n'):
                cols = [c.strip().strip('"') for c in line.split(',')]
                d = roc_to_date(cols[0]) if cols else None
                if d: cur_date = d
                if not cur_date: continue
                if '外資' in line or 'Foreign' in line:
                    nums = []
                    for c in cols:
                        try: nums.append(int(c.replace(',','')))
                        except: pass
                    if len(nums) >= 4:
                        opt_rows.append({
                            'date':            cur_date,
                            'opt_call_net':    nums[0] - nums[1],
                            'opt_put_net':     nums[2] - nums[3],
                            'opt_net_total':   (nums[0]-nums[1]) - (nums[2]-nums[3]),
                        })
        except Exception as e:
            print(f"  [警告] {y}/{m:02d}: {e}")
        time.sleep(0.4)

    if opt_rows:
        odf = pd.DataFrame(opt_rows)
        odf['date'] = pd.to_datetime(odf['date'])
        odf = odf.set_index('date').sort_index()
        odf = odf[~odf.index.duplicated(keep='last')]
        frames['option'] = odf
        print(f"  ✅ {len(odf)} 筆")
    else:
        print("  ❌ 失敗")

# ════════════════════════════════════════════════════
# 7. 合併本年資料並儲存
# ════════════════════════════════════════════════════
print(f"\n【7】合併 {YEAR} 年資料")

if 'yahoo' not in frames:
    print("❌ Yahoo 資料失敗，無法繼續")
    sys.exit(1)

master = frames['yahoo'].copy()
for key in ['inst','margin','TXF','MTX','MXF','largetrader','option']:
    if key in frames and not frames[key].empty:
        master = master.join(frames[key], how='left')

master.index.name = 'date'
out_path = f'data/history/{YEAR}.csv'
master.to_csv(out_path)
print(f"✅ 已儲存：{out_path}（{len(master)} 列 × {master.shape[1]} 欄）")

# 更新進度記錄
progress_path = 'data/history/progress.json'
try:
    with open(progress_path) as f:
        progress = json.load(f)
except:
    progress = {}
progress[str(YEAR)] = {
    'rows': len(master),
    'cols': master.shape[1],
    'done': True,
    'updated': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
}
with open(progress_path, 'w') as f:
    json.dump(progress, f, indent=2)
print(f"✅ 進度記錄更新：{progress_path}")
