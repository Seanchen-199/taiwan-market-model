"""
台股多因子模型 - 歷史資料收集 v3
修復：TWSE 三大法人（2012/05起）、融資餘額格式、TAIFEX 舊端點解析
執行：python collect_history_actions.py --year 2015
"""

import subprocess, sys, os, json, time, datetime, calendar, warnings, re
import argparse

def install(pkg):
    subprocess.check_call([sys.executable,'-m','pip','install',pkg,'-q'])
install('yfinance'); install('pandas'); install('requests'); install('lxml')

import pandas as pd
import yfinance as yf
import requests
warnings.filterwarnings('ignore')

os.makedirs('data/history', exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True)
args   = parser.parse_args()

YEAR  = args.year
START = f'{YEAR}-01-01'
END   = f'{YEAR}-12-31'
print(f"\n{'='*50}\n抓取年份：{YEAR}\n{'='*50}\n")

H_TWSE   = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}
H_TAIFEX = {'User-Agent':'Mozilla/5.0','Referer':'https://www.taifex.com.tw/'}
months   = [(YEAR, m) for m in range(1, 13)]
frames   = {}

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

# ════════════════════════════════════════════════════
# 1. Yahoo Finance
# ════════════════════════════════════════════════════
print("【1】Yahoo Finance")
YAHOO = {
    '^SOX':'SOX', '^TWII':'TWII', 'DX-Y.NYB':'DXY',
    'USDTWD=X':'USDTWD', '^TNX':'US10Y', '^VIX':'VIX',
    '^IXIC':'NASDAQ', '0050.TW':'ETF0050',
}
yf_frames = {}
for sym, name in YAHOO.items():
    try:
        df = yf.download(sym, start=START, end=END, progress=False, auto_adjust=True)
        if df.empty: continue
        s = df['Close'].squeeze(); s.name = name
        yf_frames[name] = s
        print(f"  ✅ {name}: {len(s)} 筆")
    except Exception as e:
        print(f"  ❌ {name}: {e}")

if yf_frames:
    ydf = pd.DataFrame(yf_frames)
    ydf.index = pd.to_datetime(ydf.index)
    frames['yahoo'] = ydf

# ════════════════════════════════════════════════════
# 2. TWSE 三大法人（2012/05 之後才有資料）
# ════════════════════════════════════════════════════
print("\n【2】TWSE 三大法人")
if YEAR < 2012 or (YEAR == 2012 and False):
    print(f"  ⚠️ {YEAR} 年早於 2012/05，TWSE 不提供此期間資料，略過")
else:
    inst_rows = []
    for y, m in months:
        if y == 2012 and m < 5:
            continue
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={y}{m:02d}01&selectType=ALL"
        try:
            resp = requests.get(url, timeout=25, headers=H_TWSE)
            data = resp.json()
            if data.get('stat') == 'OK' and data.get('data'):
                for row in data['data']:
                    d = roc_to_date(row[0])
                    if not d: continue
                    inst_rows.append({
                        'date':        d,
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
# 3. TWSE 融資餘額（修復：改用正確查詢格式）
# ════════════════════════════════════════════════════
print("\n【3】TWSE 融資餘額")
margin_rows = []
for y, m in months:
    # 嘗試多種日期格式
    for day in ['01', '15']:
        date_str = f"{y}{m:02d}{day}"
        url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json&date={date_str}&selectType=ALL"
        try:
            resp = requests.get(url, timeout=25, headers=H_TWSE)
            data = resp.json()
            if data.get('stat') == 'OK':
                tables = data.get('tables', [])
                for table in tables:
                    for row in table.get('data', []):
                        d = roc_to_date(row[0]) if len(row) > 0 else None
                        if not d: continue
                        margin_rows.append({
                            'date':           d,
                            'margin_balance': pn(row[1]) if len(row) > 1 else 0,
                            'short_balance':  pn(row[4]) if len(row) > 4 else 0,
                        })
                if tables:
                    break
        except Exception as e:
            pass
        time.sleep(0.3)
    time.sleep(0.3)

if margin_rows:
    mdf = pd.DataFrame(margin_rows)
    mdf['date'] = pd.to_datetime(mdf['date'])
    mdf = mdf.set_index('date').sort_index()
    mdf = mdf[~mdf.index.duplicated(keep='last')]
    frames['margin'] = mdf
    print(f"  ✅ {len(mdf)} 筆")
else:
    print("  ❌ 失敗（TWSE 融資餘額此期間可能不提供）")

# ════════════════════════════════════════════════════
# 4. TAIFEX 期貨（TXF / MTX / MXF）
#    修復：2024 之前用舊端點 futContractsDate（HTML），
#          2024 之後用新端點 futContractsDateDown（CSV）
# ════════════════════════════════════════════════════
def fetch_taifex_futures_v2(y, m, commodity):
    """
    自動選擇正確端點：
    - 2024 之後：futContractsDateDown（CSV格式）
    - 2024 之前：futContractsDateDown 先試，失敗再用舊端點解析
    """
    last = calendar.monthrange(y, m)[1]
    start_str = f"{y}/{m:02d}/01"
    end_str   = f"{y}/{m:02d}/{last:02d}"
    rows = []

    # 先試新端點（CSV）
    try:
        resp = requests.post(
            'https://www.taifex.com.tw/cht/3/futContractsDateDown',
            data={'queryStartDate': start_str, 'queryEndDate': end_str, 'commodityId': commodity},
            headers=H_TAIFEX, timeout=25)
        cur_date = None
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
        if rows:
            return rows
    except: pass

    # 舊端點（HTML 表格格式）
    try:
        resp = requests.post(
            'https://www.taifex.com.tw/cht/3/futContractsDate',
            data={'queryStartDate': start_str, 'queryEndDate': end_str, 'commodityId': commodity},
            headers=H_TAIFEX, timeout=25)

        # 用正則解析 HTML 表格中的數字
        html = resp.text
        # 找日期和對應的外資淨多單
        # 典型格式：日期在 td 裡，外資行包含特定欄位
        date_pattern = re.compile(r'(\d{3}/\d{2}/\d{2})')
        # 找所有 tr 行
        tr_blocks = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)

        cur_date = None
        for tr in tr_blocks:
            # 抓日期
            date_match = date_pattern.search(tr)
            if date_match:
                d = roc_to_date(date_match.group(1))
                if d: cur_date = d

            # 抓外資行的數字
            if cur_date and ('外資' in tr or 'Foreign' in tr):
                # 移除 HTML tags
                clean = re.sub(r'<[^>]+>', ' ', tr)
                nums = []
                for n in re.findall(r'-?\d{1,3}(?:,\d{3})*', clean):
                    try: nums.append(int(n.replace(',','')))
                    except: pass
                if len(nums) >= 5:
                    rows.append({'date': cur_date, f'{commodity}_net': nums[4]})

        if rows:
            return rows
    except Exception as e:
        print(f"  [警告] 舊端點解析失敗 {y}/{m:02d}: {e}")

    return []

for commodity in ['TXF', 'MTX']:
    print(f"\n【期貨】{commodity}")
    fut_rows = []
    for y, m in months:
        fut_rows.extend(fetch_taifex_futures_v2(y, m, commodity))
        time.sleep(0.4)
    if fut_rows:
        fdf = pd.DataFrame(fut_rows)
        fdf['date'] = pd.to_datetime(fdf['date'])
        fdf = fdf.set_index('date').sort_index()
        fdf = fdf[~fdf.index.duplicated(keep='last')]
        frames[commodity] = fdf
        print(f"  ✅ {len(fdf)} 筆")
    else:
        print(f"  ❌ 失敗")

# 微台指（2017/11 後）
if YEAR >= 2017:
    commodity = 'MXF'
    print(f"\n【期貨】{commodity}（2017/11起）")
    mxf_rows = []
    for y, m in months:
        if y == 2017 and m < 11: continue
        mxf_rows.extend(fetch_taifex_futures_v2(y, m, commodity))
        time.sleep(0.4)
    if mxf_rows:
        mdf2 = pd.DataFrame(mxf_rows)
        mdf2['date'] = pd.to_datetime(mdf2['date'])
        mdf2 = mdf2.set_index('date').sort_index()
        mdf2 = mdf2[~mdf2.index.duplicated(keep='last')]
        frames['MXF'] = mdf2
        print(f"  ✅ {len(mdf2)} 筆")
    else:
        print(f"  ❌ 失敗")

# ════════════════════════════════════════════════════
# 5. TAIFEX 前五大/十大交易人留倉
# ════════════════════════════════════════════════════
print("\n【5】TAIFEX 前五大/十大留倉")
lt_rows = []
for y, m in months:
    last = calendar.monthrange(y, m)[1]
    try:
        resp = requests.post(
            'https://www.taifex.com.tw/cht/3/largeTraderFutDown',
            data={'queryStartDate':f"{y}/{m:02d}/01",
                  'queryEndDate':  f"{y}/{m:02d}/{last:02d}",
                  'commodityId':   'TX'},
            headers=H_TAIFEX, timeout=25)
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
                    'date':      cur_date,
                    'top5_net':  nums[0] - nums[1],
                    'top10_net': nums[4] - nums[5] if len(nums) > 5 else 0,
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
# 6. TAIFEX 外資選擇權（2007 之後）
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
                headers=H_TAIFEX, timeout=25)
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
                            'date':         cur_date,
                            'opt_call_net': nums[0] - nums[1],
                            'opt_put_net':  nums[2] - nums[3],
                            'opt_net':      (nums[0]-nums[1]) - (nums[2]-nums[3]),
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
# 7. 合併本年資料
# ════════════════════════════════════════════════════
print(f"\n【7】合併 {YEAR} 年資料")
if 'yahoo' not in frames:
    print("❌ Yahoo 資料失敗"); sys.exit(1)

master = frames['yahoo'].copy()
for key in ['inst','margin','TXF','MTX','MXF','largetrader','option']:
    if key in frames and not frames[key].empty:
        master = master.join(frames[key], how='left')

master.index.name = 'date'
out_path = f'data/history/{YEAR}.csv'
master.to_csv(out_path)
print(f"✅ 已儲存：{out_path}（{len(master)} 列 × {master.shape[1]} 欄）")

# 更新進度
progress_path = 'data/history/progress.json'
try:
    with open(progress_path) as f: progress = json.load(f)
except: progress = {}
progress[str(YEAR)] = {
    'rows': len(master), 'cols': master.shape[1],
    'done': True,
    'updated': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
    'sources': list(frames.keys()),
}
with open(progress_path, 'w') as f:
    json.dump(progress, f, indent=2)
print(f"✅ 進度記錄更新")
