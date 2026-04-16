"""
台股多因子模型 - 歷史資料收集 v4
修復：
- TWSE 三大法人：改抓大盤合計行
- TWSE 融資餘額：重新解析統計表格式
- TAIFEX：只有 2023/05 之後用 CSV 端點，之前僅保留前五大/十大
"""

import subprocess, sys, os, json, time, datetime, calendar, warnings, re
import argparse

def install(pkg):
    subprocess.check_call([sys.executable,'-m','pip','install',pkg,'-q'])
install('yfinance'); install('pandas'); install('requests')

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
# 2. TWSE 三大法人（2012/05 之後）
#    修復：逐日查詢取得合計，不再用月查詢
# ════════════════════════════════════════════════════
print("\n【2】TWSE 三大法人")
if YEAR < 2012:
    print(f"  ⚠️ {YEAR} 年早於 2012/05，略過")
else:
    inst_rows = []
    for y, m in months:
        if y == 2012 and m < 5:
            continue
        # 用月份查詢，取得該月所有交易日的合計
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={y}{m:02d}01&selectType=ALL"
        try:
            resp = requests.get(url, timeout=25, headers=H_TWSE)
            data = resp.json()
            if data.get('stat') != 'OK' or not data.get('data'):
                continue

            rows = data['data']
            # 資料是個股列表，需要找出每個交易日的合計
            # TWSE T86 月查詢：最後一行通常是當月最後一天合計
            # 改用另一種方式：直接查 T86 逐日資料
            # 實際上月查詢返回的是該月「所有個股」的買賣超
            # 我們需要用不同的邏輯：把所有個股的外資買賣超加總

            # 建立日期->合計的字典
            date_totals = {}
            for row in rows:
                # 日期欄位不在 T86 月查詢裡，改用日查詢
                pass

            # 改用逐日查詢
            last_day = calendar.monthrange(y, m)[1]
            for day in range(1, last_day + 1):
                date_obj = datetime.date(y, m, day)
                if date_obj.weekday() >= 5:  # 跳過週末
                    continue
                date_str = date_obj.strftime('%Y%m%d')
                day_url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date_str}&selectType=ALLBUT0999"
                try:
                    day_resp = requests.get(day_url, timeout=15, headers=H_TWSE)
                    day_data = day_resp.json()
                    if day_data.get('stat') != 'OK' or not day_data.get('data'):
                        continue
                    # 找合計行（通常是最後一行，或包含'合計'字樣）
                    day_rows = day_data['data']
                    total_row = None
                    for r in reversed(day_rows):
                        if '合計' in str(r[0]) or str(r[0]).strip() == '':
                            total_row = r
                            break
                    if not total_row:
                        # 若無合計行，加總所有個股
                        foreign_total = sum(pn(r[4]) for r in day_rows if len(r) > 4)
                        invest_total  = sum(pn(r[7]) for r in day_rows if len(r) > 7)
                        dealer_total  = sum(pn(r[8]) for r in day_rows if len(r) > 8)
                    else:
                        foreign_total = pn(total_row[4]) if len(total_row) > 4 else 0
                        invest_total  = pn(total_row[7]) if len(total_row) > 7 else 0
                        dealer_total  = pn(total_row[8]) if len(total_row) > 8 else 0

                    inst_rows.append({
                        'date':        date_obj,
                        'foreign_net': foreign_total,
                        'invest_net':  invest_total,
                        'dealer_net':  dealer_total,
                    })
                except:
                    pass
                time.sleep(0.15)

        except Exception as e:
            print(f"  [警告] {y}/{m:02d}: {e}")
        print(f"  {y}/{m:02d} 完成，累計 {len(inst_rows)} 筆")
        time.sleep(0.3)

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
# 3. TWSE 融資餘額（修復：正確解析統計表格式）
#    格式：tables[0].data = [融資行, 融券行, 融資金額行]
#    逐日查詢取得每天的餘額
# ════════════════════════════════════════════════════
print("\n【3】TWSE 融資餘額")
margin_rows = []
for y, m in months:
    last_day = calendar.monthrange(y, m)[1]
    for day in range(1, last_day + 1):
        date_obj = datetime.date(y, m, day)
        if date_obj.weekday() >= 5:
            continue
        date_str = date_obj.strftime('%Y%m%d')
        url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json&date={date_str}&selectType=ALL"
        try:
            resp = requests.get(url, timeout=15, headers=H_TWSE)
            data = resp.json()
            if data.get('stat') != 'OK':
                continue
            tables = data.get('tables', [])
            if not tables:
                continue
            # 第一個 table 是信用交易統計
            # data[0] = 融資(交易單位) 行
            # data[1] = 融券(交易單位) 行
            table_data = tables[0].get('data', [])
            margin_balance = 0
            short_balance  = 0
            for row in table_data:
                if len(row) < 5: continue
                if '融資' in str(row[0]) and '金額' not in str(row[0]):
                    margin_balance = pn(row[4])  # 今日餘額
                elif '融券' in str(row[0]):
                    short_balance = pn(row[4])
            if margin_balance > 0:
                margin_rows.append({
                    'date':           date_obj,
                    'margin_balance': margin_balance,
                    'short_balance':  short_balance,
                })
        except:
            pass
        time.sleep(0.15)
    print(f"  {y}/{m:02d} 完成，累計 {len(margin_rows)} 筆")

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
#    只有 2023/05 之後的 CSV 端點可用
# ════════════════════════════════════════════════════
def fetch_taifex_csv(y, m, commodity):
    """CSV 端點，只有近期資料可用"""
    last = calendar.monthrange(y, m)[1]
    try:
        resp = requests.post(
            'https://www.taifex.com.tw/cht/3/futContractsDateDown',
            data={'queryStartDate': f"{y}/{m:02d}/01",
                  'queryEndDate':   f"{y}/{m:02d}/{last:02d}",
                  'commodityId':    commodity},
            headers=H_TAIFEX, timeout=25)
        # 檢查是否為有效 CSV（不是 HTML）
        if '<html' in resp.text[:100].lower():
            return []
        rows = []
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
        return rows
    except:
        return []

for commodity in ['TXF', 'MTX']:
    print(f"\n【期貨】{commodity}")
    fut_rows = []
    for y, m in months:
        fut_rows.extend(fetch_taifex_csv(y, m, commodity))
        time.sleep(0.3)
    if fut_rows:
        fdf = pd.DataFrame(fut_rows)
        fdf['date'] = pd.to_datetime(fdf['date'])
        fdf = fdf.set_index('date').sort_index()
        fdf = fdf[~fdf.index.duplicated(keep='last')]
        frames[commodity] = fdf
        print(f"  ✅ {len(fdf)} 筆")
    else:
        print(f"  ⚠️ {YEAR} 年無 CSV 資料（正常，2023/05 前不支援）")

if YEAR >= 2017:
    commodity = 'MXF'
    print(f"\n【期貨】{commodity}")
    mxf_rows = []
    for y, m in months:
        if y == 2017 and m < 11: continue
        mxf_rows.extend(fetch_taifex_csv(y, m, commodity))
        time.sleep(0.3)
    if mxf_rows:
        mdf2 = pd.DataFrame(mxf_rows)
        mdf2['date'] = pd.to_datetime(mdf2['date'])
        mdf2 = mdf2.set_index('date').sort_index()
        mdf2 = mdf2[~mdf2.index.duplicated(keep='last')]
        frames['MXF'] = mdf2
        print(f"  ✅ {len(mdf2)} 筆")
    else:
        print(f"  ⚠️ {YEAR} 年無 MXF CSV 資料")

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
            data={'queryStartDate': f"{y}/{m:02d}/01",
                  'queryEndDate':   f"{y}/{m:02d}/{last:02d}",
                  'commodityId':    'TX'},
            headers=H_TAIFEX, timeout=25)
        if '<html' in resp.text[:100].lower():
            continue
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
# 6. TAIFEX 外資選擇權（只有近期 CSV 可用）
# ════════════════════════════════════════════════════
if YEAR >= 2007:
    print("\n【6】TAIFEX 外資選擇權")
    opt_rows = []
    for y, m in months:
        last = calendar.monthrange(y, m)[1]
        try:
            resp = requests.post(
                'https://www.taifex.com.tw/cht/3/callsAndPutsDateDown',
                data={'queryStartDate': f"{y}/{m:02d}/01",
                      'queryEndDate':   f"{y}/{m:02d}/{last:02d}",
                      'commodityId':    'TXO'},
                headers=H_TAIFEX, timeout=25)
            if '<html' in resp.text[:100].lower():
                continue
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
        print("  ⚠️ 此年份無選擇權 CSV 資料（正常）")

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
cols_got = [k for k in ['inst','margin','TXF','MTX','MXF','largetrader','option'] if k in frames]
print(f"✅ 已儲存：{out_path}（{len(master)} 列 × {master.shape[1]} 欄）")
print(f"   成功來源：{cols_got}")

progress_path = 'data/history/progress.json'
try:
    with open(progress_path) as f: progress = json.load(f)
except: progress = {}
progress[str(YEAR)] = {
    'rows': len(master), 'cols': master.shape[1],
    'done': True,
    'updated': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
    'sources': cols_got,
}
with open(progress_path, 'w') as f:
    json.dump(progress, f, indent=2)
