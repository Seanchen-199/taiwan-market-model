"""
藤落線（ADL）歷史資料收集腳本
來源：TWSE table[7] 漲跌證券數合計
執行：python fetch_adl.py
輸出：data/adl_data.csv
"""

import subprocess, sys
subprocess.check_call([sys.executable,'-m','pip','install','requests','pandas','-q'])

import os, re, time, datetime, calendar
import pandas as pd
import requests

os.makedirs('data', exist_ok=True)

H = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}

def parse_num(s):
    """從 '8,459(155)' 取出 8459"""
    try:
        return int(str(s).split('(')[0].replace(',','').strip())
    except:
        return 0

def fetch_adl_day(date_obj):
    """抓取單日漲跌家數，回傳 (up, down, unchanged)"""
    date_str = date_obj.strftime('%Y%m%d')
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date={date_str}"
    try:
        resp = requests.get(url, timeout=15, headers=H)
        data = resp.json()
        if data.get('stat') != 'OK':
            return None
        tables = data.get('tables', [])
        for t in tables:
            if t.get('title','') == '漲跌證券數合計' or '漲跌' in t.get('title',''):
                rows = t.get('data', [])
                up = down = unchanged = 0
                for row in rows:
                    label = str(row[0])
                    # 整體市場欄位（欄位index=1）
                    if '上漲' in label:
                        up = parse_num(row[1])
                    elif '下跌' in label:
                        down = parse_num(row[1])
                    elif '未比價' in label or '平盤' in label or '無比價' in label:
                        unchanged += parse_num(row[1])
                if up > 0 or down > 0:
                    return up, down, unchanged
    except Exception as e:
        pass
    return None

def iter_months(start_year, start_month, end_year, end_month):
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1

# 抓取歷史資料
print("開始收集藤落線歷史資料...")
print("資料來源：TWSE 漲跌證券數合計")

now = datetime.date.today()
START_YEAR, START_MONTH = 2004, 1

rows = []
months = list(iter_months(START_YEAR, START_MONTH, now.year, now.month))
total  = len(months)

for i, (y, m) in enumerate(months):
    last_day = calendar.monthrange(y, m)[1]
    month_rows = []

    for day in range(1, last_day + 1):
        d = datetime.date(y, m, day)
        if d > now: break
        if d.weekday() >= 5: continue  # 跳過週末

        result = fetch_adl_day(d)
        if result:
            up, down, unchanged = result
            month_rows.append({
                'date':      d,
                'adl_up':    up,
                'adl_down':  down,
                'adl_diff':  up - down,  # 當日淨值
            })
        time.sleep(0.12)

    rows.extend(month_rows)
    progress = f"{i+1}/{total}"
    print(f"  {y}/{m:02d} 完成，取得 {len(month_rows)} 筆，累計 {len(rows)} 筆 ({progress})")
    time.sleep(0.3)

if not rows:
    print("❌ 無法取得資料")
    sys.exit(1)

# 建立 DataFrame 並計算 ADL 累計值
df = pd.DataFrame(rows)
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date').sort_index()
df = df[~df.index.duplicated(keep='last')]

# ADL = 每日漲跌家數差的累計
df['adl'] = df['adl_diff'].cumsum()

# 5日和20日 ADL 移動平均（用於判斷趨勢）
df['adl_ma5']  = df['adl'].rolling(5).mean()
df['adl_ma20'] = df['adl'].rolling(20).mean()

print(f"\n✅ 完成！共 {len(df)} 筆")
print(f"   日期範圍：{df.index[0].date()} ～ {df.index[-1].date()}")
print(f"\n   最近5筆：")
print(df[['adl_up','adl_down','adl_diff','adl']].tail())

df.index.name = 'date'
df.to_csv('data/adl_data.csv')
print(f"\n✅ 已儲存至 data/adl_data.csv")
