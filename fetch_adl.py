"""
藤落線（ADL）月統計版
來源：TWSE 漲跌證券數合計（月統計）
執行：python fetch_adl.py
輸出：data/adl_data.csv
"""

import subprocess, sys
subprocess.check_call([sys.executable,'-m','pip','install','requests','pandas','-q'])

import os, re, time, datetime
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

def fetch_adl_month(year, month):
    """
    抓取單月漲跌家數
    回傳 dict 或 None
    """
    date_str = f"{year}{month:02d}01"
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date={date_str}&type=MS"
    try:
        resp = requests.get(url, timeout=20, headers=H)
        data = resp.json()
        if data.get('stat') != 'OK':
            return None
        tables = data.get('tables', [])
        for t in tables:
            title = t.get('title','')
            rows  = t.get('data', [])
            if '漲跌' in title and rows:
                up = down = flat = unchanged = 0
                for row in rows:
                    label = str(row[0])
                    val   = parse_num(row[1]) if len(row) > 1 else 0
                    if '上漲' in label:
                        up = val
                    elif '下跌' in label:
                        down = val
                    elif '平盤' in label:
                        flat = val
                    elif '未比價' in label or '無比價' in label:
                        unchanged = val
                if up > 0 or down > 0:
                    return {
                        'adl_up':        up,
                        'adl_down':      down,
                        'adl_flat':      flat,
                        'adl_diff':      up - down,
                        'adl_breadth':   round(up / max(up + down, 1) * 100, 1),  # 上漲比例%
                    }
    except Exception as e:
        pass
    return None

def iter_months(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1

now = datetime.date.today()
print("開始收集藤落線月統計資料...")
print("資料來源：TWSE 漲跌證券數合計（月統計）")
print(f"資料區間：2004/01 ～ {now.year}/{now.month:02d}\n")

months = list(iter_months(2004, 1, now.year, now.month))
rows   = []

for i, (y, m) in enumerate(months):
    result = fetch_adl_month(y, m)
    if result:
        # 日期設為當月1日
        result['date'] = datetime.date(y, m, 1)
        rows.append(result)
        print(f"  ✅ {y}/{m:02d}：上漲 {result['adl_up']:,}，下跌 {result['adl_down']:,}，淨值 {result['adl_diff']:+,}，廣度 {result['adl_breadth']}%")
    else:
        print(f"  ⚠️ {y}/{m:02d}：無資料")
    time.sleep(0.3)

if not rows:
    print("❌ 無法取得資料")
    sys.exit(1)

df = pd.DataFrame(rows)
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date').sort_index()

# 計算 ADL 累計值（月度）
df['adl'] = df['adl_diff'].cumsum()

# ADL 3個月和12個月移動平均
df['adl_ma3']  = df['adl'].rolling(3).mean()
df['adl_ma12'] = df['adl'].rolling(12).mean()

# ADL 趨勢：ma3 > ma12 為多方廣度擴張
df['adl_trend'] = (df['adl_ma3'] > df['adl_ma12']).astype(int)

print(f"\n✅ 完成！共 {len(df)} 個月")
print(f"   日期範圍：{df.index[0].date()} ～ {df.index[-1].date()}")
print(f"\n   最近6筆：")
print(df[['adl_up','adl_down','adl_diff','adl_breadth','adl_trend']].tail(6).to_string())

df.index.name = 'date'
df.to_csv('data/adl_data.csv')
print(f"\n✅ 已儲存至 data/adl_data.csv")
