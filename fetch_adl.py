"""
藤落線（ADL）月統計版 v2
不用 type=MS，改用月份第一個交易日直接查
"""

import subprocess, sys
subprocess.check_call([sys.executable,'-m','pip','install','requests','pandas','-q'])

import os, time, datetime, calendar
import pandas as pd
import requests

os.makedirs('data', exist_ok=True)
H = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}

def parse_num(s):
    try:
        return int(str(s).split('(')[0].replace(',','').strip())
    except:
        return 0

def fetch_adl_for_date(date_str):
    """查詢指定日期的漲跌家數，回傳 dict 或 None"""
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date={date_str}"
    try:
        resp = requests.get(url, timeout=20, headers=H)
        data = resp.json()
        if data.get('stat') != 'OK':
            return None
        for t in data.get('tables', []):
            title = t.get('title','')
            rows  = t.get('data', [])
            if '漲跌' in title and rows:
                up = down = flat = 0
                for row in rows:
                    label = str(row[0])
                    val   = parse_num(row[1]) if len(row) > 1 else 0
                    if '上漲' in label:   up   = val
                    elif '下跌' in label: down = val
                    elif '平盤' in label: flat = val
                if up > 0 or down > 0:
                    return {'adl_up': up, 'adl_down': down, 'adl_flat': flat,
                            'adl_diff': up - down,
                            'adl_breadth': round(up / max(up+down, 1) * 100, 1)}
    except:
        pass
    return None

def fetch_adl_month(year, month):
    """
    嘗試該月的每一天，找到有資料的那天
    月統計端點只有當天有效，所以找月初第一個交易日
    """
    last = calendar.monthrange(year, month)[1]
    for day in range(1, min(last+1, 8)):  # 只試前7天找到第一個交易日
        d = datetime.date(year, month, day)
        if d.weekday() >= 5:
            continue
        result = fetch_adl_for_date(d.strftime('%Y%m%d'))
        if result:
            return result
        time.sleep(0.2)
    return None

now = datetime.date.today()
print("開始收集藤落線月統計資料...")

def iter_months(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y+1

rows = []
for y, m in iter_months(2004, 1, now.year, now.month):
    result = fetch_adl_month(y, m)
    if result:
        result['date'] = datetime.date(y, m, 1)
        rows.append(result)
        print(f"  ✅ {y}/{m:02d}：上漲 {result['adl_up']:,}，下跌 {result['adl_down']:,}，廣度 {result['adl_breadth']}%")
    else:
        print(f"  ⚠️ {y}/{m:02d}：無資料")
    time.sleep(0.3)

if not rows:
    print("❌ 無法取得資料")
    sys.exit(1)

df = pd.DataFrame(rows)
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date').sort_index()
df['adl']     = df['adl_diff'].cumsum()
df['adl_ma3'] = df['adl'].rolling(3).mean()
df['adl_ma12']= df['adl'].rolling(12).mean()
df['adl_trend']= (df['adl_ma3'] > df['adl_ma12']).astype(int)

print(f"\n✅ 完成！共 {len(df)} 個月")
print(f"   日期範圍：{df.index[0].date()} ～ {df.index[-1].date()}")
print(f"\n   最近6筆：")
print(df[['adl_up','adl_down','adl_diff','adl_breadth','adl_trend']].tail(6).to_string())

df.index.name = 'date'
df.to_csv('data/adl_data.csv')
print(f"\n✅ 已儲存至 data/adl_data.csv")
