"""
測試 TWSE 不同日期的漲跌家數格式
"""
import subprocess, sys
subprocess.check_call([sys.executable,'-m','pip','install','requests','-q'])

import requests, time

H = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}

# 測試不同日期
dates = ['20260416', '20260415', '20260410', '20250101', '20240601', '20200601']

for date_str in dates:
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date={date_str}"
    try:
        resp = requests.get(url, timeout=15, headers=H)
        data = resp.json()
        stat = data.get('stat','')
        tables = data.get('tables', [])
        # 找 table[7]
        found = False
        for i, t in enumerate(tables):
            title  = t.get('title','')
            rows   = t.get('data', [])
            fields = t.get('fields', [])
            if rows and ('漲跌' in title or i == 7):
                print(f"  {date_str} table[{i}]：{title}")
                print(f"    欄位：{fields}")
                print(f"    筆數：{len(rows)}")
                if rows:
                    print(f"    第一筆：{rows[0]}")
                found = True
        if not found:
            print(f"  {date_str}：stat={stat}，未找到漲跌資料（tables={len(tables)}）")
    except Exception as e:
        print(f"  {date_str}：錯誤 {e}")
    time.sleep(0.5)

print("\n" + "="*50)
print("測試月統計端點（歷史）")
for date_str in ['20260401', '20250401', '20240401', '20200401']:
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date={date_str}&type=MS"
    try:
        resp = requests.get(url, timeout=15, headers=H)
        data = resp.json()
        stat = data.get('stat','')
        tables = data.get('tables', [])
        for i, t in enumerate(tables):
            rows = t.get('data', [])
            title = t.get('title','')
            if rows and '漲跌' in title:
                print(f"  {date_str} table[{i}]：{title}，筆數：{len(rows)}")
                print(f"    前2筆：{rows[:2]}")
    except Exception as e:
        print(f"  {date_str}：錯誤 {e}")
    time.sleep(0.5)

print("\n" + "="*50)
print("測試個股漲跌統計（計算漲跌家數）")
# 直接從個股資料計算漲跌家數
for date_str in ['20260416', '20250101', '20200601']:
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date={date_str}&type=ALLBUT0999"
    try:
        resp = requests.get(url, timeout=15, headers=H)
        data = resp.json()
        tables = data.get('tables', [])
        for t in tables:
            rows = t.get('data', [])
            title = t.get('title','')
            if rows and '收盤行情' in title:
                # 從漲跌欄位計算
                up = down = flat = 0
                for row in rows:
                    if len(row) > 9:
                        chg = str(row[9])
                        if 'red' in chg or '+' in chg:
                            up += 1
                        elif 'green' in chg or '-' in chg:
                            down += 1
                        else:
                            flat += 1
                print(f"  {date_str}：上漲 {up}，下跌 {down}，平盤 {flat}（共 {len(rows)} 檔）")
    except Exception as e:
        print(f"  {date_str}：錯誤 {e}")
    time.sleep(0.5)
