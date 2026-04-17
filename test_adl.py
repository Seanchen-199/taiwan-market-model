"""
測試藤落線正確 API 端點
"""
import subprocess, sys
subprocess.check_call([sys.executable,'-m','pip','install','requests','-q'])

import requests, time, json

H = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}

tests = [
    ("大盤統計-上漲下跌家數",
     "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260416&type=ALLBUT0999"),
    ("每日大盤行情",
     "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260416&type=MS"),
    ("個股漲跌幅統計",
     "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260416&type=ALLBUT0999"),
    ("大盤成交資訊",
     "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU?response=json&date=20260416"),
    ("每日行情-漲跌家數",
     "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260416&type=ALL"),
    ("歷史漲跌家數",
     "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260416&selectType=MS"),
]

for name, url in tests:
    print(f"\n{'='*50}")
    print(f"測試：{name}")
    try:
        resp = requests.get(url, timeout=15, headers=H)
        data = resp.json()
        stat = data.get('stat','')
        tables = data.get('tables', [])
        print(f"stat：{stat}")
        print(f"tables 數量：{len(tables)}")
        for i, t in enumerate(tables):
            title = t.get('title','')
            fields = t.get('fields', [])
            rows = t.get('data', [])
            if rows and ('漲' in str(fields) or '跌' in str(fields) or '家' in str(fields)):
                print(f"  ✅ table[{i}] 標題：{title}")
                print(f"     欄位：{fields}")
                print(f"     筆數：{len(rows)}")
                print(f"     最後一筆：{rows[-1]}")
    except Exception as e:
        print(f"錯誤：{e}")
    time.sleep(0.5)

# 專門找漲跌家數的端點
print(f"\n{'='*50}")
print("測試：TWSE 每日大盤統計（舊格式）")
url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260416"
try:
    resp = requests.get(url, timeout=15, headers=H)
    data = resp.json()
    print(f"stat：{data.get('stat')}")
    tables = data.get('tables', [])
    print(f"tables 數量：{len(tables)}")
    for i, t in enumerate(tables):
        title = t.get('title','--')
        fields = t.get('fields', [])
        rows   = t.get('data', [])
        print(f"  table[{i}]：{title}，欄位：{fields[:5]}，筆數：{len(rows)}")
        if rows:
            print(f"    最後一筆：{rows[-1]}")
except Exception as e:
    print(f"錯誤：{e}")

# 嘗試月統計
print(f"\n{'='*50}")
print("測試：TWSE 月統計漲跌家數")
url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260401&type=MS"
try:
    resp = requests.get(url, timeout=15, headers=H)
    data = resp.json()
    print(f"stat：{data.get('stat')}")
    tables = data.get('tables', [])
    for i, t in enumerate(tables):
        title  = t.get('title','--')
        fields = t.get('fields', [])
        rows   = t.get('data', [])
        print(f"  table[{i}]：{title}")
        print(f"    欄位：{fields}")
        if rows:
            print(f"    前2筆：{rows[:2]}")
except Exception as e:
    print(f"錯誤：{e}")
