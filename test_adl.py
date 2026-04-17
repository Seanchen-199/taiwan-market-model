"""
測試藤落線（ADL）和景氣燈號資料來源
在 GitHub Actions 執行：python test_adl.py
"""
import subprocess, sys
subprocess.check_call([sys.executable,'-m','pip','install','requests','-q'])

import requests, time, re

H_TWSE = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}

print("="*60)
print("診斷 1：TWSE 每日漲跌家數（藤落線原始資料）")
print("="*60)

# 方法一：TWSE 每日市場成交資訊
url1 = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=json&date=20260416&selectType=ALL"
try:
    resp = requests.get(url1, timeout=15, headers=H_TWSE)
    print(f"HTTP 狀態：{resp.status_code}")
    data = resp.json()
    print(f"stat：{data.get('stat')}")
    if data.get('data'):
        print(f"筆數：{len(data['data'])}")
        print(f"欄位：{data.get('fields')}")
        print(f"第一筆：{data['data'][0]}")
except Exception as e:
    print(f"錯誤：{e}")

time.sleep(1)

# 方法二：TWSE 上漲下跌家數
print("\n" + "="*60)
print("診斷 2：TWSE 漲跌家數統計")
print("="*60)
url2 = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260416&type=MS"
try:
    resp = requests.get(url2, timeout=15, headers=H_TWSE)
    print(f"HTTP 狀態：{resp.status_code}")
    data = resp.json()
    print(f"stat：{data.get('stat')}")
    tables = data.get('tables', [])
    print(f"tables 數量：{len(tables)}")
    for i, t in enumerate(tables[:3]):
        print(f"  table[{i}] 標題：{t.get('title','')}")
        rows = t.get('data', [])
        print(f"  筆數：{len(rows)}")
        if rows:
            print(f"  欄位：{t.get('fields')}")
            print(f"  最後幾筆：{rows[-3:]}")
except Exception as e:
    print(f"錯誤：{e}")

time.sleep(1)

# 方法三：另一個端點
print("\n" + "="*60)
print("診斷 3：TWSE 大盤統計資訊（漲跌家數）")
print("="*60)
url3 = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260416&type=IND"
try:
    resp = requests.get(url3, timeout=15, headers=H_TWSE)
    print(f"HTTP 狀態：{resp.status_code}")
    data = resp.json()
    print(f"stat：{data.get('stat')}")
    tables = data.get('tables', [])
    for i, t in enumerate(tables):
        title = t.get('title','')
        if '漲' in title or '跌' in title or '家' in title:
            print(f"  ✅ 找到相關 table：{title}")
            print(f"  欄位：{t.get('fields')}")
            print(f"  資料：{t.get('data',[])[:5]}")
except Exception as e:
    print(f"錯誤：{e}")

time.sleep(1)

# 方法四：直接搜尋漲跌家數
print("\n" + "="*60)
print("診斷 4：TWSE 每日收盤行情（含漲跌家數）")
print("="*60)
url4 = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=json&date=20260416&selectType=MS"
try:
    resp = requests.get(url4, timeout=15, headers=H_TWSE)
    print(f"HTTP 狀態：{resp.status_code}")
    print(f"回應前300字：{resp.text[:300]}")
except Exception as e:
    print(f"錯誤：{e}")

time.sleep(1)

print("\n" + "="*60)
print("診斷 5：國發會景氣燈號")
print("="*60)
url5 = "https://index.ndc.gov.tw/n/zh_tw/data/business_monitor"
try:
    resp = requests.get(url5, timeout=15,
                        headers={'User-Agent':'Mozilla/5.0','Referer':'https://index.ndc.gov.tw/'})
    print(f"HTTP 狀態：{resp.status_code}")
    print(f"回應前500字：{resp.text[:500]}")
    # 找燈號關鍵字
    if '紅燈' in resp.text or '黃紅燈' in resp.text or '綠燈' in resp.text:
        print("✅ 找到燈號資料！")
        for keyword in ['紅燈','黃紅燈','綠燈','黃藍燈','藍燈']:
            if keyword in resp.text:
                idx = resp.text.index(keyword)
                print(f"  {keyword} 出現位置附近：{resp.text[max(0,idx-30):idx+50]}")
    else:
        print("❌ 未找到燈號關鍵字")
except Exception as e:
    print(f"錯誤：{e}")

time.sleep(1)

print("\n" + "="*60)
print("診斷 6：國發會景氣燈號 API")
print("="*60)
url6 = "https://index.ndc.gov.tw/n/zh_tw/api/business_monitor"
try:
    resp = requests.get(url6, timeout=15,
                        headers={'User-Agent':'Mozilla/5.0','Referer':'https://index.ndc.gov.tw/'})
    print(f"HTTP 狀態：{resp.status_code}")
    print(f"回應前500字：{resp.text[:500]}")
except Exception as e:
    print(f"錯誤：{e}")
