"""
API 診斷腳本 - 顯示完整錯誤訊息和回應內容
在 GitHub Actions 執行：python diagnose_api.py
"""
import subprocess, sys
subprocess.check_call([sys.executable,'-m','pip','install','requests','-q'])

import requests, time, calendar, re

H_TWSE   = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}
H_TAIFEX = {'User-Agent':'Mozilla/5.0','Referer':'https://www.taifex.com.tw/'}

print("="*60)
print("診斷 1：TWSE 三大法人 T86（2015/06）")
print("="*60)
url = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date=20150601&selectType=ALL"
try:
    resp = requests.get(url, timeout=25, headers=H_TWSE)
    print(f"HTTP 狀態碼：{resp.status_code}")
    print(f"回應前500字：{resp.text[:500]}")
    data = resp.json()
    print(f"stat：{data.get('stat')}")
    print(f"data 筆數：{len(data.get('data',[]))}")
    if data.get('data'):
        print(f"第一筆：{data['data'][0]}")
except Exception as e:
    print(f"完整錯誤：{type(e).__name__}: {e}")

print("\n"+"="*60)
print("診斷 2：TWSE 融資餘額 MI_MARGN（2015/06）")
print("="*60)
for date_str in ['20150601','20150615','20150630']:
    url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json&date={date_str}&selectType=ALL"
    try:
        resp = requests.get(url, timeout=25, headers=H_TWSE)
        print(f"\n日期{date_str} HTTP狀態：{resp.status_code}")
        print(f"回應前300字：{resp.text[:300]}")
        data = resp.json()
        print(f"stat：{data.get('stat')}")
        tables = data.get('tables',[])
        print(f"tables 數量：{len(tables)}")
        if tables:
            print(f"第一個table筆數：{len(tables[0].get('data',[]))}")
    except Exception as e:
        print(f"錯誤：{type(e).__name__}: {e}")
    time.sleep(0.5)

print("\n"+"="*60)
print("診斷 3：TAIFEX TXF 舊端點（2015/06）")
print("="*60)
url = "https://www.taifex.com.tw/cht/3/futContractsDate"
try:
    resp = requests.post(url, data={
        'queryStartDate': '2015/06/01',
        'queryEndDate':   '2015/06/30',
        'commodityId':    'TXF',
    }, headers=H_TAIFEX, timeout=25)
    print(f"HTTP 狀態碼：{resp.status_code}")
    print(f"回應前1000字：\n{resp.text[:1000]}")
    # 找外資行
    lines_with_foreign = [l for l in resp.text.split('\n') if '外資' in l or 'Foreign' in l]
    print(f"\n含外資關鍵字的行數：{len(lines_with_foreign)}")
    if lines_with_foreign:
        print(f"前3行：")
        for l in lines_with_foreign[:3]:
            print(f"  {l[:200]}")
except Exception as e:
    print(f"完整錯誤：{type(e).__name__}: {e}")

print("\n"+"="*60)
print("診斷 4：TAIFEX TXF 新端點（2015/06）")
print("="*60)
url = "https://www.taifex.com.tw/cht/3/futContractsDateDown"
try:
    resp = requests.post(url, data={
        'queryStartDate': '2015/06/01',
        'queryEndDate':   '2015/06/30',
        'commodityId':    'TXF',
    }, headers=H_TAIFEX, timeout=25)
    print(f"HTTP 狀態碼：{resp.status_code}")
    print(f"回應前500字：\n{resp.text[:500]}")
    lines_with_foreign = [l for l in resp.text.split('\n') if '外資' in l or 'Foreign' in l]
    print(f"\n含外資關鍵字的行數：{len(lines_with_foreign)}")
    if lines_with_foreign:
        print(f"前3行：")
        for l in lines_with_foreign[:3]:
            print(f"  {l[:200]}")
except Exception as e:
    print(f"完整錯誤：{type(e).__name__}: {e}")

print("\n"+"="*60)
print("診斷 5：TAIFEX 選擇權（2015/06）")
print("="*60)
url = "https://www.taifex.com.tw/cht/3/callsAndPutsDateDown"
try:
    resp = requests.post(url, data={
        'queryStartDate': '2015/06/01',
        'queryEndDate':   '2015/06/30',
        'commodityId':    'TXO',
    }, headers=H_TAIFEX, timeout=25)
    print(f"HTTP 狀態碼：{resp.status_code}")
    print(f"回應前500字：\n{resp.text[:500]}")
    lines_with_foreign = [l for l in resp.text.split('\n') if '外資' in l or 'Foreign' in l]
    print(f"\n含外資關鍵字的行數：{len(lines_with_foreign)}")
    if lines_with_foreign:
        for l in lines_with_foreign[:3]:
            print(f"  {l[:200]}")
except Exception as e:
    print(f"完整錯誤：{type(e).__name__}: {e}")
