"""
測試 TWSE 不同年份的 API 格式
執行後會顯示哪個端點可以抓到資料
在 GitHub Actions 上執行：python test_twse.py
"""
import subprocess, sys
subprocess.check_call([sys.executable,'-m','pip','install','requests','-q'])

import requests, json, time

HEADERS = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}

def test_institutional(year, month):
    """測試三大法人 T86"""
    date_str = f"{year}{month:02d}01"
    
    # 目前使用的端點（rwd）
    url1 = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date_str}&selectType=ALL"
    # 舊端點
    url2 = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
    # 更舊的端點
    url3 = f"https://www.twse.com.tw/exchangeReport/T86?response=json&date={date_str}&selectType=ALL"
    
    for i, url in enumerate([url1, url2, url3], 1):
        try:
            resp = requests.get(url, timeout=15, headers=HEADERS)
            data = resp.json()
            stat = data.get('stat','')
            rows = len(data.get('data', []))
            print(f"  端點{i}: stat={stat}, 資料筆數={rows}")
            if stat == 'OK' and rows > 0:
                print(f"    ✅ 成功！使用端點{i}")
                return True, url
        except Exception as e:
            print(f"  端點{i}: 錯誤 {e}")
        time.sleep(0.3)
    return False, None

def test_margin(year, month):
    """測試融資餘額 MI_MARGN"""
    date_str = f"{year}{month:02d}01"
    
    url1 = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json&date={date_str}&selectType=CS"
    url2 = f"https://www.twse.com.tw/marginTrading/MI_MARGN?response=json&date={date_str}&selectType=CS"
    url3 = f"https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={date_str}&selectType=CS"
    
    for i, url in enumerate([url1, url2, url3], 1):
        try:
            resp = requests.get(url, timeout=15, headers=HEADERS)
            data = resp.json()
            stat = data.get('stat','')
            tables = data.get('tables', [])
            rows = len(tables[0].get('data',[])) if tables else 0
            print(f"  端點{i}: stat={stat}, 資料筆數={rows}")
            if stat == 'OK' and rows > 0:
                print(f"    ✅ 成功！使用端點{i}")
                return True, url
        except Exception as e:
            print(f"  端點{i}: 錯誤 {e}")
        time.sleep(0.3)
    return False, None

def test_taifex_futures(year, month, commodity='TXF'):
    """測試 TAIFEX 期貨"""
    import calendar
    last = calendar.monthrange(year, month)[1]
    
    urls = [
        ('futContractsDateDown', f"https://www.taifex.com.tw/cht/3/futContractsDateDown"),
        ('futContractsDate',     f"https://www.taifex.com.tw/cht/3/futContractsDate"),
    ]
    
    for name, url in urls:
        try:
            resp = requests.post(url, data={
                'queryStartDate': f"{year}/{month:02d}/01",
                'queryEndDate':   f"{year}/{month:02d}/{last:02d}",
                'commodityId':    commodity,
            }, headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.taifex.com.tw/'}, timeout=15)
            
            lines = [l for l in resp.text.strip().split('\n') if '外資' in l or 'Foreign' in l]
            print(f"  {name}: 找到外資行 {len(lines)} 筆")
            if lines:
                print(f"    ✅ 成功！範例：{lines[0][:80]}")
                return True
        except Exception as e:
            print(f"  {name}: 錯誤 {e}")
        time.sleep(0.3)
    return False

# ── 測試各年份 ────────────────────────────────────────────────
test_cases = [
    (2010, 6),
    (2015, 6),
    (2018, 6),
    (2022, 6),
    (2024, 6),
]

print("="*60)
print("TWSE 三大法人 T86 測試")
print("="*60)
for year, month in test_cases:
    print(f"\n{year}/{month:02d}：")
    ok, url = test_institutional(year, month)
    time.sleep(0.5)

print("\n"+"="*60)
print("TWSE 融資餘額 MI_MARGN 測試")
print("="*60)
for year, month in test_cases:
    print(f"\n{year}/{month:02d}：")
    ok, url = test_margin(year, month)
    time.sleep(0.5)

print("\n"+"="*60)
print("TAIFEX TXF 期貨測試")
print("="*60)
for year, month in test_cases:
    print(f"\n{year}/{month:02d}：")
    ok = test_taifex_futures(year, month, 'TXF')
    time.sleep(0.5)
