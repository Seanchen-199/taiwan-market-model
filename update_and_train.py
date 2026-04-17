"""
台股多因子模型 - 每日更新資料 + 每週重訓模型
由 GitHub Actions 呼叫
- 每天：把最新一天的資料附加到 raw_data.csv
- 每週：重新訓練模型，更新 model_output.json
"""
 
import subprocess, sys, os, json, time, datetime, warnings, math
import argparse
 
def install(pkg):
    subprocess.check_call([sys.executable,'-m','pip','install',pkg,'-q'])
for pkg in ['yfinance','pandas','numpy','scikit-learn','requests']:
    install(pkg)
 
import pandas as pd
import numpy as np
import yfinance as yf
import requests
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import accuracy_score
warnings.filterwarnings('ignore')
 
parser = argparse.ArgumentParser()
parser.add_argument('--retrain', action='store_true', help='重新訓練模型')
args = parser.parse_args()
 
os.makedirs('data', exist_ok=True)
today = datetime.date.today()
print(f"執行日期：{today}")
print(f"模式：{'重新訓練' if args.retrain else '只更新資料'}")
 
H_TWSE   = {'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'}
H_TAIFEX = {'User-Agent':'Mozilla/5.0','Referer':'https://www.taifex.com.tw/'}
 
def roc_to_date(s):
    try:
        p = str(s).strip().split('/')
        if len(p)==3:
            y=int(p[0])
            if y<1000: y+=1911
            return datetime.date(y,int(p[1]),int(p[2]))
    except: pass
    return None
 
def pn(s):
    try: return int(str(s).replace(',','').replace(' ',''))
    except: return 0
 
# ════════════════════════════════════════════════════
# 1. 抓取最近 5 個交易日的資料（確保最新資料納入）
# ════════════════════════════════════════════════════
print("\n【1】更新最新市場資料")
end_date   = today.strftime('%Y-%m-%d')
start_date = (today - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
 
YAHOO = {
    '^SOX':'SOX', '^TWII':'TWII', 'DX-Y.NYB':'DXY',
    'USDTWD=X':'USDTWD', '^TNX':'US10Y', '^VIX':'VIX',
    '^IXIC':'NASDAQ', '0050.TW':'ETF0050',
}
new_frames = {}
for sym, name in YAHOO.items():
    try:
        df = yf.download(sym, start=start_date, end=end_date,
                         progress=False, auto_adjust=True)
        if df.empty: continue
        s = df['Close'].squeeze(); s.name = name
        new_frames[name] = s
        print(f"  ✅ {name}: {len(s)} 筆")
    except Exception as e:
        print(f"  ❌ {name}: {e}")
    time.sleep(0.3)
 
if new_frames:
    new_df = pd.DataFrame(new_frames)
    new_df.index = pd.to_datetime(new_df.index)
 
    # TWSE 三大法人（最近幾天）
    for days_back in range(7):
        d = today - datetime.timedelta(days=days_back)
        if d.weekday() >= 5: continue
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={d.strftime('%Y%m%d')}&selectType=ALL"
        try:
            resp = requests.get(url, timeout=20, headers=H_TWSE)
            data = resp.json()
            if data.get('stat') == 'OK' and data.get('data'):
                rows = data['data']
                total_row = None
                for r in reversed(rows):
                    if '合計' in str(r[0]):
                        total_row = r; break
                # 取最後一行（合計行），欄位索引與 fetch_data.py 一致
                last        = rows[-1]
                foreign_net = pn(last[4])  if len(last) > 4  else 0
                invest_net  = pn(last[7])  if len(last) > 7  else 0
                dealer_net  = pn(last[10]) if len(last) > 10 else 0
 
                # 合理性檢查：單日外資買賣超不應超過 500 億（50,000,000 千元）
                if abs(foreign_net) > 50_000_000:
                    print(f"  [警告] 外資數值異常（{foreign_net/100_000:.1f}億），略過")
                    continue
 
                pd_date = pd.Timestamp(d)
                new_df.loc[pd_date, 'foreign_net_bil'] = foreign_net / 100_000
                new_df.loc[pd_date, 'invest_net_bil']  = invest_net  / 100_000
                new_df.loc[pd_date, 'dealer_net_bil']  = dealer_net  / 100_000
                print(f"  ✅ 三大法人 {d}：外資 {foreign_net/100_000:.1f}億，投信 {invest_net/100_000:.1f}億")
                break
        except Exception as e:
            print(f"  [警告] 三大法人 {d}: {e}")
        time.sleep(0.3)
 
    # TAIFEX 期貨（最近幾天）
    import calendar as cal
    for days_back in range(7):
        d = today - datetime.timedelta(days=days_back)
        if d.weekday() >= 5: continue
        date_str = d.strftime('%Y/%m/%d')
        try:
            resp = requests.post(
                'https://www.taifex.com.tw/cht/3/futContractsDateDown',
                data={'queryStartDate':date_str,'queryEndDate':date_str,'commodityId':'TXF'},
                headers=H_TAIFEX, timeout=20)
            if '<html' not in resp.text[:100].lower():
                for line in resp.text.strip().split('\n'):
                    if '外資' in line or 'Foreign' in line:
                        cols=[c.strip().strip('"') for c in line.split(',')]
                        nums=[]
                        for c in cols:
                            try: nums.append(int(c.replace(',','')))
                            except: pass
                        if len(nums)>=5:
                            pd_date = pd.Timestamp(d)
                            new_df.loc[pd_date,'TXF_net'] = nums[4]
                            print(f"  ✅ TXF 期貨 {d}：{nums[4]:,} 口")
                            break
                break
        except: pass
        time.sleep(0.3)
 
    # 前五大/十大交易人
    for days_back in range(7):
        d = today - datetime.timedelta(days=days_back)
        if d.weekday() >= 5: continue
        date_str = d.strftime('%Y/%m/%d')
        try:
            resp = requests.post(
                'https://www.taifex.com.tw/cht/3/largeTraderFutDown',
                data={'queryStartDate':date_str,'queryEndDate':date_str,'commodityId':'TX'},
                headers=H_TAIFEX, timeout=20)
            if '<html' not in resp.text[:100].lower():
                for line in resp.text.strip().split('\n'):
                    cols=[c.strip().strip('"') for c in line.split(',')]
                    nums=[]
                    for c in cols:
                        try: nums.append(int(c.replace(',','')))
                        except: pass
                    if len(nums)>=6:
                        pd_date = pd.Timestamp(d)
                        new_df.loc[pd_date,'top5_net']  = nums[0]-nums[1]
                        new_df.loc[pd_date,'top10_net'] = nums[4]-nums[5] if len(nums)>5 else 0
                        print(f"  ✅ 前五大/十大 {d}：top5={nums[0]-nums[1]:,}")
                        break
                break
        except: pass
        time.sleep(0.3)
 
    # 讀取現有 raw_data.csv，合併新資料
    raw_path = 'data/raw_data.csv'
    if os.path.exists(raw_path):
        existing = pd.read_csv(raw_path, index_col='date', parse_dates=True)
        # 合併：新資料覆蓋舊資料（如果同日期）
        combined = pd.concat([existing, new_df]).sort_index()
        combined = combined[~combined.index.duplicated(keep='last')]
 
        # 重新計算衍生指標
        if 'TWII' in combined.columns:
            p = combined['TWII']
            combined['MA5']  = p.rolling(5).mean()
            combined['MA20'] = p.rolling(20).mean()
            combined['MA60'] = p.rolling(60).mean()
            delta = p.diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            rs    = gain / loss.replace(0, float('nan'))
            combined['RSI14']       = 100 - (100/(1+rs))
            ema12 = p.ewm(span=12,adjust=False).mean()
            ema26 = p.ewm(span=26,adjust=False).mean()
            combined['MACD']        = ema12 - ema26
            combined['MACD_signal'] = combined['MACD'].ewm(span=9,adjust=False).mean()
            combined['MACD_hist']   = combined['MACD'] - combined['MACD_signal']
 
        if 'margin_balance' in combined.columns:
            combined['margin_chg5'] = combined['margin_balance'].pct_change(5)
 
        if 'ETF0050' in combined.columns:
            combined['target_5d_return'] = combined['ETF0050'].pct_change(5).shift(-5)
            combined['target_signal']    = (combined['target_5d_return'] > 0).astype(int)
 
        combined.index.name = 'date'
        combined.to_csv(raw_path)
        print(f"\n✅ raw_data.csv 已更新：{len(combined)} 列 × {combined.shape[1]} 欄")
        df = combined
    else:
        print("❌ raw_data.csv 不存在，請先執行歷史資料收集")
        sys.exit(1)
else:
    print("❌ Yahoo 資料抓取失敗")
    sys.exit(1)
 
# ════════════════════════════════════════════════════
# 2. 重新訓練模型（只在 --retrain 模式下執行）
# ════════════════════════════════════════════════════
if not args.retrain:
    print("\n✅ 資料更新完成（未重新訓練）")
    sys.exit(0)
 
print("\n【2】重新訓練模型")
 
CORE_FEATURES = [
    'SOX','US10Y','TWII','DXY','USDTWD',
    'foreign_net_bil','invest_net_bil',
    'top5_net','top10_net',
]
SECONDARY_FEATURES = ['RSI14','MACD','MACD_hist','MA5','MA20']
CORE_FEATURES      = [f for f in CORE_FEATURES      if f in df.columns]
SECONDARY_FEATURES = [f for f in SECONDARY_FEATURES if f in df.columns]
ALL_FEATURES       = CORE_FEATURES + SECONDARY_FEATURES
 
feat_df = df[ALL_FEATURES].copy()
 
PRICE_COLS = ['SOX','TWII','DXY','USDTWD','US10Y','foreign_net_bil','invest_net_bil']
PRICE_COLS = [c for c in PRICE_COLS if c in feat_df.columns]
for col in PRICE_COLS:
    feat_df[f'{col}_ret1'] = feat_df[col].pct_change(1)
    feat_df[f'{col}_ret5'] = feat_df[col].pct_change(5)
 
LAG_COLS = [f'{c}_ret1' for c in PRICE_COLS if f'{c}_ret1' in feat_df.columns]
LAG_COLS += [c for c in SECONDARY_FEATURES if c in feat_df.columns]
for col in LAG_COLS:
    for lag in [1,3,5]:
        feat_df[f'{col}_lag{lag}'] = feat_df[col].shift(lag)
 
feat_df = feat_df.drop(columns=[c for c in PRICE_COLS if c in feat_df.columns], errors='ignore')
feat_df = feat_df.replace([float('inf'),float('-inf')],float('nan'))
for col in feat_df.columns:
    try:
        feat_df[col] = feat_df[col].clip(
            lower=feat_df[col].quantile(0.001),
            upper=feat_df[col].quantile(0.999))
    except: pass
 
MUST_HAVE = [c for c in feat_df.columns
             if any(k in c for k in ['SOX','TWII','DXY','USDTWD','US10Y','NASDAQ','VIX','RSI','MACD','MA5'])]
FILL_ZERO = [c for c in feat_df.columns if c not in MUST_HAVE]
feat_df[FILL_ZERO] = feat_df[FILL_ZERO].fillna(0)
 
target   = df['target_signal'].copy()
combined_ft = feat_df.join(target)
combined_ft = combined_ft.dropna(subset=MUST_HAVE+['target_signal'])
feat_df  = combined_ft.drop(columns=['target_signal'])
target   = combined_ft['target_signal']
 
# 移除高共線性
corr = feat_df.corr().abs()
cols = corr.columns
drop_cols = set()
for i in range(len(cols)):
    for j in range(i+1,len(cols)):
        if corr.iloc[i,j]>0.9:
            drop_cols.add(cols[j])
feat_df = feat_df.drop(columns=list(drop_cols),errors='ignore')
feature_cols = feat_df.columns.tolist()
 
scaler = StandardScaler()
X = pd.DataFrame(scaler.fit_transform(feat_df),index=feat_df.index,columns=feature_cols)
y = target.loc[X.index]
 
n=len(X)
tr_end=int(n*0.60); va_end=int(n*0.80)
X_train,y_train=X.iloc[:tr_end],y.iloc[:tr_end]
X_val,y_val=X.iloc[tr_end:va_end],y.iloc[tr_end:va_end]
X_test,y_test=X.iloc[va_end:],y.iloc[va_end:]
 
print(f"  樣本數：{len(X)}，訓練：{len(X_train)}，驗證：{len(X_val)}，測試：{len(X_test)}")
 
tscv=TimeSeriesSplit(n_splits=5)
models={
    'LogisticRegression': LogisticRegression(C=0.1,max_iter=1000,random_state=42),
    'RandomForest':       RandomForestClassifier(n_estimators=100,max_depth=5,random_state=42),
    'GradientBoosting':   GradientBoostingClassifier(n_estimators=100,max_depth=3,
                                                      learning_rate=0.05,random_state=42),
}
results={}
for name,model in models.items():
    try:
        cv=cross_val_score(model,X_train,y_train,cv=tscv,scoring='accuracy')
        cv_mean=cv.mean()
    except: cv_mean=0.5
    model.fit(X_train,y_train)
    val_acc=accuracy_score(y_val,model.predict(X_val))
    test_acc=accuracy_score(y_test,model.predict(X_test))
    results[name]={'cv':cv_mean,'val':val_acc,'test':test_acc,'model':model}
    print(f"  {name}: CV={cv_mean:.3f} Val={val_acc:.3f} Test={test_acc:.3f}")
 
best_name=max(results,key=lambda k:results[k]['val'])
best_model=results[best_name]['model']
print(f"  最佳模型：{best_name}（驗證集 {results[best_name]['val']:.3f}）")
 
# 因子重要性
if hasattr(best_model,'feature_importances_'):
    imp=best_model.feature_importances_
elif hasattr(best_model,'coef_'):
    imp=np.abs(best_model.coef_[0])
else:
    imp=np.ones(len(feature_cols))
 
imp_df=pd.DataFrame({'feature':feature_cols,'importance':imp}).sort_values('importance',ascending=False)
imp_df['is_core']=imp_df['feature'].apply(lambda f:any(c in f for c in CORE_FEATURES))
 
# 預測當前
latest_X=X.iloc[[-1]]
pred=int(best_model.predict(latest_X)[0])
proba=best_model.predict_proba(latest_X)[0]
bull_prob=float(proba[list(best_model.classes_).index(1)]) if 1 in best_model.classes_ else 0.5
 
print(f"\n  當前訊號：{'多方' if pred==1 else '空方'}")
print(f"  多方機率：{bull_prob*100:.1f}%")
print(f"  預測日期：{X.index[-1].date()}")
 
# 滾動回測
def rolling_backtest(X,y,price_series,window=504,step=21):
    returns=[]; dates=[]
    prices=price_series.reindex(X.index).ffill()
    for start in range(0,len(X)-window-step,step):
        end=start+window
        X_tr=X.iloc[start:end]; y_tr=y.iloc[start:end]
        X_fw=X.iloc[end:end+step]; p_fw=prices.iloc[end:end+step]
        if len(X_tr)<120 or len(X_fw)==0 or y_tr.nunique()<2: continue
        m=GradientBoostingClassifier(n_estimators=100,max_depth=3,
                                      learning_rate=0.05,random_state=42,subsample=0.8)
        try:
            m.fit(X_tr,y_tr)
            preds=m.predict(X_fw)
            proba=m.predict_proba(X_fw)
            bull_col=list(m.classes_).index(1) if 1 in m.classes_ else 0
        except: continue
        for i in range(len(X_fw)-1):
            if i>=len(p_fw)-1: break
            raw_ret=(p_fw.iloc[i+1]-p_fw.iloc[i])/p_fw.iloc[i]
            bp=proba[i][bull_col] if len(proba)>i else 0.5
            sig=1 if (preds[i]==1 and bp>0.55) else 0
            returns.append(raw_ret*sig); dates.append(X_fw.index[i])
    return pd.Series(returns,index=dates)
 
price_col='ETF0050' if 'ETF0050' in df.columns else 'TWII'
bt_returns=rolling_backtest(X,y,df[price_col])
bh_returns=df[price_col].pct_change().reindex(bt_returns.index).dropna()
 
def calc_metrics(returns):
    if len(returns)==0: return {}
    equity=(1+returns).cumprod()
    total_ret=equity.iloc[-1]-1
    n_years=len(returns)/252
    cagr=(1+total_ret)**(1/max(n_years,0.1))-1
    peak=equity.cummax()
    max_dd=(equity-peak).div(peak).min()
    ann_ret=returns.mean()*252
    ann_std=returns.std()*np.sqrt(252)
    sharpe=ann_ret/ann_std if ann_std>0 else 0
    win_rate=(returns>0).sum()/max((returns!=0).sum(),1)
    return {'cagr':round(float(cagr*100),2),'max_dd':round(float(max_dd*100),2),
            'sharpe':round(float(sharpe),3),'win_rate':round(float(win_rate*100),2),
            'total_ret':round(float(total_ret*100),2),'n_trades':int((returns!=0).sum())}
 
bt_m=calc_metrics(bt_returns); bh_m=calc_metrics(bh_returns)
print(f"\n  回測：CAGR={bt_m.get('cagr',0):.2f}%  夏普={bt_m.get('sharpe',0):.3f}  最大回撤={bt_m.get('max_dd',0):.2f}%")
 
# 淨值曲線
equity_curve=(1+bt_returns).cumprod()
if len(equity_curve)>250: equity_curve=equity_curve.iloc[-250:]
bh_curve=(1+bh_returns.reindex(equity_curve.index)).cumprod()
curve_data=[]
for d,v in equity_curve.items():
    bh_v=float(bh_curve.get(d,float('nan')))
    curve_data.append({'date':str(d.date()),'strategy':round(float(v),4),
                       'buyhold':round(bh_v,4) if not np.isnan(bh_v) else None})
 
# 整合輸出
top_factors=[]
for _,row in imp_df.head(20).iterrows():
    top_factors.append({'name':row['feature'],'importance':round(float(row['importance']),5),
                        'is_core':bool(row['is_core']),
                        'pct':round(float(row['importance']/imp_df['importance'].sum()*100),1)})
 
model_comparison=[]
for name,res in results.items():
    model_comparison.append({'name':name,'cv_acc':round(res['cv']*100,1),
                              'val_acc':round(res['val']*100,1),
                              'test_acc':round(res['test']*100,1),'is_best':name==best_name})
 
def clean(obj):
    if isinstance(obj,dict): return {k:clean(v) for k,v in obj.items()}
    elif isinstance(obj,list): return [clean(v) for v in obj]
    elif isinstance(obj,float):
        if math.isnan(obj) or math.isinf(obj): return None
        return round(obj,6)
    return obj
 
output=clean({
    'updated_at':datetime.datetime.now().strftime('%Y/%m/%d %H:%M'),
    'model_name':best_name,
    'data_range':{'start':str(X.index[0].date()),'end':str(X.index[-1].date()),'days':len(X)},
    'split':{'train':len(X_train),'val':len(X_val),'test':len(X_test)},
    'current_signal':{'signal':pred,'signal_label':'多方' if pred==1 else '空方',
                      'bull_prob':round(bull_prob*100,1),'bear_prob':round((1-bull_prob)*100,1),
                      'date':str(X.index[-1].date())},
    'backtest':{'strategy':bt_m,'buyhold':bh_m},
    'factor_importance':top_factors,
    'equity_curve':curve_data,
    'model_comparison':model_comparison,
    'feature_count':{'core':len(CORE_FEATURES),'secondary':len(SECONDARY_FEATURES),
                     'total':len(feature_cols)},
})
 
with open('data/model_output.json','w',encoding='utf-8') as f:
    json.dump(output,f,ensure_ascii=False,indent=2)
 
print(f"\n✅ model_output.json 已更新")
print(f"   訊號：{'多方' if pred==1 else '空方'}（多方機率 {bull_prob*100:.1f}%）")
