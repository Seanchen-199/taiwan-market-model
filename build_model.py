"""
台股多因子模型 - 週頻版（目標：未來4週報酬）
執行：python build_model.py
輸入：data/raw_data.csv
輸出：data/model_output.json
"""

import subprocess, sys, os, json, warnings, math
import numpy as np
import pandas as pd
from datetime import datetime

def install(pkg):
    subprocess.check_call([sys.executable,'-m','pip','install',pkg,'-q'])
for pkg in ['pandas','numpy','scikit-learn','scipy']:
    install(pkg)

from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import accuracy_score
warnings.filterwarnings('ignore')

print("載入資料...")
df_daily = pd.read_csv('data/raw_data.csv', index_col='date', parse_dates=True)
print(f"日資料：{df_daily.shape[0]} 列 × {df_daily.shape[1]} 欄")
print(f"日期：{df_daily.index[0].date()} ～ {df_daily.index[-1].date()}")

# ════════════════════════════════════════════════════
# 1. 日資料 → 週資料（取每週最後一個交易日）
# ════════════════════════════════════════════════════
print("\n降頻：日資料 → 週資料...")

# 價格類：取週末收盤價
PRICE_COLS = ['SOX','TWII','DXY','USDTWD','US10Y','VIX','NASDAQ','ETF0050','ETF006208']
PRICE_COLS = [c for c in PRICE_COLS if c in df_daily.columns]

# 流量類：取週合計（法人買賣超、融資）
SUM_COLS = ['foreign_net_bil','invest_net_bil','dealer_net_bil']
SUM_COLS = [c for c in SUM_COLS if c in df_daily.columns]

# 水位類：取週末值（融資餘額、期貨未平倉）
LAST_COLS = ['margin_balance','short_balance','TXF_net','MTX_net','MXF_net',
             'top5_net','top10_net','opt_call_net','opt_put_net','opt_net']
LAST_COLS = [c for c in LAST_COLS if c in df_daily.columns]

# 技術指標：取週末值
TECH_COLS = ['MA5','MA20','MA60','RSI14','MACD','MACD_signal','MACD_hist']
TECH_COLS = [c for c in TECH_COLS if c in df_daily.columns]

weekly = pd.DataFrame()

# 價格：週末收盤
if PRICE_COLS:
    weekly[PRICE_COLS] = df_daily[PRICE_COLS].resample('W').last()

# 流量：週合計
if SUM_COLS:
    weekly[SUM_COLS] = df_daily[SUM_COLS].resample('W').sum()

# 水位：週末值
if LAST_COLS:
    weekly[LAST_COLS] = df_daily[LAST_COLS].resample('W').last()

# 技術指標：週末值
if TECH_COLS:
    weekly[TECH_COLS] = df_daily[TECH_COLS].resample('W').last()

weekly = weekly.sort_index()
weekly = weekly[weekly.index <= pd.Timestamp.today()]

# 重新計算週線技術指標（用週收盤價）
if 'TWII' in weekly.columns:
    p = weekly['TWII']
    weekly['W_MA5']  = p.rolling(5).mean()   # 5週均線
    weekly['W_MA13'] = p.rolling(13).mean()  # 季線
    weekly['W_MA26'] = p.rolling(26).mean()  # 半年線
    delta = p.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, float('nan'))
    weekly['W_RSI14']       = 100 - (100/(1+rs))
    ema12 = p.ewm(span=12,adjust=False).mean()
    ema26 = p.ewm(span=26,adjust=False).mean()
    weekly['W_MACD']        = ema12 - ema26
    weekly['W_MACD_signal'] = weekly['W_MACD'].ewm(span=9,adjust=False).mean()
    weekly['W_MACD_hist']   = weekly['W_MACD'] - weekly['W_MACD_signal']

# 目標變數：未來 4 週報酬（約 20 個交易日）
if 'ETF0050' in weekly.columns:
    weekly['target_4w_return'] = weekly['ETF0050'].pct_change(4).shift(-4)
    weekly['target_signal']    = (weekly['target_4w_return'] > 0).astype(int)
    print(f"✅ 目標變數：未來4週報酬")

# 前向填補（最多2週）
weekly = weekly.ffill(limit=2)

print(f"週資料：{len(weekly)} 週 × {weekly.shape[1]} 欄")
print(f"日期：{weekly.index[0].date()} ～ {weekly.index[-1].date()}")

# ════════════════════════════════════════════════════
# 2. 因子定義
# ════════════════════════════════════════════════════
CORE_FEATURES = [
    'SOX','US10Y','TWII','DXY','USDTWD',
    'foreign_net_bil','invest_net_bil',
    'top5_net','top10_net',
]
SECONDARY_FEATURES = [
    'W_RSI14','W_MACD','W_MACD_hist','W_MA5','W_MA13',
]
CORE_FEATURES      = [f for f in CORE_FEATURES      if f in weekly.columns]
SECONDARY_FEATURES = [f for f in SECONDARY_FEATURES if f in weekly.columns]
ALL_FEATURES       = CORE_FEATURES + SECONDARY_FEATURES

print(f"\n核心因子（{len(CORE_FEATURES)}）：{CORE_FEATURES}")
print(f"次要因子（{len(SECONDARY_FEATURES)}）：{SECONDARY_FEATURES}")

# ════════════════════════════════════════════════════
# 3. 特徵工程
# ════════════════════════════════════════════════════
print("\n【特徵工程】")
feat_df = weekly[ALL_FEATURES].copy()

PRICE_COLS2 = ['SOX','TWII','DXY','USDTWD','US10Y','foreign_net_bil','invest_net_bil']
PRICE_COLS2 = [c for c in PRICE_COLS2 if c in feat_df.columns]

# 週報酬率
for col in PRICE_COLS2:
    feat_df[f'{col}_ret1'] = feat_df[col].pct_change(1)
    feat_df[f'{col}_ret4'] = feat_df[col].pct_change(4)  # 4週變化

# 滯後特徵（t-1, t-2, t-4 週）
LAG_COLS = [f'{c}_ret1' for c in PRICE_COLS2 if f'{c}_ret1' in feat_df.columns]
LAG_COLS += [c for c in SECONDARY_FEATURES if c in feat_df.columns]
for col in LAG_COLS:
    for lag in [1, 2, 4]:
        feat_df[f'{col}_lag{lag}'] = feat_df[col].shift(lag)

# 移除原始價格欄
feat_df = feat_df.drop(columns=[c for c in PRICE_COLS2 if c in feat_df.columns], errors='ignore')

# 清理 Infinity 和極端值
feat_df = feat_df.replace([float('inf'),float('-inf')],float('nan'))
for col in feat_df.columns:
    try:
        feat_df[col] = feat_df[col].clip(
            lower=feat_df[col].quantile(0.001),
            upper=feat_df[col].quantile(0.999))
    except: pass

# 法人籌碼缺值填 0，市場指標 dropna
MUST_HAVE = [c for c in feat_df.columns
             if any(k in c for k in ['SOX','TWII','DXY','USDTWD','US10Y',
                                      'NASDAQ','VIX','RSI','MACD','MA'])]
FILL_ZERO = [c for c in feat_df.columns if c not in MUST_HAVE]
feat_df[FILL_ZERO] = feat_df[FILL_ZERO].fillna(0)

target   = weekly['target_signal'].copy()
combined = feat_df.join(target)
combined = combined.dropna(subset=MUST_HAVE+['target_signal'])
feat_df  = combined.drop(columns=['target_signal'])
target   = combined['target_signal']

# 移除高共線性
corr = feat_df.corr().abs()
cols = corr.columns
drop_cols = set()
for i in range(len(cols)):
    for j in range(i+1,len(cols)):
        if corr.iloc[i,j] > 0.9:
            drop_cols.add(cols[j])
feat_df = feat_df.drop(columns=list(drop_cols), errors='ignore')
feature_cols = feat_df.columns.tolist()

print(f"  特徵數量（含滯後）：{len(feature_cols)}")
print(f"  有效樣本數：{len(feat_df)} 週")
print(f"  填 0 欄位：{len(FILL_ZERO)} 個")

# 正規化
scaler = StandardScaler()
X = pd.DataFrame(scaler.fit_transform(feat_df),
                 index=feat_df.index, columns=feature_cols)
y = target.loc[X.index]

# 訓練 / 驗證 / 測試
n      = len(X)
tr_end = int(n*0.60)
va_end = int(n*0.80)
X_train,y_train = X.iloc[:tr_end],       y.iloc[:tr_end]
X_val,  y_val   = X.iloc[tr_end:va_end], y.iloc[tr_end:va_end]
X_test, y_test  = X.iloc[va_end:],       y.iloc[va_end:]

print(f"\n訓練集：{len(X_train)} 週 ({X_train.index[0].date()} ～ {X_train.index[-1].date()})")
print(f"驗證集：{len(X_val)}   週 ({X_val.index[0].date()} ～ {X_val.index[-1].date()})")
print(f"測試集：{len(X_test)}  週 ({X_test.index[0].date()} ～ {X_test.index[-1].date()})")

# ════════════════════════════════════════════════════
# 4. 模型訓練
# ════════════════════════════════════════════════════
print("\n【模型訓練】")

# 修復 CV=NaN：使用 gap 避免資料洩漏，並確保每折有兩種標籤
def safe_cv_score(model, X, y, n_splits=5):
    """安全的時間序列交叉驗證，自動跳過只有單一標籤的折疊"""
    tscv = TimeSeriesSplit(n_splits=n_splits, gap=4)  # gap=4週避免未來洩漏
    scores = []
    for tr_idx, val_idx in tscv.split(X):
        X_tr, y_tr = X.iloc[tr_idx], y.iloc[tr_idx]
        X_vl, y_vl = X.iloc[val_idx], y.iloc[val_idx]
        if y_tr.nunique() < 2 or len(X_tr) < 52:
            continue
        try:
            m = model.__class__(**model.get_params())
            m.fit(X_tr, y_tr)
            scores.append(accuracy_score(y_vl, m.predict(X_vl)))
        except:
            continue
    return np.mean(scores) if scores else float('nan')

models = {
    'LogisticRegression': LogisticRegression(
        C=0.05, max_iter=2000, random_state=42,
        class_weight='balanced'),          # balanced 解決標籤不平衡
    'RandomForest':       RandomForestClassifier(
        n_estimators=200, max_depth=5,
        min_samples_leaf=10,               # 防過擬合
        random_state=42),
    'GradientBoosting':   GradientBoostingClassifier(
        n_estimators=200, max_depth=3,
        learning_rate=0.05, subsample=0.8,
        random_state=42),
}
results = {}
for name, model in models.items():
    cv_mean = safe_cv_score(model, X_train, y_train)
    model.fit(X_train, y_train)
    val_acc  = accuracy_score(y_val,  model.predict(X_val))
    test_acc = accuracy_score(y_test, model.predict(X_test))
    results[name] = {'cv':cv_mean,'val':val_acc,'test':test_acc,'model':model}
    cv_str = f"{cv_mean:.3f}" if not np.isnan(cv_mean) else "N/A"
    print(f"  {name}: CV={cv_str} Val={val_acc:.3f} Test={test_acc:.3f}")

best_name  = max(results, key=lambda k: results[k]['val'])
best_model = results[best_name]['model']
print(f"\n✅ 最佳模型：{best_name}（驗證集 {results[best_name]['val']:.3f}）")

# ════════════════════════════════════════════════════
# 5. 因子重要性
# ════════════════════════════════════════════════════
if hasattr(best_model,'feature_importances_'):
    imp = best_model.feature_importances_
elif hasattr(best_model,'coef_'):
    imp = np.abs(best_model.coef_[0])
else:
    imp = np.ones(len(feature_cols))

imp_df = pd.DataFrame({'feature':feature_cols,'importance':imp})\
           .sort_values('importance',ascending=False)
imp_df['is_core'] = imp_df['feature'].apply(
    lambda f: any(c in f for c in CORE_FEATURES))

print("\n  Top 15 重要因子：")
for _, row in imp_df.head(15).iterrows():
    tag = "★核心" if row['is_core'] else "  次要"
    bar = "█" * int(row['importance']/imp_df['importance'].max()*20)
    print(f"  {tag}  {row['feature']:35s} {bar} {row['importance']:.4f}")

# ════════════════════════════════════════════════════
# 6. 滾動回測（週頻）
# ════════════════════════════════════════════════════
print("\n【滾動式回測（週頻）】")

def rolling_backtest_weekly(X, y, price_series, window=156, step=4):
    """
    週頻滾動回測
    window：訓練窗口（156週 ≈ 3年）
    step：前進步長（4週 ≈ 1個月）
    持有：訊號多方且機率>55%時持有，否則空手
    """
    returns = []; dates = []
    prices  = price_series.reindex(X.index).ffill()

    for start in range(0, len(X)-window-step, step):
        end  = start + window
        X_tr = X.iloc[start:end]; y_tr = y.iloc[start:end]
        X_fw = X.iloc[end:end+step]; p_fw = prices.iloc[end:end+step]

        if len(X_tr) < 52 or len(X_fw) == 0 or y_tr.nunique() < 2:
            continue

        m = GradientBoostingClassifier(
            n_estimators=100,max_depth=3,
            learning_rate=0.05,random_state=42,subsample=0.8)
        try:
            m.fit(X_tr,y_tr)
            preds = m.predict(X_fw)
            proba = m.predict_proba(X_fw)
            bull_col = list(m.classes_).index(1) if 1 in m.classes_ else 0
        except: continue

        for i in range(len(X_fw)-1):
            if i >= len(p_fw)-1: break
            # 4週持有報酬
            raw_ret = (p_fw.iloc[i+1] - p_fw.iloc[i]) / p_fw.iloc[i]
            bp = proba[i][bull_col] if len(proba)>i else 0.5
            sig = 1 if (preds[i]==1 and bp>0.55) else 0
            returns.append(raw_ret*sig)
            dates.append(X_fw.index[i])

    return pd.Series(returns, index=dates)

price_col = 'ETF0050' if 'ETF0050' in weekly.columns else 'TWII'
bt_returns = rolling_backtest_weekly(X, y, weekly[price_col])
bh_returns = weekly[price_col].pct_change().reindex(bt_returns.index).dropna()

def calc_metrics(returns):
    if len(returns)==0: return {}
    equity    = (1+returns).cumprod()
    total_ret = equity.iloc[-1]-1
    n_years   = len(returns)/52  # 週頻用52
    cagr      = (1+total_ret)**(1/max(n_years,0.1))-1
    peak      = equity.cummax()
    max_dd    = ((equity-peak)/peak).min()
    ann_ret   = returns.mean()*52
    ann_std   = returns.std()*np.sqrt(52)
    sharpe    = ann_ret/ann_std if ann_std>0 else 0
    win_rate  = (returns>0).sum()/max((returns!=0).sum(),1)
    return {
        'cagr':     round(float(cagr*100),2),
        'max_dd':   round(float(max_dd*100),2),
        'sharpe':   round(float(sharpe),3),
        'win_rate': round(float(win_rate*100),2),
        'total_ret':round(float(total_ret*100),2),
        'n_trades': int((returns!=0).sum()),
    }

bt_m = calc_metrics(bt_returns)
bh_m = calc_metrics(bh_returns)

print(f"\n  {'指標':15s} {'策略':>12s} {'買持0050':>12s}")
print(f"  {'-'*40}")
print(f"  {'年化報酬(CAGR)':15s} {bt_m.get('cagr',0):>11.2f}% {bh_m.get('cagr',0):>11.2f}%")
print(f"  {'最大回撤':15s} {bt_m.get('max_dd',0):>11.2f}% {bh_m.get('max_dd',0):>11.2f}%")
print(f"  {'夏普值':15s} {bt_m.get('sharpe',0):>12.3f} {bh_m.get('sharpe',0):>12.3f}")
print(f"  {'勝率':15s} {bt_m.get('win_rate',0):>11.2f}% {bh_m.get('win_rate',0):>11.2f}%")

# ════════════════════════════════════════════════════
# 7. 當前預測
# ════════════════════════════════════════════════════
# 7. 當前市場狀態 + 訊號強度分級
# ════════════════════════════════════════════════════
print("\n【當前市場狀態】")
latest_X  = X.iloc[[-1]]
pred      = int(best_model.predict(latest_X)[0])
proba     = best_model.predict_proba(latest_X)[0]
bull_prob = float(proba[list(best_model.classes_).index(1)]) if 1 in best_model.classes_ else 0.5

# 訊號強度五級分類
def get_signal_level(bull_prob):
    if bull_prob >= 0.75:
        return '強多', '🟢🟢', '多方訊號強烈，可考慮積極加碼'
    elif bull_prob >= 0.60:
        return '弱多', '🟢', '多方訊號，可小幅加碼或維持持倉'
    elif bull_prob >= 0.45:
        return '中性', '⚪', '訊號不明確，維持標準倉位觀望'
    elif bull_prob >= 0.30:
        return '弱空', '🔴', '空方訊號，可考慮小幅減碼'
    else:
        return '強空', '🔴🔴', '空方訊號強烈，建議大幅減碼或空手'

signal_level, signal_emoji, signal_advice = get_signal_level(bull_prob)

print(f"  預測訊號：{signal_emoji} {signal_level}（多方機率 {bull_prob*100:.1f}%）")
print(f"  操作建議：{signal_advice}")
print(f"  預測週：{X.index[-1].date()}")
print(f"  目標週期：未來 4 週（約 20 個交易日）")

# ── 訊號記錄（每週自動累積）──────────────────────────
import csv
log_path = 'data/signal_log.csv'
log_exists = os.path.exists(log_path)
signal_date = str(X.index[-1].date())

# 讀取現有記錄，避免同一週重複記錄
existing_dates = set()
if log_exists:
    with open(log_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_dates.add(row.get('signal_date',''))

if signal_date not in existing_dates:
    with open(log_path, 'a', newline='', encoding='utf-8') as f:
        fieldnames = ['signal_date','recorded_at','model','bull_prob','signal_level',
                      'signal_label','advice','cagr','sharpe','max_dd',
                      'actual_4w_return','correct']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not log_exists:
            writer.writeheader()
        writer.writerow({
            'signal_date':   signal_date,
            'recorded_at':   datetime.now().strftime('%Y/%m/%d %H:%M'),
            'model':         best_name,
            'bull_prob':     round(bull_prob*100,1),
            'signal_level':  signal_level,
            'signal_label':  '多方' if pred==1 else '空方',
            'advice':        signal_advice,
            'cagr':          bt_m.get('cagr',0),
            'sharpe':        bt_m.get('sharpe',0),
            'max_dd':        bt_m.get('max_dd',0),
            'actual_4w_return': '',   # 4週後填入實際報酬
            'correct':          '',   # 4週後填入是否正確
        })
    print(f"\n✅ 訊號已記錄至 data/signal_log.csv（{signal_date}）")
else:
    print(f"\n  訊號本週已記錄過（{signal_date}），略過")

# ════════════════════════════════════════════════════
# 8. 輸出 model_output.json
# ════════════════════════════════════════════════════
equity_curve = (1+bt_returns).cumprod()
if len(equity_curve) > 104:
    equity_curve = equity_curve.iloc[-104:]  # 最近2年週資料
bh_curve = (1+bh_returns.reindex(equity_curve.index)).cumprod()

curve_data = []
for d,v in equity_curve.items():
    bh_v = float(bh_curve.get(d, float('nan')))
    curve_data.append({
        'date':     str(d.date()),
        'strategy': round(float(v),4),
        'buyhold':  round(bh_v,4) if not np.isnan(bh_v) else None,
    })

top_factors = []
for _,row in imp_df.head(20).iterrows():
    top_factors.append({
        'name':       row['feature'],
        'importance': round(float(row['importance']),5),
        'is_core':    bool(row['is_core']),
        'pct':        round(float(row['importance']/imp_df['importance'].sum()*100),1),
    })

model_comparison = []
for name,res in results.items():
    model_comparison.append({
        'name':     name,
        'cv_acc':   round(res['cv']*100,1),
        'val_acc':  round(res['val']*100,1),
        'test_acc': round(res['test']*100,1),
        'is_best':  name==best_name,
    })

def clean(obj):
    if isinstance(obj,dict): return {k:clean(v) for k,v in obj.items()}
    elif isinstance(obj,list): return [clean(v) for v in obj]
    elif isinstance(obj,float):
        if math.isnan(obj) or math.isinf(obj): return None
        return round(obj,6)
    return obj

output = clean({
    'updated_at':  datetime.now().strftime('%Y/%m/%d %H:%M'),
    'model_name':  best_name,
    'freq':        'weekly',
    'target':      '未來4週報酬',
    'data_range':  {
        'start': str(X.index[0].date()),
        'end':   str(X.index[-1].date()),
        'days':  len(X),
    },
    'split':       {'train':len(X_train),'val':len(X_val),'test':len(X_test)},
    'current_signal': {
        'signal':        pred,
        'signal_label':  '多方' if pred==1 else '空方',
        'signal_level':  signal_level,
        'signal_emoji':  signal_emoji,
        'signal_advice': signal_advice,
        'bull_prob':     round(bull_prob*100,1),
        'bear_prob':     round((1-bull_prob)*100,1),
        'date':          str(X.index[-1].date()),
        'target_desc':   '未來4週（約20個交易日）方向預測',
    },
    'backtest':    {'strategy':bt_m,'buyhold':bh_m},
    'factor_importance': top_factors,
    'equity_curve':      curve_data,
    'model_comparison':  model_comparison,
    'feature_count': {
        'core':      len(CORE_FEATURES),
        'secondary': len(SECONDARY_FEATURES),
        'total':     len(feature_cols),
    },
})

os.makedirs('data', exist_ok=True)
with open('data/model_output.json','w',encoding='utf-8') as f:
    json.dump(output,f,ensure_ascii=False,indent=2)

print(f"\n✅ 已儲存至 data/model_output.json")
print(f"\n{'='*50}")
print(f"完成！摘要：")
print(f"  頻率：週頻，目標：未來4週報酬")
print(f"  最佳模型：{best_name}")
print(f"  當前訊號：{'🟢 多方' if pred==1 else '🔴 空方'}（多方機率 {bull_prob*100:.1f}%）")
print(f"  策略CAGR：{bt_m.get('cagr',0):.2f}%  夏普：{bt_m.get('sharpe',0):.3f}  最大回撤：{bt_m.get('max_dd',0):.2f}%")
print(f"{'='*50}")
