"""
台股多因子模型 - 模型建構、回測、參數優化
在本機執行：python build_model.py
輸入：data/raw_data.csv
輸出：data/model_output.json（供網頁展示用）
"""

import subprocess, sys

def install(pkg):
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg, '-q'])

print("安裝必要套件...")
for pkg in ['pandas', 'numpy', 'scikit-learn', 'scipy']:
    install(pkg)

import os, json, warnings
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import accuracy_score, classification_report
from sklearn.inspection import permutation_importance
warnings.filterwarnings('ignore')

print("\n載入資料...")
df = pd.read_csv('data/raw_data.csv', index_col='date', parse_dates=True)
print(f"資料：{df.shape[0]} 列 × {df.shape[1]} 欄")
print(f"日期：{df.index[0].date()} ～ {df.index[-1].date()}")

# ════════════════════════════════════════════════════
# 1. 因子定義與權重分類
# ════════════════════════════════════════════════════
# 核心因子（高權重）
CORE_FEATURES = [
    'SOX',                  # 費城半導體
    'US10Y',                # 美國10年公債殖利率
    'TWII',                 # 台灣加權指數
    'DXY',                  # 美元指數
    'USDTWD',               # 台幣匯率
    'foreign_net_bil',      # 外資買賣超
    'invest_net_bil',       # 投信買賣超
    'futures_foreign_net',  # 外資期貨未平倉
]

# 次要因子（低權重）
SECONDARY_FEATURES = [
    'RSI14',
    'MACD',
    'MACD_hist',
    'MA5',
    'MA20',
]

# 只保留實際存在的欄位
CORE_FEATURES      = [f for f in CORE_FEATURES      if f in df.columns]
SECONDARY_FEATURES = [f for f in SECONDARY_FEATURES if f in df.columns]
ALL_FEATURES       = CORE_FEATURES + SECONDARY_FEATURES

print(f"\n核心因子（{len(CORE_FEATURES)}）：{CORE_FEATURES}")
print(f"次要因子（{len(SECONDARY_FEATURES)}）：{SECONDARY_FEATURES}")

# ════════════════════════════════════════════════════
# 2. 特徵工程
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【2】特徵工程")
print("=" * 50)

feat_df = df[ALL_FEATURES].copy()

# 2a. 轉換為報酬率（價格類因子）
PRICE_COLS = ['SOX', 'TWII', 'DXY', 'USDTWD', 'US10Y',
              'foreign_net_bil', 'invest_net_bil', 'futures_foreign_net']
PRICE_COLS = [c for c in PRICE_COLS if c in feat_df.columns]

for col in PRICE_COLS:
    feat_df[f'{col}_ret1'] = feat_df[col].pct_change(1)
    feat_df[f'{col}_ret5'] = feat_df[col].pct_change(5)

# 2b. 滯後特徵（t-1, t-3, t-5）
LAG_COLS = [f'{c}_ret1' for c in PRICE_COLS if f'{c}_ret1' in feat_df.columns]
LAG_COLS += [c for c in SECONDARY_FEATURES if c in feat_df.columns]

for col in LAG_COLS:
    for lag in [1, 3, 5]:
        feat_df[f'{col}_lag{lag}'] = feat_df[col].shift(lag)

# 2c. 移除原始價格欄（保留報酬率和滯後）
drop_raw = [c for c in PRICE_COLS if c in feat_df.columns]
feat_df  = feat_df.drop(columns=drop_raw, errors='ignore')

# 清理 Infinity 和極端值
feat_df = feat_df.replace([float('inf'), float('-inf')], float('nan'))
for col in feat_df.columns:
    try:
        q_low  = feat_df[col].quantile(0.001)
        q_high = feat_df[col].quantile(0.999)
        feat_df[col] = feat_df[col].clip(lower=q_low, upper=q_high)
    except:
        pass

# 2d. 缺值處理策略：
#   - 核心市場因子（SOX、TWII、DXY 等）：dropna，這些缺值代表真正沒有交易日
#   - 法人籌碼因子（三大法人、融資等）：填 0（代表無資料，中性訊號）
#     這樣 2004～2011 的樣本不會被丟棄，模型會學到「0 = 無此訊號」
MUST_HAVE = [c for c in feat_df.columns
             if any(k in c for k in ['SOX','TWII','DXY','USDTWD','US10Y',
                                      'NASDAQ','VIX','RSI','MACD','MA5'])]
FILL_ZERO = [c for c in feat_df.columns if c not in MUST_HAVE]

# 法人籌碼相關欄位填 0
feat_df[FILL_ZERO] = feat_df[FILL_ZERO].fillna(0)

# 2e. 移除缺值（只針對必要欄位）
target   = df['target_signal'].copy()
combined = feat_df.join(target)
combined = combined.dropna(subset=MUST_HAVE + ['target_signal'])
feat_df  = combined.drop(columns=['target_signal'])
target   = combined['target_signal']

feature_cols = feat_df.columns.tolist()
print(f"  特徵數量（含滯後）：{len(feature_cols)}")
print(f"  有效樣本數：{len(feat_df)}")
print(f"  填 0 處理欄位：{len(FILL_ZERO)} 個（法人籌碼類）")
print(f"  日期範圍：{feat_df.index[0].date()} ～ {feat_df.index[-1].date()}")

# 2e. 共線性偵測（VIF 簡化版）
print("\n  共線性檢測（相關係數 > 0.9 的配對）：")
corr_matrix = feat_df.corr().abs()
high_corr   = []
cols        = corr_matrix.columns
for i in range(len(cols)):
    for j in range(i+1, len(cols)):
        if corr_matrix.iloc[i, j] > 0.9:
            high_corr.append((cols[i], cols[j], corr_matrix.iloc[i, j]))
if high_corr:
    for c1, c2, v in high_corr[:10]:
        print(f"    {c1} ↔ {c2}：{v:.3f}")
    # 移除高共線性欄位（保留第一個）
    drop_cols = list(set([pair[1] for pair in high_corr]))
    feat_df   = feat_df.drop(columns=drop_cols, errors='ignore')
    feature_cols = feat_df.columns.tolist()
    print(f"  移除 {len(drop_cols)} 個高共線性特徵，剩餘 {len(feature_cols)} 個")
else:
    print("  ✅ 無嚴重共線性問題")

# 2f. 正規化
scaler  = StandardScaler()
X       = pd.DataFrame(scaler.fit_transform(feat_df), index=feat_df.index, columns=feature_cols)
y       = target.loc[X.index]

# ════════════════════════════════════════════════════
# 3. 訓練 / 驗證 / 測試集切分
# ════════════════════════════════════════════════════
n       = len(X)
tr_end  = int(n * 0.60)  # 60% 訓練
va_end  = int(n * 0.80)  # 20% 驗證
# 測試集：最後 20%

X_train, y_train = X.iloc[:tr_end],       y.iloc[:tr_end]
X_val,   y_val   = X.iloc[tr_end:va_end], y.iloc[tr_end:va_end]
X_test,  y_test  = X.iloc[va_end:],       y.iloc[va_end:]

print(f"\n訓練集：{len(X_train)} 筆 ({X_train.index[0].date()} ～ {X_train.index[-1].date()})")
print(f"驗證集：{len(X_val)}   筆 ({X_val.index[0].date()} ～ {X_val.index[-1].date()})")
print(f"測試集：{len(X_test)}  筆 ({X_test.index[0].date()} ～ {X_test.index[-1].date()})")

# ════════════════════════════════════════════════════
# 4. 模型訓練（多模型比較）
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【4】模型訓練")
print("=" * 50)

tscv = TimeSeriesSplit(n_splits=5)

models = {
    'LogisticRegression': LogisticRegression(C=0.1, max_iter=1000, random_state=42),
    'RandomForest':       RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42),
    'GradientBoosting':   GradientBoostingClassifier(n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42),
}

results = {}
for name, model in models.items():
    cv_scores = cross_val_score(model, X_train, y_train, cv=tscv, scoring='accuracy')
    model.fit(X_train, y_train)
    val_acc  = accuracy_score(y_val,  model.predict(X_val))
    test_acc = accuracy_score(y_test, model.predict(X_test))
    results[name] = {
        'cv_mean':  cv_scores.mean(),
        'cv_std':   cv_scores.std(),
        'val_acc':  val_acc,
        'test_acc': test_acc,
        'model':    model,
    }
    print(f"\n  {name}")
    print(f"    CV 準確率：{cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"    驗證集：  {val_acc:.3f}")
    print(f"    測試集：  {test_acc:.3f}")

# 選最佳模型（以驗證集準確率為準）
best_name  = max(results, key=lambda k: results[k]['val_acc'])
best_model = results[best_name]['model']
print(f"\n✅ 最佳模型：{best_name}（驗證集準確率 {results[best_name]['val_acc']:.3f}）")

# ════════════════════════════════════════════════════
# 5. 因子重要性
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【5】因子重要性")
print("=" * 50)

if hasattr(best_model, 'feature_importances_'):
    importances = best_model.feature_importances_
elif hasattr(best_model, 'coef_'):
    importances = np.abs(best_model.coef_[0])
else:
    importances = np.ones(len(feature_cols))

imp_df = pd.DataFrame({
    'feature':    feature_cols,
    'importance': importances,
}).sort_values('importance', ascending=False)

# 標記是否為核心因子
imp_df['is_core'] = imp_df['feature'].apply(
    lambda f: any(c in f for c in CORE_FEATURES)
)

print("\n  Top 15 重要因子：")
for _, row in imp_df.head(15).iterrows():
    tag = "★核心" if row['is_core'] else "  次要"
    bar = "█" * int(row['importance'] / imp_df['importance'].max() * 20)
    print(f"  {tag}  {row['feature']:35s} {bar} {row['importance']:.4f}")

# ════════════════════════════════════════════════════
# 6. 回測框架
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【6】滾動式回測")
print("=" * 50)

def rolling_backtest(X, y, price_series, window=504, step=21):
    """
    滾動式回測
    window：訓練窗口（504 = 約2年交易日，比原本1年更穩定）
    step：每次前進天數（約1個月）
    策略：多方訊號持有，空方訊號空手（不做空）
    """
    returns   = []
    dates     = []
    prices    = price_series.reindex(X.index).ffill()

    for start in range(0, len(X) - window - step, step):
        end  = start + window
        X_tr = X.iloc[start:end]
        y_tr = y.iloc[start:end]
        X_fw = X.iloc[end:end+step]
        p_fw = prices.iloc[end:end+step]

        if len(X_tr) < 120 or len(X_fw) == 0:
            continue

        # 確保訓練集有兩種標籤
        if y_tr.nunique() < 2:
            continue

        m = GradientBoostingClassifier(
            n_estimators=100, max_depth=3,
            learning_rate=0.05, random_state=42,
            subsample=0.8
        )
        try:
            m.fit(X_tr, y_tr)
            preds = m.predict(X_fw)
            proba = m.predict_proba(X_fw)
            bull_col = list(m.classes_).index(1) if 1 in m.classes_ else 0
        except:
            continue

        for i in range(len(X_fw) - 1):
            if i >= len(p_fw) - 1:
                break
            raw_ret = (p_fw.iloc[i+1] - p_fw.iloc[i]) / p_fw.iloc[i]
            # 只有多方訊號且機率 > 55% 才持有，否則空手
            bull_prob = proba[i][bull_col] if len(proba) > i else 0.5
            sig = 1 if (preds[i] == 1 and bull_prob > 0.55) else 0
            returns.append(raw_ret * sig)
            dates.append(X_fw.index[i])

    return pd.Series(returns, index=dates), pd.Series(
        [1 if r != 0 else 0 for r in returns], index=dates
    )

# 使用 0050 價格做回測
price_col = 'ETF0050' if 'ETF0050' in df.columns else 'TWII'
bt_returns, bt_signals = rolling_backtest(X, y, df[price_col])

# 評估指標
def calc_metrics(returns):
    if len(returns) == 0:
        return {}
    equity    = (1 + returns).cumprod()
    total_ret = equity.iloc[-1] - 1
    n_years   = len(returns) / 252
    cagr      = (1 + total_ret) ** (1 / max(n_years, 0.1)) - 1

    # 最大回撤
    peak       = equity.cummax()
    drawdown   = (equity - peak) / peak
    max_dd     = drawdown.min()

    # 夏普值
    ann_ret    = returns.mean() * 252
    ann_std    = returns.std() * np.sqrt(252)
    sharpe     = ann_ret / ann_std if ann_std > 0 else 0

    # 勝率（正報酬天數）
    win_rate   = (returns > 0).sum() / max((returns != 0).sum(), 1)

    return {
        'cagr':      round(float(cagr * 100), 2),
        'max_dd':    round(float(max_dd * 100), 2),
        'sharpe':    round(float(sharpe), 3),
        'win_rate':  round(float(win_rate * 100), 2),
        'total_ret': round(float(total_ret * 100), 2),
        'n_trades':  int((returns != 0).sum()),
    }

bt_metrics = calc_metrics(bt_returns)

# Benchmark：Buy & Hold 0050
bh_returns = df[price_col].pct_change().reindex(bt_returns.index).dropna()
bh_metrics = calc_metrics(bh_returns)

print(f"\n  {'指標':15s} {'策略':>12s} {'買持0050':>12s}")
print(f"  {'-'*40}")
print(f"  {'年化報酬(CAGR)':15s} {bt_metrics.get('cagr',0):>11.2f}% {bh_metrics.get('cagr',0):>11.2f}%")
print(f"  {'最大回撤':15s} {bt_metrics.get('max_dd',0):>11.2f}% {bh_metrics.get('max_dd',0):>11.2f}%")
print(f"  {'夏普值':15s} {bt_metrics.get('sharpe',0):>12.3f} {bh_metrics.get('sharpe',0):>12.3f}")
print(f"  {'勝率':15s} {bt_metrics.get('win_rate',0):>11.2f}% {bh_metrics.get('win_rate',0):>11.2f}%")

# ════════════════════════════════════════════════════
# 7. 當前市場狀態預測
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【7】當前市場狀態")
print("=" * 50)

# 用最新資料預測
latest_X = X.iloc[[-1]]
pred_signal   = int(best_model.predict(latest_X)[0])
pred_proba    = best_model.predict_proba(latest_X)[0]
bull_prob     = float(pred_proba[1])
bear_prob     = float(pred_proba[0])

print(f"\n  預測訊號：{'多方 (看漲)' if pred_signal == 1 else '空方 (看跌)'}")
print(f"  多方機率：{bull_prob*100:.1f}%")
print(f"  空方機率：{bear_prob*100:.1f}%")
print(f"  預測日期：{X.index[-1].date()}")

# 最新各因子數值
latest_raw = df[CORE_FEATURES].iloc[-1].to_dict()
print(f"\n  最新核心因子數值：")
for k, v in latest_raw.items():
    if pd.notna(v):
        print(f"    {k:25s}：{v:.4f}")

# ════════════════════════════════════════════════════
# 8. 輸出 model_output.json
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("【8】輸出 model_output.json")
print("=" * 50)

# 因子重要性（前20）
top_factors = []
for _, row in imp_df.head(20).iterrows():
    top_factors.append({
        'name':       row['feature'],
        'importance': round(float(row['importance']), 5),
        'is_core':    bool(row['is_core']),
        'pct':        round(float(row['importance'] / imp_df['importance'].sum() * 100), 1),
    })

# 近期回測淨值曲線（最後250筆）
equity_curve = (1 + bt_returns).cumprod()
if len(equity_curve) > 250:
    equity_curve = equity_curve.iloc[-250:]
bh_curve = (1 + bh_returns.reindex(equity_curve.index)).cumprod()

curve_data = []
for d, v in equity_curve.items():
    bh_v = float(bh_curve.get(d, float('nan')))
    curve_data.append({
        'date':     str(d.date()),
        'strategy': round(float(v), 4),
        'buyhold':  round(bh_v, 4) if not np.isnan(bh_v) else None,
    })

# 模型比較
model_comparison = []
for name, res in results.items():
    model_comparison.append({
        'name':      name,
        'cv_acc':    round(res['cv_mean'] * 100, 1),
        'val_acc':   round(res['val_acc'] * 100, 1),
        'test_acc':  round(res['test_acc'] * 100, 1),
        'is_best':   name == best_name,
    })

output = {
    'updated_at':       datetime.now().strftime('%Y/%m/%d %H:%M'),
    'model_name':       best_name,
    'data_range': {
        'start': str(X.index[0].date()),
        'end':   str(X.index[-1].date()),
        'days':  len(X),
    },
    'split': {
        'train': len(X_train),
        'val':   len(X_val),
        'test':  len(X_test),
    },
    'current_signal': {
        'signal':       pred_signal,
        'signal_label': '多方' if pred_signal == 1 else '空方',
        'bull_prob':    round(bull_prob * 100, 1),
        'bear_prob':    round(bear_prob * 100, 1),
        'date':         str(X.index[-1].date()),
    },
    'backtest': {
        'strategy':  bt_metrics,
        'buyhold':   bh_metrics,
    },
    'factor_importance': top_factors,
    'equity_curve':      curve_data,
    'model_comparison':  model_comparison,
    'feature_count': {
        'core':      len(CORE_FEATURES),
        'secondary': len(SECONDARY_FEATURES),
        'total':     len(feature_cols),
    },
}

os.makedirs('data', exist_ok=True)
# 清理 NaN / Infinity（JSON 不支援這些值）
def clean_for_json(obj):
    import math
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return round(obj, 6)
    return obj

output = clean_for_json(output)

with open('data/model_output.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"\n✅ 已儲存至 data/model_output.json")
print(f"\n{'='*50}")
print(f"完成！摘要：")
print(f"  最佳模型：{best_name}")
print(f"  當前訊號：{'🟢 多方' if pred_signal == 1 else '🔴 空方'}（多方機率 {bull_prob*100:.1f}%）")
print(f"  策略CAGR：{bt_metrics.get('cagr',0):.2f}%  夏普：{bt_metrics.get('sharpe',0):.3f}  最大回撤：{bt_metrics.get('max_dd',0):.2f}%")
print(f"{'='*50}")
print(f"\n下一步：")
print(f"  1. 把 data/model_output.json 上傳到 GitHub repository")
print(f"  2. 更新網頁以顯示模型結果")
