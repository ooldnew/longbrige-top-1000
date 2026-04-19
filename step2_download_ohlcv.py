import os
import time
import pandas as pd
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext

# ===================== 【极速版】加速核心 =====================
LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

YEARS = [2021,2022,2023,2024,2025]
BASE = "us_1000_turnover"
DELAY = 0.05          # 🔥 从 0.3 → 0.05（超快）

config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
quote_ctx = QuoteContext(config)
os.makedirs(BASE, exist_ok=True)

# ===================== 下载 =====================
def dl(symbol, year):
    try:
        klines = quote_ctx.candlesticks(symbol, "day", f"{year}-01-01", f"{year}-12-31", adjust="forward")
        rows = [{"date":k.timestamp.strftime("%Y-%m-%d"), "open":k.open, "high":k.high, "low":k.low, "close":k.close, "volume":k.volume, "turnover":k.turnover} for k in klines]
        return pd.DataFrame(rows)
    except:
        return None

# ===================== 每年下载前1000只 =====================
df_all = pd.read_csv("top1000_by_year.csv")

for year in YEARS:
    folder = os.path.join(BASE, str(year))
    os.makedirs(folder, exist_ok=True)
    df = df_all[df_all.year==year].head(1000)

    print(f"\n===== {year} 前1000只 =====")
    for _, row in tqdm(df.iterrows(), total=len(df)):
        sym = row["symbol"]
        path = os.path.join(folder, f"{sym}.csv")
        if os.path.exists(path):
            continue
        df_k = dl(sym, year)
        if df_k is not None and not df_k.empty:
            df_k.to_csv(path, index=False)
        
        # 🔥 极速延迟
        time.sleep(DELAY)

print("✅ 全部下载完成！")
