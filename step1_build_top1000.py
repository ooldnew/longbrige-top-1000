import pandas as pd
import time
import os
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext

# ===================== 【极速版】加速核心 =====================
LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

YEARS = [2021, 2022, 2023, 2024, 2025]
TOP_N = 1000
DELAY = 0.05           # 🔥 从 0.2 → 0.05（超快）
CACHE_FILE = "step1_progress_cache.csv"
OUTPUT_CSV = "top1000_by_year.csv"

# ===================== 初始化 =====================
config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
ctx = QuoteContext(config)

# ===================== 获取股票列表 =====================
def get_us_tickers():
    url = "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv"
    df = pd.read_csv(url)
    return df[df["symbol"].str.match(r"^[A-Z]{1,5}$", na=False)].symbol.unique()

# ===================== 主程序 =====================
def main():
    tickers = get_us_tickers()
    done = set()

    if not os.path.exists(CACHE_FILE):
        pd.DataFrame(columns=["year", "symbol", "turnover"]).to_csv(CACHE_FILE, index=False)

    try:
        done = set(pd.read_csv(CACHE_FILE)["symbol"].str.replace(".US", "").tolist())
    except:
        done = set()

    todo = [t for t in tickers if t not in done]
    print(f"待获取：{len(todo)} 只")

    for t in tqdm(todo):
        sym = f"{t}.US"
        try:
            klines = ctx.candlesticks(sym, "day", "2021-01-01", "2025-12-31", adjust="none")
            year_sum = {}
            for k in klines:
                y = k.timestamp.year
                if y in YEARS:
                    year_sum[y] = year_sum.get(y, 0.0) + float(k.turnover)

            rows = [{"year": y, "symbol": sym, "turnover": v} for y, v in year_sum.items()]
            pd.DataFrame(rows).to_csv(CACHE_FILE, mode="a", header=False, index=False)
        except Exception as e:
            pass
        
        # 🔥 极速延迟（不触发限流，又超快）
        time.sleep(DELAY)

    # 生成每年 TOP1000
    df = pd.read_csv(CACHE_FILE)
    final = []
    for y in YEARS:
        year_data = df[df["year"] == y].copy()
        year_data = year_data.sort_values("turnover", ascending=False)
        top = year_data.head(TOP_N)
        final.append(top)

    pd.concat(final).to_csv(OUTPUT_CSV, index=False)
    print(f"✅ 生成完成：{OUTPUT_CSV}")

if __name__ == "__main__":
    main()
