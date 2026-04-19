import pandas as pd
import os
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext

LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

YEARS = [2021, 2022, 2023, 2024, 2025]
TOP_N = 1000
CACHE_FILE = "step1_progress_cache.csv"
OUTPUT_CSV = "top1000_by_year.csv"

config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
ctx = QuoteContext(config)

def get_us_tickers():
    url = "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv"
    df = pd.read_csv(url)
    return df[df["symbol"].str.match(r"^[A-Z]{1,5}$", na=False)].symbol.unique()

def get_year_turnover(sym, year):
    try:
        klines = ctx.history_candlesticks_by_date(
            sym, "day", f"{year}-01-01", f"{year}-12-31", adjust_type="no_adjust"
        )
        total = sum(float(k.turnover) for k in klines)
        return total if total > 0 else None
    except Exception:
        return None

def main():
    tickers = get_us_tickers()
    print(f"总股票数：{len(tickers)}")

    if not os.path.exists(CACHE_FILE):
        pd.DataFrame(columns=["year","symbol","turnover"]).to_csv(CACHE_FILE, index=False)

    try:
        df_cache = pd.read_csv(CACHE_FILE)
        done = set(df_cache["symbol"].str.replace(".US","",regex=False).tolist())
    except:
        done = set()

    todo = [t for t in tickers if t not in done]
    print(f"待获取：{len(todo)} 只，已完成：{len(done)} 只")

    success_count = 0
    error_count = 0

    for t in tqdm(todo):
        sym = f"{t}.US"
        rows = []
        for year in YEARS:
            v = get_year_turnover(sym, year)
            if v:
                rows.append({"year": year, "symbol": sym, "turnover": v})
            # 无 sleep，由长桥SDK自动限流

        if rows:
            pd.DataFrame(rows).to_csv(CACHE_FILE, mode="a", header=False, index=False)
            success_count += 1
        else:
            pd.DataFrame([{"year":0,"symbol":sym,"turnover":0}]).to_csv(
                CACHE_FILE, mode="a", header=False, index=False)
            error_count += 1
            if error_count <= 10:
                print(f"\n[NO DATA] {sym}")

    print(f"\n获取完成：有数据 {success_count}，无数据/失败 {error_count}")

    df = pd.read_csv(CACHE_FILE)
    df = df[df["year"].isin(YEARS)]
    final = []
    for y in YEARS:
        year_df = df[df["year"]==y].sort_values("turnover", ascending=False).head(TOP_N)
        print(f"{y} 年：{len(year_df)} 只")
        final.append(year_df)

    final_df = pd.concat(final)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ 已生成：{OUTPUT_CSV}，共 {len(final_df)} 行")

if __name__ == "__main__":
    main()
