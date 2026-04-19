import pandas as pd
import time
import os
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext, Period, AdjustType

LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

YEARS = [2021, 2022, 2023, 2024, 2025]
TOP_N = 1000
DELAY = 0.05
CACHE_FILE = "step1_progress_cache.csv"
OUTPUT_CSV = "top1000_by_year.csv"

config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
ctx = QuoteContext(config)

def get_us_tickers():
    url = "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv"
    df = pd.read_csv(url)
    return df[df["symbol"].str.match(r"^[A-Z]{1,5}$", na=False)].symbol.unique()

def main():
    tickers = get_us_tickers()
    print(f"总股票数：{len(tickers)}")

    if not os.path.exists(CACHE_FILE):
        pd.DataFrame(columns=["year", "symbol", "turnover"]).to_csv(CACHE_FILE, index=False)

    try:
        df_cache = pd.read_csv(CACHE_FILE)
        done = set(df_cache["symbol"].str.replace(".US", "", regex=False).tolist())
    except Exception:
        done = set()

    todo = [t for t in tickers if t not in done]
    print(f"待获取：{len(todo)} 只，已完成：{len(done)} 只")

    error_count = 0
    success_count = 0

    for t in tqdm(todo):
        sym = f"{t}.US"
        try:
            # 旧版 longbridge <3.0.0 的正确调用方式
            klines = ctx.candlesticks(
                sym,
                Period.Day,
                1000,          # count，取足够多的条数
                AdjustType.NoAdjust
            )
            year_sum = {}
            for k in klines:
                y = k.timestamp.year
                if y in YEARS:
                    year_sum[y] = year_sum.get(y, 0.0) + float(k.turnover)

            if year_sum:
                rows = [{"year": y, "symbol": sym, "turnover": v} for y, v in year_sum.items()]
                pd.DataFrame(rows).to_csv(CACHE_FILE, mode="a", header=False, index=False)
                success_count += 1
            else:
                # 没有目标年份数据，写一个占位行避免重复请求
                pd.DataFrame([{"year": 0, "symbol": sym, "turnover": 0}]).to_csv(
                    CACHE_FILE, mode="a", header=False, index=False
                )
        except Exception as e:
            error_count += 1
            if error_count <= 20:  # 只打印前20个错误避免刷屏
                print(f"\n[ERROR] {sym}: {e}")

        time.sleep(DELAY)

    print(f"\n获取完成：成功 {success_count}，失败 {error_count}")

    # 生成 TOP1000
    df = pd.read_csv(CACHE_FILE)
    df = df[df["year"].isin(YEARS)]  # 过滤掉占位行
    final = []
    for y in YEARS:
        year_df = df[df["year"] == y].copy()
        year_df = year_df.sort_values("turnover", ascending=False).head(TOP_N)
        print(f"{y} 年：{len(year_df)} 只")
        final.append(year_df)

    final_df = pd.concat(final)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ 已生成：{OUTPUT_CSV}，共 {len(final_df)} 行")

if __name__ == "__main__":
    main()
