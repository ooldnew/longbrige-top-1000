import os
import pandas as pd
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext, Period, AdjustType

LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
quote_ctx = QuoteContext(config)

YEARS = [2021, 2022, 2023, 2024, 2025]
BASE_DIR = "us_1000_turnover"
TOP_N = 1000

os.makedirs(BASE_DIR, exist_ok=True)

def download(symbol, year):
    try:
        klines = quote_ctx.candlesticks(symbol, Period.Day, 1000, AdjustType.ForwardAdjust)
        rows = [{"date": k.timestamp.strftime("%Y-%m-%d"),
                 "open": k.open, "high": k.high, "low": k.low,
                 "close": k.close, "volume": k.volume, "turnover": k.turnover}
                for k in klines if k.timestamp.year == year]
        return pd.DataFrame(rows) if rows else None
    except Exception:
        return None

df_all = pd.read_csv("top1000_by_year.csv")
print(f"读取到股票记录：{len(df_all)} 行")

total_ok = 0
total_fail = 0

for year in YEARS:
    df_year = df_all[df_all["year"] == year].head(TOP_N)
    print(f"\n===== {year} 年 共 {len(df_year)} 只 =====")

    year_folder = os.path.join(BASE_DIR, str(year))
    os.makedirs(year_folder, exist_ok=True)

    ok = 0
    fail = 0
    for _, row in tqdm(df_year.iterrows(), total=len(df_year)):
        sym = row["symbol"]
        csv_path = os.path.join(year_folder, f"{sym}.csv")

        if os.path.exists(csv_path):
            ok += 1
            continue

        df = download(sym, year)
        if df is not None and not df.empty:
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            ok += 1
        else:
            fail += 1

    print(f"{year} 年：成功 {ok}，失败/空 {fail}")
    total_ok += ok
    total_fail += fail

print(f"\n✅ 全部完成！总成功 {total_ok}，总失败 {total_fail}")
