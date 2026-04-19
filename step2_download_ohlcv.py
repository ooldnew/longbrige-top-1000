import os
import time
import pandas as pd
from tqdm import tqdm
from datetime import date
from longbridge.openapi import (
    QuoteContext,
    Config,
    Period,
    AdjustType
)

# ===================== 长桥 v4.0.5 官方标准写法 =====================
# 从环境变量自动创建 config
config = Config.from_env()
quote_ctx = QuoteContext(config)

# 配置
YEARS = [2021, 2022, 2023, 2024, 2025]
BASE_DIR = "us_1000_turnover"
os.makedirs(BASE_DIR, exist_ok=True)
DELAY = 0.4

# 读取股票列表
df_symbols = pd.read_csv("top1000_by_year.csv")
failed_records = []

# ===================== 官方 K线 接口 =====================
def download(symbol, year):
    try:
        resp = quote_ctx.history_candlesticks_by_date(
            symbol=symbol,
            period=Period.Day,
            adjust_type=AdjustType.Forward,
            start_date=date(year, 1, 1),
            end_date=date(year, 12, 31)
        )

        rows = []
        for bar in resp.candlesticks:
            rows.append({
                "date": pd.to_datetime(bar.timestamp, unit="s").strftime("%Y-%m-%d"),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": bar.volume,
                "turnover": float(bar.turnover),
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"FAIL {symbol}: {str(e)}")
        return None

# ===================== 按年份下载 =====================
for year in YEARS:
    year_dir = os.path.join(BASE_DIR, str(year))
    os.makedirs(year_dir, exist_ok=True)

    df_year = df_symbols[df_symbols["year"] == year].head(1000)
    symbols = df_year["symbol"].tolist()

    print(f"\n=== {year} 年 前1000只 ===")

    for symbol in tqdm(symbols):
        csv_path = os.path.join(year_dir, f"{symbol}.csv")
        if os.path.exists(csv_path):
            continue

        df = download(symbol, year)
        if df is not None and not df.empty:
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        else:
            failed_records.append([year, symbol])

        time.sleep(DELAY)

pd.DataFrame(failed_records, columns=["year", "symbol"]).to_csv("failed_symbols.csv", index=False)
print(f"\n✅ 下载完成！文件在 {BASE_DIR}")
