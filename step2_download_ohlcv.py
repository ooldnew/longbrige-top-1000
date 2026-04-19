import os
import time
import pandas as pd
from tqdm import tqdm
from datetime import date
from longbridge.openapi import (
    QuoteContext,
    Period,
    AdjustType
)

# ===================== 长桥新版 SDK 官方用法 =====================
# 新版不再需要手动创建 Config！
# 环境变量必须是：
# LONGBRIDGE_APP_KEY
# LONGBRIDGE_APP_SECRET
# LONGBRIDGE_ACCESS_TOKEN
# =================================================================

YEARS = [2021,2022,2023,2024,2025]
BASE_DIR = "us_1000_turnover"
os.makedirs(BASE_DIR, exist_ok=True)
DELAY = 0.4

# 自动读取环境变量初始化
quote_ctx = QuoteContext()

df_symbols = pd.read_csv("top1000_by_year.csv")
failed_records = []

# ===================== 下载 K线（官方标准接口） =====================
def download_stock(symbol, year):
    try:
        start = date(year, 1, 1)
        end = date(year, 12, 31)

        # 官方最新接口
        resp = quote_ctx.history_candlesticks_by_date(
            symbol=symbol,
            period=Period.Day,
            adjust_type=AdjustType.Forward,
            start_date=start,
            end_date=end
        )

        rows = []
        for bar in resp.candlesticks:
            rows.append({
                "date": pd.to_datetime(bar.timestamp, unit='s').strftime("%Y-%m-%d"),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": bar.volume,
                "turnover": float(bar.turnover)
            })
        return pd.DataFrame(rows)

    except Exception as e:
        print(f"[失败] {symbol} | {str(e)}")
        return None

# ===================== 主逻辑 =====================
for year in YEARS:
    year_dir = os.path.join(BASE_DIR, str(year))
    os.makedirs(year_dir, exist_ok=True)

    df_year = df_symbols[df_symbols["year"] == year].head(1000)
    symbols = df_year["symbol"].tolist()

    print(f"\n===== {year} 年 前1000只 =====")

    for symbol in tqdm(symbols):
        csv_path = os.path.join(year_dir, f"{symbol}.csv")
        if os.path.exists(csv_path):
            continue

        df = download_stock(symbol, year)
        if df is not None and not df.empty:
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        else:
            failed_records.append([year, symbol])

        time.sleep(DELAY)

# 保存失败清单
pd.DataFrame(failed_records, columns=["year", "symbol"]).to_csv("failed_symbols.csv", index=False)
print(f"\n✅ 全部完成！文件保存在：{BASE_DIR}/")
