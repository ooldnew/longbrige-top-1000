import os
import time
import pandas as pd
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext

# ===================== 长桥配置 =====================
LB_APP_KEY = os.getenv("LB_APP_KEY")
LB_APP_SECRET = os.getenv("LB_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LB_ACCESS_TOKEN")

# 下载年份
YEARS = [2021, 2022, 2023, 2024, 2025]

# 根目录
BASE_DIR = "us_1000_turnover"
os.makedirs(BASE_DIR, exist_ok=True)

# 限流
DELAY_SECONDS = 0.25
# ====================================================

# 初始化长桥
config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
quote_ctx = QuoteContext(config)

# 读取股票池
df_symbols = pd.read_csv("top1000_by_year.csv")

# 失败记录
failed_records = []

def download_one(symbol, year):
    try:
        start = f"{year}-01-01"
        end = f"{year}-12-31"

        resp = quote_ctx.candlesticks(
            symbol=symbol,
            period="day",
            start=start,
            end=end,
            adjust="forward"
        )

        rows = []
        for bar in resp:
            rows.append({
                "date": bar.timestamp.strftime("%Y-%m-%d"),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "turnover": bar.turnover,
            })

        return pd.DataFrame(rows)

    except Exception as e:
        print(f"[异常] {symbol} | {str(e)}")
        return None

# ===================== 按年份下载 =====================
for year in YEARS:
    year_dir = os.path.join(BASE_DIR, str(year))
    os.makedirs(year_dir, exist_ok=True)

    df_y = df_symbols[df_symbols["year"] == year].head(1000)
    symbols = df_y["symbol"].tolist()

    print(f"\n===== {year} 年 前1000只 开始下载 =====")

    for symbol in tqdm(symbols, desc=f"{year}"):
        csv_path = os.path.join(year_dir, f"{symbol}.csv")

        # ===================== 断点续传 =====================
        if os.path.exists(csv_path):
            continue

        df = download_one(symbol, year)

        if df is None or df.empty:
            failed_records.append({"year": year, "symbol": symbol})
            continue

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        time.sleep(DELAY_SECONDS)

# 保存失败清单
pd.DataFrame(failed_records).to_csv("failed_symbols.csv", index=False)
print(f"\n✅ 全部完成！失败数量：{len(failed_records)}")
print(f"📂 文件保存在：{BASE_DIR}/")
