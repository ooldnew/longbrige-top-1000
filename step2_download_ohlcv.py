import os
import time
import pandas as pd
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext

# ===================== 密钥已按你的名字修正 =====================
LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

# 下载年份
YEARS = [2021, 2022, 2023, 2024, 2025]

# 保存目录
BASE_DIR = "us_1000_turnover"
os.makedirs(BASE_DIR, exist_ok=True)

# 延迟防限流
DELAY_SECONDS = 0.3
# =================================================================

# 初始化长桥
config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
quote_ctx = QuoteContext(config)

# 读取股票列表
df_symbols = pd.read_csv("top1000_by_year.csv")
failed_records = []

# 下载单只股票
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
        print(f"[失败] {symbol} | {str(e)}")
        return None

# 按年份下载
for year in YEARS:
    year_dir = os.path.join(BASE_DIR, str(year))
    os.makedirs(year_dir, exist_ok=True)

    df_y = df_symbols[df_symbols["year"] == year].head(1000)
    symbols = df_y["symbol"].tolist()

    print(f"\n===== {year} 年 前1000只 开始下载 =====")

    for symbol in tqdm(symbols, desc=f"{year}"):
        csv_path = os.path.join(year_dir, f"{symbol}.csv")

        # 断点续传：已下载就跳过
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
print(f"\n✅ 全部完成！文件保存在：{BASE_DIR}/")
