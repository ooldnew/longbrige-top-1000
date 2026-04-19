import os
import time
import pandas as pd
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext

# ===================== 你原来能用的旧版长桥 SDK 写法 =====================
LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

# 配置
YEARS = [2021, 2022, 2023, 2024, 2025]
BASE_DIR = "us_1000_turnover"
DELAY = 0.3

# 初始化（你原来能用的写法！）
config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
quote_ctx = QuoteContext(config)

# 创建主目录
os.makedirs(BASE_DIR, exist_ok=True)

# 读取股票池
df_symbols = pd.read_csv("top1000_by_year.csv")
failed = []

# ===================== 下载单票 =====================
def download(symbol, year):
    try:
        start = f"{year}-01-01"
        end = f"{year}-12-31"

        klines = quote_ctx.candlesticks(
            symbol=symbol,
            period="day",
            start=start,
            end=end,
            adjust="forward"
        )

        rows = []
        for k in klines:
            rows.append({
                "date": k.timestamp.strftime("%Y-%m-%d"),
                "open": k.open,
                "high": k.high,
                "low": k.low,
                "close": k.close,
                "volume": k.volume,
                "turnover": k.turnover,
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"失败 {symbol}: {e}")
        return None

# ===================== 每年下载前1000只 =====================
for year in YEARS:
    year_dir = os.path.join(BASE_DIR, str(year))
    os.makedirs(year_dir, exist_ok=True)

    # 每年 取 前1000只
    df_year = df_symbols[df_symbols["year"] == year].head(1000)
    symbols = df_year["symbol"].tolist()

    print(f"\n===== {year} 年 前1000只 开始下载 =====")

    for symbol in tqdm(symbols):
        csv_path = os.path.join(year_dir, f"{symbol}.csv")

        # 断点续传：已下载就跳过
        if os.path.exists(csv_path):
            continue

        df = download(symbol, year)
        if df is not None and not df.empty:
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        else:
            failed.append([year, symbol])

        time.sleep(DELAY)

# 保存失败清单
pd.DataFrame(failed, columns=["year", "symbol"]).to_csv("failed_symbols.csv", index=False)
print(f"\n✅ 全部完成！文件保存在：{BASE_DIR}")
