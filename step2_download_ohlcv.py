import os
import time
import pandas as pd
from tqdm import tqdm

# ===================== 长桥 新版 SDK 正确导入 =====================
from longbridge.http import HttpClient
from datetime import datetime

# ===================== 配置 =====================
LB_APP_KEY = os.getenv("LB_APP_KEY")
LB_APP_SECRET = os.getenv("LB_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LB_ACCESS_TOKEN")

YEARS = [2021, 2022, 2023, 2024, 2025]
BASE_DIR = "us_1000_turnover"
DELAY = 0.3
# ==================================================

# 初始化长桥 HTTP 客户端（新版唯一正确方式）
http = HttpClient(
    app_key=LB_APP_KEY,
    app_secret=LB_APP_SECRET,
    access_token=LB_ACCESS_TOKEN
)

os.makedirs(BASE_DIR, exist_ok=True)
df_symbols = pd.read_csv("top1000_by_year.csv")
failed = []

# ===================== 下载 K 线 =====================
def download_stock(symbol, year):
    try:
        start = f"{year}-01-01"
        end = f"{year}-12-31"

        # 长桥 新版 API 接口
        resp = http.request(
            method="GET",
            path="/v1/quote/candlestick",
            query={
                "symbol": symbol,
                "period": "day",
                "start": start,
                "end": end,
                "adjust_type": "forward"
            }
        )

        data = resp.get("data", {})
        bars = data.get("candlesticks", [])
        if not bars:
            return None

        rows = []
        for b in bars:
            rows.append({
                "date": datetime.fromtimestamp(b["timestamp"]).strftime("%Y-%m-%d"),
                "open": b["open"],
                "high": b["high"],
                "low": b["low"],
                "close": b["close"],
                "volume": b["volume"],
                "turnover": b["turnover"]
            })
        return pd.DataFrame(rows)

    except Exception as e:
        print(f"失败 {symbol}: {str(e)}")
        return None

# ===================== 按年份下载 =====================
for year in YEARS:
    year_dir = os.path.join(BASE_DIR, str(year))
    os.makedirs(year_dir, exist_ok=True)

    df_year = df_symbols[df_symbols["year"] == year].head(1000)
    symbols = df_year["symbol"].tolist()

    print(f"\n=== 下载 {year} 年 前1000只 ===")

    for symbol in tqdm(symbols):
        csv_path = os.path.join(year_dir, f"{symbol}.csv")
        if os.path.exists(csv_path):
            continue

        df = download_stock(symbol, year)
        if df is not None and not df.empty:
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        else:
            failed.append([year, symbol])

        time.sleep(DELAY)

# 保存失败清单
pd.DataFrame(failed, columns=["year", "symbol"]).to_csv("failed_symbols.csv", index=False)
print(f"\n✅ 全部完成！文件在：{BASE_DIR}")
