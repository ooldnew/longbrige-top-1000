import pandas as pd
import time
import os
from tqdm import tqdm
from longport.openapi import Config, QuoteContext, Period, AdjustType
from datetime import date
import config

UNIVERSE_URL = "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv"

def get_tickers():
    print("▶ 正在从 GitHub 获取全量美股代码...")
    df = pd.read_csv(UNIVERSE_URL)
    return df[df["symbol"].str.match(r"^[A-Z]{1,5}$", na=False)]["symbol"].unique().tolist()

def fetch_data(ctx, symbol, start_d, end_d):
    for i in range(config.MAX_RETRIES):
        try:
            klines = ctx.history_candlesticks_by_date(
                symbol=symbol, period=Period.Day, 
                start=start_d, end=end_d, adjust_type=AdjustType.NoAdjust
            )
            return klines if klines else []
        except Exception as e:
            if "Rate limit" in str(e): time.sleep(5)
            time.sleep(1 * (i + 1))
    return []

def main():
    tickers = get_tickers()
    cfg = Config(config.APP_KEY, config.APP_SECRET, config.ACCESS_TOKEN)
    ctx = QuoteContext(cfg)
    
    annual_data = []
    # 分段以规避长桥 1000 条限制
    segments = [(date(2021, 1, 1), date(2023, 6, 30)), (date(2023, 7, 1), date(2025, 12, 31))]

    for symbol_raw in tqdm(tickers, desc="获取成交额排名"):
        symbol = f"{symbol_raw}.US"
        stock_sum = {}
        for s_d, e_d in segments:
            klines = fetch_data(ctx, symbol, s_d, e_d)
            for k in klines:
                y = k.timestamp.year
                if y in config.YEARS:
                    stock_sum[y] = stock_sum.get(y, 0.0) + float(k.turnover)
        
        for y, val in stock_sum.items():
            annual_data.append({"year": y, "ticker": symbol_raw, "symbol": symbol, "annual_amount_usd": val})
        
        time.sleep(config.REQUEST_DELAY)

    if not annual_data:
        print("❌ 未获取到任何数据")
        return

    df_all = pd.DataFrame(annual_data)
    final = []
    for y in config.YEARS:
        top = df_all[df_all["year"] == y].sort_values("annual_amount_usd", ascending=False).head(config.TOP_N).copy()
        top["rank"] = range(1, len(top) + 1)
        final.append(top)
        print(f"✅ {y} 年数据已构建 ({len(top)} 只)")

    pd.concat(final).to_csv(config.TOP1000_CSV, index=False)
    print(f"✨ 列表保存成功: {config.TOP1000_CSV}")

if __name__ == "__main__":
    main()
