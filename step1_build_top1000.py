import pandas as pd
import time
import os
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext, Period, AdjustType
from datetime import date

# ===================== 【修复】长桥新版 SDK 配置 =====================
# 从环境变量读取（和你的 workflow 完全匹配）
APP_KEY = os.getenv("LP_APP_KEY")
APP_SECRET = os.getenv("LP_APP_SECRET")
ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

# 固定配置
YEARS = [2021, 2022, 2023, 2024, 2025]
BASE_DIR = "."
TOP_N = 1000
REQUEST_DELAY = 0.2
TOP1000_CSV = "top1000_by_year.csv"
CACHE_FILE = "step1_progress_cache.csv"
# =================================================================

def get_tickers():
    print("▶ 正在从 GitHub 获取全量美股代码...")
    df = pd.read_csv("https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv")
    return df[df["symbol"].str.match(r"^[A-Z]{1,5}$", na=False)]["symbol"].unique().tolist()

def main():
    all_tickers = get_tickers()
    
    done_tickers = set()
    if os.path.exists(CACHE_FILE):
        try:
            df_cache = pd.read_csv(CACHE_FILE)
            if not df_cache.empty:
                done_tickers = set(df_cache['ticker'].unique().tolist())
                print(f"▶ 检测到断点：已完成 {len(done_tickers)} 只，跳过并继续...")
        except:
            pass
    
    tickers_to_run = [t for t in all_tickers if t not in done_tickers]

    # ===================== 【修复】新版 SDK 唯一正确写法 =====================
    config = Config.from_env()
    ctx = QuoteContext(config)
    # ======================================================================

    segments = [
        (date(2021, 1, 1), date(2023, 6, 30)), 
        (date(2023, 7, 1), date(2025, 12, 31))
    ]

    for symbol_raw in tqdm(tickers_to_run, desc="获取成交额排名"):
        symbol = f"{symbol_raw}.US"
        stock_yearly_data = []
        stock_sum = {}
        
        try:
            for s_d, e_d in segments:
                klines = ctx.history_candlesticks_by_date(
                    symbol=symbol, 
                    period=Period.Day, 
                    start_date=s_d, 
                    end_date=e_d, 
                    adjust_type=AdjustType.NoAdjust
                )
                if klines:
                    for k in klines.candlesticks:
                        y = pd.to_datetime(k.timestamp, unit='s').year
                        if y in YEARS:
                            stock_sum[y] = stock_sum.get(y, 0.0) + float(k.turnover)
            
            for y, val in stock_sum.items():
                stock_yearly_data.append({
                    "year": y, "ticker": symbol_raw, "symbol": symbol, "annual_amount_usd": val
                })

            if stock_yearly_data:
                pd.DataFrame(stock_yearly_data).to_csv(
                    CACHE_FILE, mode='a', index=False, header=not os.path.exists(CACHE_FILE)
                )
            else:
                pd.DataFrame([{"year": 0, "ticker": symbol_raw, "symbol": symbol, "annual_amount_usd": 0}]).to_csv(
                    CACHE_FILE, mode='a', index=False, header=not os.path.exists(CACHE_FILE)
                )
                
        except Exception as e:
            if "Rate limit" in str(e):
                time.sleep(10)
            continue
        
        time.sleep(max(REQUEST_DELAY, 0.15))

    print("\n▶ 正在生成最终排名名单...")
    if os.path.exists(CACHE_FILE):
        full_df = pd.read_csv(CACHE_FILE)
        full_df = full_df[full_df['year'] > 0]
        
        final_list = []
        for y in YEARS:
            top = (full_df[full_df["year"] == y]
                   .sort_values("annual_amount_usd", ascending=False)
                   .head(TOP_N).copy())
            top["rank"] = range(1, len(top) + 1)
            final_list.append(top)
        
        pd.concat(final_list).to_csv(TOP1000_CSV, index=False)
        print(f"✅ 任务成功！最终名单已保存至: {TOP1000_CSV}")

if __name__ == "__main__":
    main()
