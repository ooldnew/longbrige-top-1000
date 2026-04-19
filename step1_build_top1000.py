import pandas as pd
import time
import os
from tqdm import tqdm
from longport.openapi import Config, QuoteContext, Period, AdjustType
from datetime import date
import config

# 缓存文件：记录已完成的成交额明细
CACHE_FILE = os.path.join(config.BASE_DIR, "step1_progress_cache.csv")

def get_tickers():
    print("▶ 正在从 GitHub 获取全量美股代码...")
    df = pd.read_csv("https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv")
    return df[df["symbol"].str.match(r"^[A-Z]{1,5}$", na=False)]["symbol"].unique().tolist()

def main():
    all_tickers = get_tickers()
    
    # ── 检查断点 ──
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

    cfg = Config(config.APP_KEY, config.APP_SECRET, config.ACCESS_TOKEN)
    ctx = QuoteContext(cfg)
    
    # 将 5 年拆成两段（每段约 600 多天），避开单次 1000 条限制，同时保证下载速度
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
                    symbol=symbol, period=Period.Day, 
                    start=s_d, end=e_d, adjust_type=AdjustType.NoAdjust
                )
                if klines:
                    for k in klines:
                        y = k.timestamp.year
                        if y in config.YEARS:
                            stock_sum[y] = stock_sum.get(y, 0.0) + float(k.turnover)
            
            # 整理该股票各年数据
            for y, val in stock_sum.items():
                stock_yearly_data.append({
                    "year": y, "ticker": symbol_raw, "symbol": symbol, "annual_amount_usd": val
                })

            # ── 关键：立即存盘（断点续传的核心） ──
            if stock_yearly_data:
                pd.DataFrame(stock_yearly_data).to_csv(
                    CACHE_FILE, mode='a', index=False, header=not os.path.exists(CACHE_FILE)
                )
            else:
                # 即使没数据也记个空值，防止下次重跑
                pd.DataFrame([{"year": 0, "ticker": symbol_raw, "symbol": symbol, "annual_amount_usd": 0}]).to_csv(
                    CACHE_FILE, mode='a', index=False, header=not os.path.exists(CACHE_FILE)
                )
                
        except Exception as e:
            if "Rate limit" in str(e):
                time.sleep(10) # 触发限流时多歇会
            continue
        
        # 速度优化：稍微缩短延迟。如果长桥账号权限高，可试着改到 0.1
        time.sleep(max(config.REQUEST_DELAY, 0.15))

    # ── 最终处理：生成 Top 1000 名单 ──
    print("\n▶ 正在生成最终排名名单...")
    if os.path.exists(CACHE_FILE):
        full_df = pd.read_csv(CACHE_FILE)
        full_df = full_df[full_df['year'] > 0] # 过滤占位数据
        
        final_list = []
        for y in config.YEARS:
            top = (full_df[full_df["year"] == y]
                   .sort_values("annual_amount_usd", ascending=False)
                   .head(config.TOP_N).copy())
            top["rank"] = range(1, len(top) + 1)
            final_list.append(top)
        
        pd.concat(final_list).to_csv(config.TOP1000_CSV, index=False)
        print(f"✅ 任务成功！最终名单已保存至: {config.TOP1000_CSV}")

if __name__ == "__main__":
    main()
