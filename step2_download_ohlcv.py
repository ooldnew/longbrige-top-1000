import os
import time
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from longport.openapi import Config, QuoteContext, Period, AdjustType
import config

def download_one(ctx, symbol):
    # 长桥单次限制 1000 条，分两段拉取 5 年数据
    segments = [(config.LONGPORT_START, "2022-12-31"), ("2023-01-01", config.LONGPORT_END)]
    rows = []
    for start, end in segments:
        try:
            candles = ctx.history_candlesticks_by_date(
                symbol=symbol, period=Period.Day,
                start=start, end=end, adjust_type=AdjustType.ForwardAdjust
            )
            for c in candles:
                rows.append({
                    "date": c.timestamp.date(),
                    "open": float(c.open), "high": float(c.high),
                    "low": float(c.low), "close": float(c.close),
                    "volume": int(c.volume), "turnover": float(c.turnover),
                    "symbol": symbol
                })
        except Exception:
            continue
    return pd.DataFrame(rows) if rows else None

def main():
    if not os.path.exists(config.TOP1000_CSV):
        print("❌ 请先运行 Step 1")
        return

    top1000 = pd.read_csv(config.TOP1000_CSV)
    all_symbols = sorted(top1000["symbol"].unique().tolist())
    
    os.makedirs(config.PRICES_DIR, exist_ok=True)
    done = {p.stem for p in Path(config.PRICES_DIR).glob("*.parquet")}
    todo = [s for s in all_symbols if s.replace(".US", "") not in done]
    
    print(f"待下载: {len(todo)} 只")
    
    if todo:
        cfg = Config(config.APP_KEY, config.APP_SECRET, config.ACCESS_TOKEN)
        ctx = QuoteContext(cfg)
        for s in tqdm(todo):
            df = download_one(ctx, s)
            if df is not None:
                ticker = s.replace(".US", "")
                df.to_parquet(Path(config.PRICES_DIR) / f"{ticker}.parquet")
            time.sleep(config.REQUEST_DELAY)

    # 合并
    print("▶ 正在合并全量文件...")
    all_dfs = [pd.read_parquet(p) for p in Path(config.PRICES_DIR).glob("*.parquet")]
    if all_dfs:
        pd.concat(all_dfs).to_parquet(config.PRICES_ALL, index=False)
        print(f"✅ 完成！大文件保存在: {config.PRICES_ALL}")

if __name__ == "__main__":
    main()