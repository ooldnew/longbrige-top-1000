import os
from datetime import date

# ── 安全性：从环境变量读取长桥密钥 ────────────────────────────────────────────
# 在 GitHub Actions 中，这些值来自 secrets.LP_APP_KEY 等
APP_KEY      = os.environ.get("LP_APP_KEY", "你的本地KEY")
APP_SECRET   = os.environ.get("LP_APP_SECRET", "你的本地SECRET")
ACCESS_TOKEN = os.environ.get("LP_ACCESS_TOKEN", "你的本地TOKEN")

# ── 路径配置 ──────────────────────────────────────────────────────────────────
# GitHub Actions 环境中 os.getcwd() 就是仓库根目录
BASE_DIR = os.getcwd()
PRICES_DIR   = os.path.join(BASE_DIR, "prices")
TOP1000_CSV  = os.path.join(BASE_DIR, "top1000_by_year.csv")
PRICES_ALL   = os.path.join(BASE_DIR, "prices_all.parquet")

# ── 业务参数 ──────────────────────────────────────────────────────────────────
YEARS = [2021, 2022, 2023, 2024, 2025]
TOP_N = 1000

LONGPORT_START = date(2020, 1, 1)
LONGPORT_END   = date(2025, 4, 18)

# 频率控制：公开仓库建议保守一点，设为 0.25 秒
REQUEST_DELAY  = 0.25 
MAX_RETRIES    = 3
