"""
Natural Gas Dashboard — Phase 1 ETL Pipeline
============================================
數據來源：
  1. EIA API        → Henry Hub 生產 / 庫存 / 消費
  2. AGSI+ (GIE)    → 歐洲天然氣庫存（TTF/NBP 地區）
  3. Yahoo Finance  → HH / TTF / JKM / NBP 期貨價格 + Woodside 股價
  4. Open-Meteo     → HDD / CDD 氣候因子（無需 API Key）

需求套件：
  pip install requests yfinance pandas sqlalchemy python-dotenv

使用方式：
  1. 複製 .env.example → .env，填入 EIA_API_KEY
  2. python fetcher.py            # 執行完整 ETL
  3. python fetcher.py --source eia   # 只跑單一來源
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ── 初始化 ──────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "gas_data.db"
ENGINE   = create_engine(f"sqlite:///{DB_PATH}", echo=False)


def safe_upsert(df: pd.DataFrame, table: str):
    """pandas 3.0 相容寫入：INSERT OR IGNORE 跳過重複鍵（SQLite）"""
    if df.empty:
        return 0
    with ENGINE.connect() as conn:
        for _, row in df.iterrows():
            cols = ", ".join(str(c) for c in row.index)
            placeholders = ", ".join(f":{c}" for c in row.index)
            sql = f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})"
            conn.execute(text(sql), {str(k): v for k, v in row.to_dict().items()})
        conn.commit()
    return len(df)


EIA_API_KEY = os.getenv("EIA_API_KEY", "")
EIA_BASE    = "https://api.eia.gov/v2"

# Yahoo Finance 代碼對應
FUTURES_TICKERS = {
    "HH":  "NG=F",    # Henry Hub 天然氣期貨
    "TTF": "TTF=F",   # TTF 荷蘭天然氣期貨（若無則用替代）
    "JKM": "LNG",     # JKM 無直接 ticker，用 Cheniere Energy 作代理
    "NBP": "GGAS.L",  # NBP 英國（倫敦掛牌）
}
STOCK_TICKERS = {
    "WDS":  "WDS",    # Woodside Energy（NYSE ADR）
    "WDS.AX": "WDS.AX",  # Woodside Energy（ASX 本地）
}
BRENT_TICKER = "BZ=F"


# ═══════════════════════════════════════════════════════
# 1. 資料庫初始化
# ═══════════════════════════════════════════════════════

def init_db():
    """建立所有資料表（若不存在）"""
    with ENGINE.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS prices (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                date      TEXT NOT NULL,
                market    TEXT NOT NULL,   -- HH / TTF / JKM / NBP
                price     REAL,
                unit      TEXT,            -- USD/MMBtu
                source    TEXT,
                fetched_at TEXT,
                UNIQUE(date, market)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS eia_supply (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT NOT NULL,
                series_id   TEXT NOT NULL,
                series_name TEXT,
                value       REAL,
                unit        TEXT,
                fetched_at  TEXT,
                UNIQUE(date, series_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS agsi_storage (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                date       TEXT NOT NULL,
                country    TEXT NOT NULL,
                full_pct   REAL,          -- 庫存百分比
                gas_in_storage REAL,      -- TWh
                trend      TEXT,
                fetched_at TEXT,
                UNIQUE(date, country)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS climate (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                date     TEXT NOT NULL,
                region   TEXT NOT NULL,   -- US / EU
                hdd      REAL,            -- Heating Degree Days
                cdd      REAL,            -- Cooling Degree Days
                temp_c   REAL,
                fetched_at TEXT,
                UNIQUE(date, region)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stocks (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                date       TEXT NOT NULL,
                ticker     TEXT NOT NULL,
                close      REAL,
                volume     INTEGER,
                fetched_at TEXT,
                UNIQUE(date, ticker)
            )
        """))
        conn.commit()
    log.info("資料庫初始化完成：%s", DB_PATH)


# ═══════════════════════════════════════════════════════
# 2. EIA API — 美國生產 / 庫存 / 消費
# ═══════════════════════════════════════════════════════

EIA_SERIES = {
    # 週頻
    "NW2_EPG0_SWO_R48_BCF": {
        "name": "US Working Gas in Storage (Lower 48)",
        "unit": "Bcf",
        "freq": "weekly",
    },
    # 月頻
    "N9070US2":  {"name": "US Dry Natural Gas Production", "unit": "MMcf", "freq": "monthly"},
    "N9140US2":  {"name": "US Natural Gas Total Consumption", "unit": "MMcf", "freq": "monthly"},
    "N9100US2":  {"name": "US Natural Gas Exports", "unit": "MMcf", "freq": "monthly"},
    "N9130US2":  {"name": "US Natural Gas Imports", "unit": "MMcf", "freq": "monthly"},
}


def fetch_eia(series_id: str, meta: dict, days_back: int = 90) -> pd.DataFrame:
    """從 EIA API v2 抓取單一 series"""
    if not EIA_API_KEY:
        log.warning("EIA_API_KEY 未設定，跳過 EIA 數據（請參考 .env.example）")
        return pd.DataFrame()

    start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # EIA v2 路徑依 series 類型不同
    if meta["freq"] == "weekly":
        url = f"{EIA_BASE}/natural-gas/stor/wkly/data/"
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": series_id,
            "start": start,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 52,
            "offset": 0,
        }
    else:
        url = f"{EIA_BASE}/natural-gas/sum/snd/data/"
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "monthly",
            "data[0]": "value",
            "facets[series][]": series_id,
            "start": start,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 24,
            "offset": 0,
        }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("response", {}).get("data", [])
        if not data:
            log.warning("EIA series %s 無資料返回", series_id)
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df = df.rename(columns={"period": "date", "value": "value"})
        df["series_id"]   = series_id
        df["series_name"] = meta["name"]
        df["unit"]        = meta["unit"]
        df["fetched_at"]  = datetime.utcnow().isoformat()
        df["value"]       = pd.to_numeric(df["value"], errors="coerce")
        return df[["date", "series_id", "series_name", "value", "unit", "fetched_at"]]

    except Exception as e:
        log.error("EIA fetch 失敗 [%s]: %s", series_id, e)
        return pd.DataFrame()


def run_eia(days_back: int = 90):
    log.info("── EIA 數據擷取開始 ──")
    all_rows = []
    for sid, meta in EIA_SERIES.items():
        df = fetch_eia(sid, meta, days_back)
        if not df.empty:
            all_rows.append(df)
            log.info("  ✓ %s：%d 筆", meta["name"], len(df))
        time.sleep(0.3)  # 避免打太快

    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
        with ENGINE.connect() as conn:
            for _, row in combined.iterrows():
                conn.execute(text("""INSERT OR IGNORE INTO eia_supply
                    (date,series_id,series_name,value,unit,fetched_at)
                    VALUES (:date,:series_id,:series_name,:value,:unit,:fetched_at)"""),
                    row.to_dict())
            conn.commit()
        log.info("EIA 共寫入 %d 筆記錄", len(combined))
    else:
        log.warning("EIA：無任何資料寫入")


# ═══════════════════════════════════════════════════════
# 3. AGSI+ (Gas Infrastructure Europe) — 歐洲庫存
# ═══════════════════════════════════════════════════════

AGSI_URL      = "https://agsi.gie.eu/api"
AGSI_COUNTRIES = ["DE", "IT", "FR", "NL", "ES", "AT", "BE", "PL", "EU"]  # EU = 歐盟加總


def fetch_agsi(country: str, days_back: int = 90) -> pd.DataFrame:
    """
    AGSI+ 免費 API（無需 key，但建議在 header 加 User-Agent）
    文件：https://agsi.gie.eu/
    """
    end   = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    params = {
        "country": country,
        "from":    start,
        "till":    end,
        "size":    200,
    }
    headers = {"User-Agent": "GasDashboard/1.0 (research)"}

    try:
        resp = requests.get(AGSI_URL, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", [])
        if not data:
            return pd.DataFrame()

        rows = []
        for item in data:
            rows.append({
                "date":           item.get("gasDayStart", "")[:10],
                "country":        country,
                "full_pct":       float(item.get("full", 0) or 0),
                "gas_in_storage": float(item.get("gasInStorage", 0) or 0),
                "trend":          item.get("trend", ""),
                "fetched_at":     datetime.utcnow().isoformat(),
            })
        return pd.DataFrame(rows)

    except Exception as e:
        log.error("AGSI fetch 失敗 [%s]: %s", country, e)
        return pd.DataFrame()


def run_agsi(days_back: int = 90):
    log.info("── AGSI+ 歐洲庫存擷取開始 ──")
    all_rows = []
    for c in AGSI_COUNTRIES:
        df = fetch_agsi(c, days_back)
        if not df.empty:
            all_rows.append(df)
            log.info("  ✓ %s：%d 筆（最新 %.1f%%）", c, len(df), df["full_pct"].iloc[0])
        time.sleep(0.5)

    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
        with ENGINE.connect() as conn:
            for _, row in combined.iterrows():
                conn.execute(text("""INSERT OR IGNORE INTO agsi_storage
                    (date,country,full_pct,gas_in_storage,trend,fetched_at)
                    VALUES (:date,:country,:full_pct,:gas_in_storage,:trend,:fetched_at)"""),
                    row.to_dict())
            conn.commit()
        log.info("AGSI+ 共寫入 %d 筆記錄", len(combined))


# ═══════════════════════════════════════════════════════
# 4. Yahoo Finance — 期貨價格 + Woodside 股價
# ═══════════════════════════════════════════════════════

def fetch_yahoo_prices(days_back: int = 90):
    log.info("── Yahoo Finance 價格擷取開始 ──")
    start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")

    price_rows = []
    stock_rows = []

    def yf_close(ticker: str) -> pd.DataFrame:
        """
        yfinance 新版 (0.2.x+) 單一 ticker 下載，
        用 Ticker.history() 避免 MultiIndex 問題
        """
        t = yf.Ticker(ticker)
        df = t.history(start=start, end=end, auto_adjust=True)
        if df.empty:
            return pd.DataFrame()
        df = df.reset_index()
        # 欄位名稱統一為字串（避免 MultiIndex 殘留）
        df.columns = [c if isinstance(c, str) else c[0] for c in df.columns]
        df = df.rename(columns={"Date": "Date", "Close": "Close", "Volume": "Volume"})
        return df

    # 天然氣期貨
    for market, ticker in FUTURES_TICKERS.items():
        try:
            df = yf_close(ticker)
            if df.empty:
                log.warning("  ✗ %s (%s)：無資料", market, ticker)
                continue

            for _, row in df.iterrows():
                price_rows.append({
                    "date":       str(row["Date"])[:10],
                    "market":     market,
                    "price":      round(float(row["Close"]), 4),
                    "unit":       "USD/MMBtu" if market != "JKM" else "proxy",
                    "source":     f"Yahoo:{ticker}",
                    "fetched_at": datetime.utcnow().isoformat(),
                })
            log.info("  ✓ %s (%s)：%d 筆，最新 %.3f",
                     market, ticker, len(df), float(df["Close"].iloc[-1]))

        except Exception as e:
            log.error("  ✗ Yahoo %s (%s): %s", market, ticker, e)
        time.sleep(0.3)

    # Brent 油價（LNG 長約定價基礎）
    try:
        df = yf_close(BRENT_TICKER)
        if not df.empty:
            for _, row in df.iterrows():
                price_rows.append({
                    "date":       str(row["Date"])[:10],
                    "market":     "BRENT",
                    "price":      round(float(row["Close"]), 4),
                    "unit":       "USD/bbl",
                    "source":     f"Yahoo:{BRENT_TICKER}",
                    "fetched_at": datetime.utcnow().isoformat(),
                })
            log.info("  ✓ BRENT (%s)：%d 筆，最新 %.2f",
                     BRENT_TICKER, len(df), float(df["Close"].iloc[-1]))
    except Exception as e:
        log.error("  ✗ Brent: %s", e)

    # Woodside + 相關股票
    all_stock_tickers = {**STOCK_TICKERS, "XOM": "XOM", "CVX": "CVX", "SHEL": "SHEL"}
    for name, ticker in all_stock_tickers.items():
        try:
            df = yf_close(ticker)
            if df.empty:
                log.warning("  ✗ %s：無資料", ticker)
                continue
            for _, row in df.iterrows():
                stock_rows.append({
                    "date":       str(row["Date"])[:10],
                    "ticker":     ticker,
                    "close":      round(float(row["Close"]), 4),
                    "volume":     int(row.get("Volume", 0)),
                    "fetched_at": datetime.utcnow().isoformat(),
                })
            log.info("  ✓ %s：%d 筆，最新 %.2f",
                     ticker, len(df), float(df["Close"].iloc[-1]))
        except Exception as e:
            log.error("  ✗ Stock %s: %s", ticker, e)
        time.sleep(0.3)

    # 寫入 DB（INSERT OR IGNORE 避免重複）
    def insert_ignore(rows, table, cols):
        placeholders = ", ".join([f":{c}" for c in cols])
        col_str = ", ".join(cols)
        sql = f"INSERT OR IGNORE INTO {table} ({col_str}) VALUES ({placeholders})"
        with ENGINE.connect() as conn:
            for row in rows:
                conn.execute(text(sql), row)
            conn.commit()

    if price_rows:
        insert_ignore(price_rows, "prices",
                      ["date","market","price","unit","source","fetched_at"])
        log.info("價格資料共寫入 %d 筆", len(price_rows))

    if stock_rows:
        insert_ignore(stock_rows, "stocks",
                      ["date","ticker","close","volume","fetched_at"])
        log.info("股價資料共寫入 %d 筆", len(stock_rows))


# ═══════════════════════════════════════════════════════
# 5. Open-Meteo — HDD / CDD 氣候因子
# ═══════════════════════════════════════════════════════

CLIMATE_LOCATIONS = {
    "US_Boston":  {"lat": 42.36, "lon": -71.06, "region": "US"},
    "US_Chicago": {"lat": 41.88, "lon": -87.63, "region": "US"},
    "EU_Berlin":  {"lat": 52.52, "lon": 13.40,  "region": "EU"},
    "EU_Paris":   {"lat": 48.85, "lon":  2.35,  "region": "EU"},
}
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def calc_hdd_cdd(temp_c: float, base: float = 18.0):
    """計算 HDD / CDD，基準溫度 18°C"""
    hdd = max(base - temp_c, 0)
    cdd = max(temp_c - base, 0)
    return round(hdd, 2), round(cdd, 2)


def fetch_climate(days_back: int = 30):
    log.info("── Open-Meteo 氣候數據擷取開始 ──")
    start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")

    rows = []
    for loc_name, loc in CLIMATE_LOCATIONS.items():
        params = {
            "latitude":       loc["lat"],
            "longitude":      loc["lon"],
            "daily":          "temperature_2m_mean",
            "start_date":     start,
            "end_date":       end,
            "timezone":       "UTC",
        }
        try:
            resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("daily", {})
            dates = data.get("time", [])
            temps = data.get("temperature_2m_mean", [])

            for date, temp in zip(dates, temps):
                if temp is None:
                    continue
                hdd, cdd = calc_hdd_cdd(temp)
                rows.append({
                    "date":       date,
                    "region":     loc["region"],
                    "hdd":        hdd,
                    "cdd":        cdd,
                    "temp_c":     round(temp, 2),
                    "fetched_at": datetime.utcnow().isoformat(),
                })
            log.info("  ✓ %s：%d 天", loc_name, len(dates))

        except Exception as e:
            log.error("  ✗ Open-Meteo [%s]: %s", loc_name, e)
        time.sleep(0.3)

    if rows:
        # 同地區同日期取平均再寫入
        df = pd.DataFrame(rows)
        df_agg = df.groupby(["date", "region"]).agg(
            hdd=("hdd", "mean"),
            cdd=("cdd", "mean"),
            temp_c=("temp_c", "mean"),
            fetched_at=("fetched_at", "first"),
        ).reset_index()
        df_agg["hdd"]    = df_agg["hdd"].round(2)
        df_agg["cdd"]    = df_agg["cdd"].round(2)
        df_agg["temp_c"] = df_agg["temp_c"].round(2)
        with ENGINE.connect() as conn:
            for _, row in df_agg.iterrows():
                conn.execute(text("""INSERT OR IGNORE INTO climate
                    (date,region,hdd,cdd,temp_c,fetched_at)
                    VALUES (:date,:region,:hdd,:cdd,:temp_c,:fetched_at)"""),
                    row.to_dict())
            conn.commit()
        log.info("氣候資料共寫入 %d 筆（地區日均）", len(df_agg))


# ═══════════════════════════════════════════════════════
# 6. 供需差計算（Feature Engineering 前置）
# ═══════════════════════════════════════════════════════

def compute_supply_demand_gap():
    """
    從 eia_supply 計算每月供需差並輸出摘要
    供需差 = 生產 + 進口 - 消費 - 出口
    """
    log.info("── 計算供需差 ──")
    query = """
        SELECT date, series_id, value
        FROM eia_supply
        ORDER BY date DESC
    """
    try:
        df = pd.read_sql(query, ENGINE)
        if df.empty:
            log.warning("eia_supply 無資料，跳過供需差計算")
            return

        pivot = df.pivot_table(index="date", columns="series_id", values="value")

        prod_col  = "N9070US2"  # 生產
        cons_col  = "N9140US2"  # 消費
        exp_col   = "N9100US2"  # 出口
        imp_col   = "N9130US2"  # 進口

        # 只計算四個 series 都有值的月份
        needed = [prod_col, cons_col, exp_col, imp_col]
        available = [c for c in needed if c in pivot.columns]

        if len(available) < 2:
            log.warning("EIA 資料不足，無法計算供需差（需要至少生產+消費）")
            return

        result = pivot[available].dropna()
        if prod_col in result.columns and cons_col in result.columns:
            result["gap_mmcf"] = (
                result.get(prod_col, 0)
                + result.get(imp_col, 0)
                - result.get(cons_col, 0)
                - result.get(exp_col, 0)
            )
            result["status"] = result["gap_mmcf"].apply(
                lambda x: "供給過剩" if x > 0 else "供給不足"
            )
            print("\n供需差摘要（最近 6 個月）：")
            print(result[["gap_mmcf", "status"]].head(6).to_string())

    except Exception as e:
        log.error("供需差計算失敗: %s", e)


# ═══════════════════════════════════════════════════════
# 7. 快速健康檢查
# ═══════════════════════════════════════════════════════

def health_check():
    """印出各資料表的最新數據摘要"""
    print("\n" + "═"*50)
    print("  資料庫健康檢查")
    print("═"*50)

    checks = {
        "prices":       "SELECT market, COUNT(*) as n, MAX(date) as latest, ROUND(AVG(price),3) as avg_price FROM prices GROUP BY market",
        "eia_supply":   "SELECT series_name, COUNT(*) as n, MAX(date) as latest FROM eia_supply GROUP BY series_name",
        "agsi_storage": "SELECT country, COUNT(*) as n, MAX(date) as latest, ROUND(AVG(full_pct),1) as avg_pct FROM agsi_storage GROUP BY country",
        "climate":      "SELECT region, COUNT(*) as n, MAX(date) as latest, ROUND(AVG(hdd),1) as avg_hdd FROM climate GROUP BY region",
        "stocks":       "SELECT ticker, COUNT(*) as n, MAX(date) as latest, ROUND(AVG(close),2) as avg_close FROM stocks GROUP BY ticker",
    }

    for table, q in checks.items():
        try:
            df = pd.read_sql(q, ENGINE)
            if df.empty:
                print(f"\n[{table}] 無資料")
            else:
                print(f"\n[{table}]")
                print(df.to_string(index=False))
        except Exception as e:
            print(f"\n[{table}] 錯誤: {e}")

    print("\n" + "═"*50 + "\n")


# ═══════════════════════════════════════════════════════
# 8. Main — CLI 介面
# ═══════════════════════════════════════════════════════

def run_all(days_back: int = 90):
    log.info("═══ Phase 1 ETL 全量執行（days_back=%d）═══", days_back)
    run_eia(days_back)
    run_agsi(days_back)
    fetch_yahoo_prices(days_back)
    fetch_climate(min(days_back, 30))
    compute_supply_demand_gap()
    health_check()
    log.info("═══ ETL 完成 ═══")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Natural Gas ETL Pipeline")
    parser.add_argument("--source", choices=["eia", "agsi", "yahoo", "climate", "all"],
                        default="all", help="要執行的數據源（預設: all）")
    parser.add_argument("--days", type=int, default=90,
                        help="抓取過去幾天的數據（預設: 90）")
    parser.add_argument("--check", action="store_true",
                        help="只執行健康檢查，不抓取數據")
    args = parser.parse_args()

    init_db()

    if args.check:
        health_check()
        sys.exit(0)

    dispatch = {
        "eia":     lambda: run_eia(args.days),
        "agsi":    lambda: run_agsi(args.days),
        "yahoo":   lambda: fetch_yahoo_prices(args.days),
        "climate": lambda: fetch_climate(min(args.days, 30)),
        "all":     lambda: run_all(args.days),
    }
    dispatch[args.source]()
