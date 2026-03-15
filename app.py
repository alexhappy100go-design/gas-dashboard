"""
Natural Gas Dashboard — Phase 2
================================
執行方式：streamlit run app.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

# ── 設定 ──────────────────────────────────────────────
DB_PATH = Path(__file__).parent / "gas_data.db"

# EIA API Key：優先讀 Streamlit Secrets，其次讀 .env
import os
from dotenv import load_dotenv
load_dotenv()
try:
    EIA_API_KEY = st.secrets["EIA_API_KEY"]
except Exception:
    EIA_API_KEY = os.getenv("EIA_API_KEY", "")

EIA_BASE = "https://api.eia.gov/v2"
EIA_SERIES = {
    "NW2_EPG0_SWO_R48_BCF": {"name": "US Working Gas in Storage", "unit": "Bcf", "freq": "weekly"},
    "N9070US2": {"name": "US Dry Natural Gas Production", "unit": "MMcf", "freq": "monthly"},
    "N9140US2": {"name": "US Natural Gas Total Consumption", "unit": "MMcf", "freq": "monthly"},
}

st.set_page_config(
    page_title="Global Gas Intelligence",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── 顏色主題 ──────────────────────────────────────────
COLORS = {
    "HH":    "#22d3a5",
    "TTF":   "#60a5fa",
    "JKM":   "#fbbf24",
    "NBP":   "#a78bfa",
    "BRENT": "#f97316",
    "WDS":   "#34d399",
    "bg":    "#0a0e17",
    "card":  "#111827",
}

# ── CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    /* 主背景 */
    .stApp { background-color: #0a0e17; color: #e2e8f0; }
    [data-testid="stAppViewContainer"] { background-color: #0a0e17; }
    [data-testid="stHeader"] { background-color: #111827; border-bottom: 1px solid rgba(255,255,255,0.07); }

    /* Metric 卡片 */
    [data-testid="stMetric"] {
        background-color: #111827;
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 10px;
        padding: 16px;
    }
    [data-testid="stMetricLabel"] { font-size: 11px !important; color: #64748b !important; letter-spacing: 0.06em; }
    [data-testid="stMetricValue"] { font-size: 28px !important; font-family: 'IBM Plex Mono', monospace !important; }

    /* Tab */
    .stTabs [data-baseweb="tab-list"] { background-color: #111827; border-bottom: 1px solid rgba(255,255,255,0.07); }
    .stTabs [data-baseweb="tab"] { color: #64748b; font-size: 13px; }
    .stTabs [aria-selected="true"] { color: #22d3a5 !important; border-bottom: 2px solid #22d3a5 !important; }

    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #111827; }

    /* DataFrame */
    [data-testid="stDataFrame"] { border: 1px solid rgba(255,255,255,0.07); border-radius: 8px; }

    /* 標題 */
    h1, h2, h3 { color: #e2e8f0 !important; }

    /* 隱藏 Streamlit 預設元素 */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    .stDeployButton { display: none; }

    /* 自訂卡片 */
    .info-card {
        background: #111827;
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .signal-bull { color: #22d3a5; font-weight: 600; }
    .signal-bear { color: #f87171; font-weight: 600; }
    .signal-neut { color: #fbbf24; font-weight: 600; }
    .label-muted { color: #64748b; font-size: 12px; }
</style>
""", unsafe_allow_html=True)


# ── 資料讀取 ──────────────────────────────────────────
FUTURES_MAP = {"HH":"NG=F","TTF":"TTF=F","JKM":"LNG","BRENT":"BZ=F"}
STOCK_MAP   = {"WDS":"WDS","WDS.AX":"WDS.AX","XOM":"XOM","CVX":"CVX","SHEL":"SHEL"}

@st.cache_data(ttl=3600)
def fetch_live(tickers_map: dict, col: str) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError:
        return pd.DataFrame()
    start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    rows = []
    for name, ticker in tickers_map.items():
        try:
            df = yf.Ticker(ticker).history(start=start, auto_adjust=True).reset_index()
            if df.empty: continue
            df.columns = [c if isinstance(c,str) else c[0] for c in df.columns]
            for _, row in df.iterrows():
                rows.append({"date": str(row["Date"])[:10], col: name, "value": round(float(row["Close"]),4)})
        except Exception:
            continue
    if not rows: return pd.DataFrame()
    r = pd.DataFrame(rows)
    r["date"] = pd.to_datetime(r["date"], format="mixed")
    return r

@st.cache_data(ttl=300)
def load_prices(days: int = 90) -> pd.DataFrame:
    if DB_PATH.exists():
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql("SELECT date, market, price FROM prices WHERE date >= ? ORDER BY date", conn, params=(since,))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], format="mixed")
            return df
    df = fetch_live(FUTURES_MAP, "market")
    if not df.empty: df = df.rename(columns={"value":"price"})
    return df


@st.cache_data(ttl=300)
def load_stocks(days: int = 90) -> pd.DataFrame:
    if DB_PATH.exists():
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql("SELECT date, ticker, close FROM stocks WHERE date >= ? ORDER BY date", conn, params=(since,))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], format="mixed")
            return df
    df = fetch_live(STOCK_MAP, "ticker")
    if not df.empty: df = df.rename(columns={"value":"close"})
    return df


@st.cache_data(ttl=3600)
def fetch_live_eia() -> pd.DataFrame:
    """雲端模式：直接從 EIA API 抓取"""
    import requests
    if not EIA_API_KEY:
        return pd.DataFrame()
    rows = []
    for sid, meta in EIA_SERIES.items():
        from datetime import datetime, timedelta
        start = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        if meta["freq"] == "weekly":
            url = f"{EIA_BASE}/natural-gas/stor/wkly/data/"
            params = {"api_key": EIA_API_KEY, "frequency": "weekly",
                      "data[0]": "value", "facets[series][]": sid,
                      "start": start, "sort[0][column]": "period",
                      "sort[0][direction]": "desc", "length": 52}
        else:
            url = f"{EIA_BASE}/natural-gas/sum/snd/data/"
            params = {"api_key": EIA_API_KEY, "frequency": "monthly",
                      "data[0]": "value", "facets[series][]": sid,
                      "start": start, "sort[0][column]": "period",
                      "sort[0][direction]": "desc", "length": 24}
        try:
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json().get("response", {}).get("data", [])
            for item in data:
                rows.append({"date": item.get("period",""),
                             "series_name": meta["name"],
                             "value": float(item.get("value") or 0),
                             "unit": meta["unit"]})
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], format="mixed")
    return df

@st.cache_data(ttl=300)
def load_eia() -> pd.DataFrame:
    if DB_PATH.exists():
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql("SELECT date, series_name, value, unit FROM eia_supply ORDER BY date DESC", conn)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], format="mixed")
            return df
    return fetch_live_eia()


@st.cache_data(ttl=300)
def load_climate(days: int = 30) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT date, region, hdd, cdd, temp_c FROM climate WHERE date >= ? ORDER BY date", conn, params=(since,))
    if df.empty: return df
    df["date"] = pd.to_datetime(df["date"], format="mixed")
    return df


def get_latest(df: pd.DataFrame, market: str) -> tuple:
    """回傳 (最新價, 週漲跌幅%)"""
    sub = df[df["market"] == market].sort_values("date")
    if sub.empty:
        return None, None
    latest = sub["price"].iloc[-1]
    week_ago = sub[sub["date"] <= sub["date"].iloc[-1] - timedelta(days=7)]
    if not week_ago.empty:
        prev = week_ago["price"].iloc[-1]
        chg = (latest - prev) / prev * 100
    else:
        chg = None
    return latest, chg


def simple_signal(chg: float | None) -> str:
    if chg is None:
        return "—"
    if chg > 3:
        return "↑ 看漲"
    elif chg < -3:
        return "↓ 看跌"
    else:
        return "→ 中性"


def signal_color(sig: str) -> str:
    if "↑" in sig:
        return "signal-bull"
    elif "↓" in sig:
        return "signal-bear"
    return "signal-neut"


# ── 圖表函式 ──────────────────────────────────────────
def make_price_chart(df: pd.DataFrame, markets: list) -> go.Figure:
    fig = go.Figure()
    for m in markets:
        sub = df[df["market"] == m]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["date"], y=sub["price"],
            name=m, mode="lines",
            line=dict(color=COLORS.get(m, "#fff"), width=1.8),
            hovertemplate=f"<b>{m}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.3f}}<extra></extra>",
        ))
    fig.update_layout(
        plot_bgcolor="#0a0e17", paper_bgcolor="#0a0e17",
        font=dict(color="#e2e8f0", size=11),
        legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.08),
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", showgrid=True),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", showgrid=True),
        hovermode="x unified",
    )
    return fig


def make_stock_chart(df: pd.DataFrame, tickers: list) -> go.Figure:
    fig = go.Figure()
    for t in tickers:
        sub = df[df["ticker"] == t].copy()
        if sub.empty:
            continue
        # 標準化為 100 方便比較
        base = sub["close"].iloc[0]
        sub["norm"] = sub["close"] / base * 100
        fig.add_trace(go.Scatter(
            x=sub["date"], y=sub["norm"],
            name=t, mode="lines",
            line=dict(width=1.8),
            hovertemplate=f"<b>{t}</b><br>%{{x|%Y-%m-%d}}<br>指數: %{{y:.1f}}<extra></extra>",
        ))
    fig.add_hline(y=100, line_dash="dash", line_color="rgba(255,255,255,0.2)")
    fig.update_layout(
        plot_bgcolor="#0a0e17", paper_bgcolor="#0a0e17",
        font=dict(color="#e2e8f0", size=11),
        legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.08),
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="指數（起始=100）"),
        hovermode="x unified",
    )
    return fig


def make_hdd_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for region, color in [("US", "#22d3a5"), ("EU", "#60a5fa")]:
        sub = df[df["region"] == region]
        if sub.empty:
            continue
        fig.add_trace(go.Bar(
            x=sub["date"], y=sub["hdd"],
            name=f"{region} HDD",
            marker_color=color, opacity=0.8,
            hovertemplate=f"<b>{region}</b><br>%{{x|%Y-%m-%d}}<br>HDD: %{{y:.1f}}<extra></extra>",
        ))
    fig.update_layout(
        plot_bgcolor="#0a0e17", paper_bgcolor="#0a0e17",
        font=dict(color="#e2e8f0", size=11),
        legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.08),
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="HDD"),
        barmode="group", hovermode="x unified",
    )
    return fig


# ══════════════════════════════════════════════════════
# UI 主體
# ══════════════════════════════════════════════════════

# Header
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("## ⛽ Global Natural Gas Intelligence")
    st.markdown(
        f"<span class='label-muted'>數據來源：EIA · Yahoo Finance · Open-Meteo &nbsp;|&nbsp; "
        f"更新時間：{datetime.now().strftime('%Y-%m-%d %H:%M')}</span>",
        unsafe_allow_html=True
    )
with col_h2:
    days_option = st.selectbox("時間範圍", [30, 60, 90], index=2, label_visibility="collapsed")

st.divider()

# 載入資料
prices_df = load_prices(days_option)
stocks_df = load_stocks(days_option)
eia_df    = load_eia()
climate_df = load_climate(30)

# ── 分頁 ──────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📊 總覽", "🔥 供需分析", "📈 股價追蹤", "🌡️ 氣候因子"])


# ════════════════════════════════════════════════════
# TAB 1：總覽
# ════════════════════════════════════════════════════
with tab1:

    # 價格卡片
    markets = ["HH", "TTF", "JKM", "BRENT"]
    cols = st.columns(4)
    for i, m in enumerate(markets):
        price, chg = get_latest(prices_df, m)
        sig = simple_signal(chg)
        with cols[i]:
            if price is not None:
                delta_str = f"{chg:+.1f}% 週" if chg is not None else "N/A"
                st.metric(
                    label=m,
                    value=f"{price:.3f}",
                    delta=delta_str,
                )
                st.markdown(
                    f"<span class='{signal_color(sig)}'>{sig}</span>",
                    unsafe_allow_html=True
                )
            else:
                st.metric(label=m, value="N/A", delta="無資料")

    st.markdown("#### 價格走勢")
    market_select = st.multiselect(
        "選擇市場", ["HH", "TTF", "JKM", "BRENT"],
        default=["HH", "TTF", "JKM"],
        label_visibility="collapsed"
    )
    if not prices_df.empty and market_select:
        st.plotly_chart(
            make_price_chart(prices_df, market_select),
            use_container_width=True
        )
    else:
        st.info("無價格資料，請先執行 `python fetcher.py --source yahoo`")

    # EIA 最新數據摘要
    if not eia_df.empty:
        st.markdown("#### EIA 最新數據")
        latest_eia = eia_df.groupby("series_name").first().reset_index()
        latest_eia = latest_eia[["series_name", "value", "unit", "date"]]
        latest_eia.columns = ["數據項目", "數值", "單位", "日期"]
        st.dataframe(latest_eia, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════
# TAB 2：供需分析
# ════════════════════════════════════════════════════
with tab2:
    if eia_df.empty:
        st.warning("EIA 資料尚未載入，請先執行 `python fetcher.py --source eia`")
    else:
        st.markdown("#### EIA 美國天然氣供需數據")

        # 庫存走勢
        storage = eia_df[eia_df["series_name"].str.contains("Storage", na=False)].copy()
        if not storage.empty:
            st.markdown("**週庫存變化（Working Gas in Storage，Bcf）**")
            fig_s = go.Figure()
            fig_s.add_trace(go.Scatter(
                x=storage["date"], y=storage["value"],
                mode="lines+markers",
                line=dict(color="#60a5fa", width=2),
                marker=dict(size=5),
                fill="tozeroy", fillcolor="rgba(96,165,250,0.08)",
                name="庫存量",
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f} Bcf<extra></extra>",
            ))
            fig_s.update_layout(
                plot_bgcolor="#0a0e17", paper_bgcolor="#0a0e17",
                font=dict(color="#e2e8f0", size=11),
                margin=dict(l=10, r=10, t=20, b=10),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Bcf"),
            )
            st.plotly_chart(fig_s, use_container_width=True)

        # 生產 vs 消費
        prod = eia_df[eia_df["series_name"].str.contains("Production", na=False)]
        cons = eia_df[eia_df["series_name"].str.contains("Consumption", na=False)]

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**生產趨勢（MMcf/月）**")
            if not prod.empty:
                st.dataframe(
                    prod[["date","value"]].rename(columns={"date":"日期","value":"MMcf"}),
                    use_container_width=True, hide_index=True
                )
        with col_b:
            st.markdown("**消費趨勢（MMcf/月）**")
            if not cons.empty:
                st.dataframe(
                    cons[["date","value"]].rename(columns={"date":"日期","value":"MMcf"}),
                    use_container_width=True, hide_index=True
                )

        # 供需差
        if not prod.empty and not cons.empty:
            st.markdown("**供需差計算**")
            p_latest = prod.sort_values("date").iloc[-1]
            c_latest = cons.sort_values("date").iloc[-1]
            gap = p_latest["value"] - c_latest["value"]
            col_x, col_y, col_z = st.columns(3)
            col_x.metric("生產量（最新月）", f"{p_latest['value']:,.0f} MMcf")
            col_y.metric("消費量（最新月）", f"{c_latest['value']:,.0f} MMcf")
            col_z.metric(
                "供需差",
                f"{gap:+,.0f} MMcf",
                delta="供給過剩" if gap > 0 else "供給不足"
            )


# ════════════════════════════════════════════════════
# TAB 3：股價追蹤
# ════════════════════════════════════════════════════
with tab3:
    if stocks_df.empty:
        st.warning("股價資料尚未載入，請先執行 `python fetcher.py --source yahoo`")
    else:
        st.markdown("#### 天然氣相關股票（標準化比較，起始=100）")

        available_tickers = stocks_df["ticker"].unique().tolist()
        selected_tickers = st.multiselect(
            "選擇股票", available_tickers,
            default=available_tickers,
            label_visibility="collapsed"
        )

        if selected_tickers:
            st.plotly_chart(
                make_stock_chart(stocks_df, selected_tickers),
                use_container_width=True
            )

        # 最新股價卡片
        st.markdown("#### 最新收盤價")
        ticker_cols = st.columns(len(available_tickers))
        for i, ticker in enumerate(available_tickers):
            sub = stocks_df[stocks_df["ticker"] == ticker].sort_values("date")
            if sub.empty:
                continue
            latest_close = sub["close"].iloc[-1]
            week_ago_sub = sub[sub["date"] <= sub["date"].iloc[-1] - timedelta(days=7)]
            if not week_ago_sub.empty:
                prev_close = week_ago_sub["close"].iloc[-1]
                chg_pct = (latest_close - prev_close) / prev_close * 100
                delta = f"{chg_pct:+.1f}%"
            else:
                delta = None
            with ticker_cols[i]:
                st.metric(label=ticker, value=f"{latest_close:.2f}", delta=delta)

        # Woodside 重點提示
        wds_data = stocks_df[stocks_df["ticker"].isin(["WDS", "WDS.AX"])].sort_values("date")
        if not wds_data.empty:
            st.divider()
            st.markdown("#### 🔍 Woodside Energy 重點")
            wds_nyse = stocks_df[stocks_df["ticker"] == "WDS"].sort_values("date")
            wds_asx  = stocks_df[stocks_df["ticker"] == "WDS.AX"].sort_values("date")

            col1, col2 = st.columns(2)
            with col1:
                if not wds_nyse.empty:
                    fig_w = go.Figure()
                    fig_w.add_trace(go.Scatter(
                        x=wds_nyse["date"], y=wds_nyse["close"],
                        mode="lines", line=dict(color="#22d3a5", width=2),
                        fill="tozeroy", fillcolor="rgba(34,211,165,0.06)",
                        name="WDS (NYSE)",
                    ))
                    fig_w.update_layout(
                        title="WDS (NYSE)",
                        plot_bgcolor="#0a0e17", paper_bgcolor="#0a0e17",
                        font=dict(color="#e2e8f0", size=11),
                        margin=dict(l=10, r=10, t=40, b=10),
                        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                    )
                    st.plotly_chart(fig_w, use_container_width=True)

            with col2:
                if not wds_asx.empty:
                    fig_wa = go.Figure()
                    fig_wa.add_trace(go.Scatter(
                        x=wds_asx["date"], y=wds_asx["close"],
                        mode="lines", line=dict(color="#34d399", width=2),
                        fill="tozeroy", fillcolor="rgba(52,211,153,0.06)",
                        name="WDS.AX (ASX)",
                    ))
                    fig_wa.update_layout(
                        title="WDS.AX (ASX)",
                        plot_bgcolor="#0a0e17", paper_bgcolor="#0a0e17",
                        font=dict(color="#e2e8f0", size=11),
                        margin=dict(l=10, r=10, t=40, b=10),
                        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                    )
                    st.plotly_chart(fig_wa, use_container_width=True)


# ════════════════════════════════════════════════════
# TAB 4：氣候因子
# ════════════════════════════════════════════════════
with tab4:
    if climate_df.empty:
        st.warning("氣候資料尚未載入，請先執行 `python fetcher.py --source climate`")
    else:
        st.markdown("#### HDD（暖氣度日數）— 數值越高代表越冷、天然氣需求越強")

        col_c1, col_c2 = st.columns(2)
        for region, col in [("US", col_c1), ("EU", col_c2)]:
            sub = climate_df[climate_df["region"] == region]
            if sub.empty:
                continue
            latest_hdd = sub.sort_values("date")["hdd"].iloc[-1]
            avg_hdd    = sub["hdd"].mean()
            with col:
                st.metric(
                    f"{region} 最新 HDD",
                    f"{latest_hdd:.1f}",
                    delta=f"vs 月均 {avg_hdd:.1f}",
                )

        st.markdown("#### 美歐 HDD 對比（近30日）")
        st.plotly_chart(make_hdd_chart(climate_df), use_container_width=True)

        st.markdown("#### HDD 與 HH 價格相關性")
        us_hdd = climate_df[climate_df["region"] == "US"][["date","hdd"]].copy()
        hh_price = prices_df[prices_df["market"] == "HH"][["date","price"]].copy()
        if not us_hdd.empty and not hh_price.empty:
            merged = pd.merge(us_hdd, hh_price, on="date", how="inner")
            if not merged.empty:
                fig_corr = px.scatter(
                    merged, x="hdd", y="price",
                    trendline="ols",
                    labels={"hdd": "HDD（美國）", "price": "HH 價格 (USD/MMBtu)"},
                    color_discrete_sequence=["#22d3a5"],
                )
                fig_corr.update_layout(
                    plot_bgcolor="#0a0e17", paper_bgcolor="#0a0e17",
                    font=dict(color="#e2e8f0", size=11),
                    margin=dict(l=10, r=10, t=20, b=10),
                    xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                    yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                )
                st.plotly_chart(fig_corr, use_container_width=True)
                corr = merged["hdd"].corr(merged["price"])
                st.markdown(
                    f"<span class='label-muted'>相關係數：</span>"
                    f"<b style='color:#22d3a5'>{corr:.3f}</b>",
                    unsafe_allow_html=True
                )
