"""
Real-Time Bitcoin Market Monitoring and Visualization System
BTC/USDT live data via Binance Spot WebSocket -> Streamlit dashboard.

Pipeline:
Binance WebSocket -> Real-Time Data Processing -> Time-based Aggregation (OHLC)
-> Real-Time Metrics -> Visualization -> Web Application
"""

import json
import threading
import time
from collections import deque
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import websocket  # websocket-client

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
BUFFER_MAXLEN = 1000
REFRESH_SECONDS = 2          # redraw every 2s instead of 1s -> less flicker
OHLC_FREQ = "1S"
VISIBLE_CANDLES = 60         # only show the most recent N candles (like a live 1-min window)

COLOR_UP = "#16A34A"
COLOR_DOWN = "#DC2626"
COLOR_BG = "#F8FAFC"

st.set_page_config(
    page_title="Real-Time BTC/USDT Dashboard",
    page_icon="\U0001F4C8",
    layout="wide",
)

# --------------------------------------------------------------------------
# WEBSOCKET STREAM (runs once per server process, in a background thread)
# --------------------------------------------------------------------------


class BinanceStream:
    def __init__(self, maxlen=BUFFER_MAXLEN):
        self.trades = deque(maxlen=maxlen)
        self.connection_status = {"connected": False, "last_update": None}
        self._ws = None
        self._thread = None
        self._lock = threading.Lock()
        self._stop = False

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            trade = {
                "time": datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc),
                "price": float(data["p"]),
                "quantity": float(data["q"]),
                "trade_id": data["a"],
            }
            with self._lock:
                self.trades.append(trade)
                self.connection_status["last_update"] = datetime.now(timezone.utc)
        except Exception as e:
            print("Parse error:", e)

    def _on_open(self, ws):
        print("WebSocket connected.")
        self.connection_status["connected"] = True

    def _on_close(self, ws, close_status_code, close_msg):
        print("WebSocket closed:", close_status_code, close_msg)
        self.connection_status["connected"] = False

    def _on_error(self, ws, error):
        print("WebSocket error:", error)
        self.connection_status["connected"] = False

    def _run_forever(self):
        while not self._stop:
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    on_message=self._on_message,
                    on_open=self._on_open,
                    on_close=self._on_close,
                    on_error=self._on_error,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                print("Reconnect after error:", e)
            if not self._stop:
                time.sleep(3)

    def start(self):
        if self._thread is None or not self._thread.is_alive():
            self._stop = False
            self._thread = threading.Thread(target=self._run_forever, daemon=True)
            self._thread.start()

    def get_trades_df(self):
        with self._lock:
            data = list(self.trades)
        if not data:
            return pd.DataFrame(columns=["time", "price", "quantity", "trade_id"])
        return pd.DataFrame(data)


@st.cache_resource
def get_stream():
    """
    st.cache_resource ensures ONE BinanceStream instance (and one background
    thread) lives for the whole server process, shared across all viewers
    and reruns -- instead of opening a new WebSocket connection every time
    Streamlit reruns the script (which happens every refresh).
    """
    s = BinanceStream()
    s.start()
    return s


# --------------------------------------------------------------------------
# DATA PROCESSING
# --------------------------------------------------------------------------


def compute_ohlc(df: pd.DataFrame, freq: str = OHLC_FREQ) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    d = df.copy().set_index("time").sort_index()
    ohlc = d["price"].resample(freq).ohlc()
    ohlc["volume"] = d["quantity"].resample(freq).sum()
    ohlc = ohlc.dropna(subset=["open", "high", "low", "close"])
    return ohlc


def compute_kpis(df: pd.DataFrame, ohlc_df: pd.DataFrame) -> dict:
    kpis = {
        "current_price": None,
        "price_change_pct": None,
        "price_change_abs": None,
        "is_up": None,
        "trade_count": len(df),
    }
    if df.empty:
        return kpis

    current_price = df["price"].iloc[-1]
    kpis["current_price"] = current_price

    if not ohlc_df.empty:
        first_open = ohlc_df["open"].iloc[0]
        change_abs = current_price - first_open
        kpis["price_change_abs"] = change_abs
        kpis["price_change_pct"] = (change_abs / first_open) * 100
        kpis["is_up"] = change_abs >= 0

    return kpis


def get_recent_transactions(df: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Time", "Price", "Quantity", "Trade ID"])
    recent = df.tail(n).sort_values("time", ascending=False).copy()
    recent["Time"] = recent["time"].dt.strftime("%H:%M:%S")
    recent["Price"] = recent["price"].map(lambda x: f"${x:,.2f}")
    recent["Quantity"] = recent["quantity"].map(lambda x: f"{x:.6f}")
    recent["Trade ID"] = recent["trade_id"]
    return recent[["Time", "Price", "Quantity", "Trade ID"]]


# --------------------------------------------------------------------------
# VISUALIZATION
# --------------------------------------------------------------------------


def plot_candlestick_with_volume(ohlc_df: pd.DataFrame) -> go.Figure:
    """
    Binance/TradingView-style layout: candlestick on top, volume bars
    below, sharing the same X axis. Only the most recent VISIBLE_CANDLES
    are drawn so each redraw stays light and the chart doesn't grow
    heavier (and slower) the longer the app has been running.
    """
    view = ohlc_df.tail(VISIBLE_CANDLES)

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.03,
    )

    fig.add_trace(
        go.Candlestick(
            x=view.index,
            open=view["open"],
            high=view["high"],
            low=view["low"],
            close=view["close"],
            increasing_line_color=COLOR_UP,
            decreasing_line_color=COLOR_DOWN,
            increasing_fillcolor=COLOR_UP,
            decreasing_fillcolor=COLOR_DOWN,
            name="Price",
        ),
        row=1,
        col=1,
    )

    bar_colors = [COLOR_UP if c >= o else COLOR_DOWN for o, c in zip(view["open"], view["close"])]
    fig.add_trace(
        go.Bar(
            x=view.index,
            y=view["volume"],
            marker_color=bar_colors,
            name="Volume",
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        title=f"BTC/USDT - Last {VISIBLE_CANDLES}s",
        template="plotly_white",
        showlegend=False,
        xaxis_rangeslider_visible=False,
        height=520,
        margin=dict(l=10, r=10, t=40, b=10),
        bargap=0.15,
    )
    fig.update_yaxes(title_text="Price (USDT)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_xaxes(title_text="Time (UTC)", row=2, col=1)

    return fig


# --------------------------------------------------------------------------
# APP LAYOUT
# --------------------------------------------------------------------------

st.title("\U0001F4C8 Real-Time Bitcoin Market Dashboard")
st.caption("BTC/USDT - LIVE MARKET DATA (Binance Spot WebSocket)")

stream = get_stream()


@st.fragment(run_every=REFRESH_SECONDS)
def render_dashboard():
    """
    Only this fragment reruns every REFRESH_SECONDS -- the rest of the page
    (title, layout) stays untouched, and each chart keeps a FIXED key so
    Plotly updates its data in place instead of tearing down and rebuilding
    the whole chart every tick. This is what removes the flicker/lag.
    """
    df = stream.get_trades_df()
    ohlc_df = compute_ohlc(df)
    kpis = compute_kpis(df, ohlc_df)
    connected = stream.connection_status["connected"]

    # ---- KPI CARDS ----
    col1, col2, col3 = st.columns(3)

    with col1:
        price_text = f"${kpis['current_price']:,.2f}" if kpis["current_price"] else "—"
        st.metric("CURRENT BTC PRICE", price_text)

    with col2:
        if kpis["price_change_pct"] is not None:
            trend_icon = "\U0001F7E2" if kpis["is_up"] else "\U0001F534"
            st.metric(
                "PRICE CHANGE",
                f"{trend_icon} {kpis['price_change_pct']:+.4f}%",
                delta=f"{kpis['price_change_abs']:+.2f} USDT",
            )
        else:
            st.metric("PRICE CHANGE", "—")

    with col3:
        status_text = "\U0001F7E2 LIVE" if connected else "\U0001F534 DISCONNECTED"
        st.metric("CONNECTION", status_text)

    st.divider()

    # ---- CANDLESTICK + VOLUME (Binance-style, shared x-axis) ----
    if not ohlc_df.empty:
        st.plotly_chart(plot_candlestick_with_volume(ohlc_df), use_container_width=True, key="candle_chart")
    else:
        st.info("Waiting for enough data to build candlesticks...")

    st.divider()

    # ---- RECENT TRANSACTIONS ----
    st.subheader("Recent Transactions")
    st.dataframe(get_recent_transactions(df, n=15), use_container_width=True, hide_index=True, key="recent_tx")


render_dashboard()