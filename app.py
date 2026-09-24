"""
Real-Time Bitcoin Market Monitoring and Visualization System
BTC/USD live data via Coinbase Advanced Trade WebSocket -> Streamlit dashboard.

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

WS_URL = "wss://advanced-trade-ws.coinbase.com"
BUFFER_MAXLEN = 1000
REFRESH_SECONDS = 2          # redraw every 2s instead of 1s -> less flicker
OHLC_FREQ = "1S"
VISIBLE_CANDLES = 60         # only show the most recent N candles (like a live 1-min window)

COLOR_UP = "#16A34A"
COLOR_DOWN = "#DC2626"
COLOR_BG = "#F8FAFC"

st.set_page_config(
    page_title="Real-Time BTC/USD Dashboard",
    page_icon="\U0001F4C8",
    layout="wide",
)

# --------------------------------------------------------------------------
# WEBSOCKET STREAM (runs once per server process, in a background thread)
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# WEBSOCKET STREAM
# --------------------------------------------------------------------------

class CoinbaseStream:
    def __init__(self):
        self.trades = deque(maxlen=BUFFER_MAXLEN)

        self.connection_status = {
            "connected": False,
            "error": None
        }

        self._thread = None
        self._lock = threading.Lock()
        self._stop = False

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)

            # Chỉ xử lý market_trades
            if data.get("channel") != "market_trades":
                return

            events = data.get("events", [])

            for event in events:
                trades = event.get("trades", [])

                for trade in trades:
                    try:
                        timestamp = pd.to_datetime(
                            trade["time"],
                            utc=True
                        )

                        price = float(trade["price"])
                        quantity = float(trade["size"])

                        trade_id = trade.get("trade_id")
                        side = trade.get("side", "UNKNOWN")

                        record = {
                            "time": timestamp,
                            "price": price,
                            "quantity": quantity,
                            "trade_id": trade_id,
                            "side": side
                        }

                        with self._lock:
                            self.trades.append(record)

                    except Exception as e:
                        print(
                            f"Trade parsing error: {e}",
                            flush=True
                        )

        except Exception as e:
            print(
                f"Message parsing error: {e}",
                flush=True
            )

    def _on_open(self, ws):
        print(
            "Connected to Coinbase WebSocket.",
            flush=True
        )

        self.connection_status["connected"] = True
        self.connection_status["error"] = None

        # Subscribe vào market_trades của BTC-USD
        subscribe_message = {
            "type": "subscribe",
            "channel": "market_trades",
            "product_ids": ["BTC-USD"]
        }

        ws.send(json.dumps(subscribe_message))

        print(
            "Subscribed to BTC-USD market trades.",
            flush=True
        )

    def _on_error(self, ws, error):
        self.connection_status["connected"] = False
        self.connection_status["error"] = str(error)

        print(
            f"WebSocket error: {error}",
            flush=True
        )

    def _on_close(self, ws, close_status_code, close_msg):
        self.connection_status["connected"] = False

        print(
            f"WebSocket closed: "
            f"{close_status_code} - {close_msg}",
            flush=True
        )

    def _run_forever(self):
        while not self._stop:

            try:
                print(
                    "Connecting to Coinbase...",
                    flush=True
                )

                ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )

                ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10
                )

            except Exception as e:
                self.connection_status["connected"] = False
                self.connection_status["error"] = str(e)

                print(
                    f"WebSocket worker exception: {e}",
                    flush=True
                )

            if not self._stop:
                time.sleep(3)

    def start(self):
        if self._thread is None or not self._thread.is_alive():

            print(
                "Starting Coinbase WebSocket worker...",
                flush=True
            )

            self._thread = threading.Thread(
                target=self._run_forever,
                daemon=True
            )

            self._thread.start()

    def get_trades_df(self):
        with self._lock:
            data = list(self.trades)

        if not data:
            return pd.DataFrame(
                columns=[
                    "time",
                    "price",
                    "quantity",
                    "trade_id",
                    "side"
                ]
            )

        return pd.DataFrame(data)

    def get_status(self):
        return self.connection_status.copy()

@st.cache_resource
def get_stream():
    s = CoinbaseStream()
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
        title=f"BTC/USD - Last {VISIBLE_CANDLES}s",
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
st.caption("BTC/USD - LIVE MARKET DATA (Coinbase Advanced Trade WebSocket)")

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
    status = stream.get_status()

    connected = status["connected"]
    if status["error"]:
        st.warning(
            f"WebSocket error: {status['error']}"
        )
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