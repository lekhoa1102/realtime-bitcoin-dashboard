# Real-Time Bitcoin Market Monitoring and Visualization System

Ung dung web truc quan hoa du lieu thi truong Bitcoin (BTC/USDT) theo thoi gian thuc,
su dung Binance Spot WebSocket Market Stream va Streamlit.

## Muc tieu

He thong tiep nhan cac trade event BTC/USDT truc tiep tu Binance qua WebSocket,
xu ly va gom nhom du lieu theo thoi gian (OHLC 1 giay), sau do hien thi tren
dashboard tu dong cap nhat. Ung dung phuc vu muc dich giam sat va truc quan hoa
thi truong (monitoring), khong dua ra khuyen nghi mua/ban.

## Kien truc

```
Binance Spot WebSocket (wss://stream.binance.com:9443/ws/btcusdt@aggTrade)
        |
        v
Python background thread (websocket-client)
        |
        v
In-memory buffer (deque, thread-safe)
        |
        v
Data Processing: parsing, validation, time-based aggregation (pandas resample)
        |
        v
OHLC Calculation (Open / High / Low / Close moi 1 giay)
        |
        v
Real-Time Metrics (current price, % change, connection status)
        |
        v
Visualization (Plotly: Candlestick + Scatter activity)
        |
        v
Streamlit Web Application (auto-refresh moi 1 giay)
```

## Cong nghe su dung

- Python 3
- Streamlit (web framework, deploy tren Streamlit Community Cloud)
- websocket-client (ket noi Binance WebSocket, tu dong reconnect)
- pandas (xu ly du lieu, resample OHLC)
- plotly (candlestick chart + scatter trade activity)
- Binance Spot Market Data (public, khong can API key)

## Cau truc thu muc

```
realtime-bitcoin-dashboard/
|-- app.py            # Toan bo logic: WebSocket, xu ly du lieu, dashboard
|-- requirements.txt   # Danh sach thu vien
|-- README.md          # Tai lieu mo ta
```

## Chay local

```bash
pip install -r requirements.txt
streamlit run app.py
```

Sau do mo trinh duyet tai `http://localhost:8501`.

## Deploy (Streamlit Community Cloud)

1. Push repo nay len GitHub (public).
2. Vao https://share.streamlit.io, dang nhap bang GitHub.
3. Chon "New app", tro toi repo nay va file `app.py`.
4. Deploy. Streamlit Cloud se tu build va cap URL dang `https://<ten-app>.streamlit.app`.

## Link ung dung da deploy

`https://<dien-sau-khi-deploy>.streamlit.app`

## Ghi chu ky thuat

- Du lieu la giao dich BTC/USDT thuc te tu Binance, khong phai du lieu lich su replay.
- `st.cache_resource` dam bao chi co MOT ket noi WebSocket duy nhat cho toan bo
  server process, tranh mo lai ket noi moi lan Streamlit rerun script.
- WebSocket tu dong reconnect neu mat ket noi (delay 3 giay giua cac lan thu).
- Bo dem trong bo nho (`deque`, toi da 500 trade) de kiem soat dung luong RAM.
