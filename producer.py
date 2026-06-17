import json
import time
import websocket
from kafka import KafkaProducer

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

print("Kafka Producer connected!")

# === HANDLER WEBSOCKET ===

def on_message(ws, message):
    data = json.loads(message)
    stream = data.get('stream', '')
    payload = data.get('data', data)

    if 'trade' in stream:
        # Format data trade
        event = {
            "symbol":    payload['s'],        # Contoh: BTCUSDT
            "price":     float(payload['p']), # Harga transaksi
            "quantity":  float(payload['q']), # Jumlah yang ditransaksikan
            "side":      "BUY" if payload['m'] == False else "SELL",
            "timestamp": payload['T'],        # Waktu transaksi (ms)
            "trade_id":  payload['t']
        }
        producer.send('trades', key=event['symbol'], value=event)
        print(f"[TRADE] {event['symbol']} | {event['side']} | "
              f"${event['price']:,.2f} | qty: {event['quantity']}")

    elif 'ticker' in stream:
        # Format data ticker (harga terkini + statistik 24 jam)
        event = {
            "symbol":        payload['s'],
            "price":         float(payload['c']),  # Harga terakhir
            "change_pct":    float(payload['P']),  # % perubahan 24 jam
            "high_24h":      float(payload['h']),  # Harga tertinggi 24 jam
            "low_24h":       float(payload['l']),  # Harga terendah 24 jam
            "volume_24h":    float(payload['v']),  # Volume 24 jam
            "timestamp":     payload['E']
        }
        producer.send('ticker', key=event['symbol'], value=event)
        print(f"[TICKER] {event['symbol']} | "
              f"${event['price']:,.2f} | {event['change_pct']:+.2f}%")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔌 WebSocket closed — reconnecting in 5 detik...")
    time.sleep(5)
    start()

def on_open(ws):
    print("WebSocket connected ke Binance!")

# === MULAI KONEKSI ===

def start():
    # Subscribe ke 4 coin sekaligus: BTC, ETH, BNB, SOL
    streams = [
        "btcusdt@trade", "ethusdt@trade",
        "bnbusdt@trade", "solusdt@trade",
        "btcusdt@ticker", "ethusdt@ticker",
        "bnbusdt@ticker", "solusdt@ticker"
    ]
    stream_url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)

    ws = websocket.WebSocketApp(
        stream_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    start()