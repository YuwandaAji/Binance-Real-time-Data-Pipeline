# Binance Real-time Data Pipeline

A production-grade real-time data pipeline that streams live cryptocurrency market data from Binance WebSocket API, processes it with Apache Spark Structured Streaming, and visualizes it on an interactive dashboard.

---

## Architecture

```
Binance WebSocket API
        │
        ▼
  Python Producer
  (websocket-client)
        │
        ▼
  Apache Kafka
  ┌─────────────┐
  │ trades      │
  │ ticker      │
  └─────────────┘
        │
        ▼
Apache Spark
Structured Streaming
  - Window Aggregation
  - Watermarking
  - BUY/SELL Analysis
        │
        ▼
   PostgreSQL
  ┌──────────────────┐
  │ trades_summary   │
  │ ticker_latest    │
  └──────────────────┘
        │
        ▼
 Streamlit Dashboard
 (Auto-refresh 5s)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | Binance WebSocket API (public, no auth) |
| Message Broker | Apache Kafka 7.5.0 + Zookeeper |
| Stream Processing | Apache Spark Structured Streaming 3.5.1 |
| Storage | PostgreSQL 15 |
| Containerization | Docker + Docker Compose |
| Dashboard | Streamlit + Plotly |
| Language | Python 3.x |

---

## Features

- **Real-time ingestion** — live BTC, ETH, BNB, SOL market data via Binance WebSocket
- **Stream processing** — 1-minute window aggregation of trade volume and price
- **BUY vs SELL analysis** — real-time market sentiment tracking
- **Live dashboard** — auto-refreshing price cards, volume charts, and price history
- **Fault tolerance** — Kafka offset tracking + Spark watermarking for late data handling
- **Containerized** — entire infrastructure runs with a single `docker compose up`

---

## Getting Started

### Prerequisites
- Docker Desktop
- Python 3.8+
- Java 17 (required for PySpark)

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/binance-pipeline.git
cd binance-pipeline
```

### 2. Start infrastructure
```bash
docker compose up -d
```
This will start Kafka, Zookeeper, and PostgreSQL automatically.

### 3. Create Kafka topics
```bash
docker exec -it kafka bash
kafka-topics --create --topic trades --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --topic ticker --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
exit
```

### 4. Create PostgreSQL tables
```bash
docker exec -it postgres psql -U postgres -d ecommerce
```
```sql
CREATE TABLE trades_summary (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    symbol       VARCHAR(20),
    side         VARCHAR(10),
    total_volume DOUBLE PRECISION,
    avg_price    DOUBLE PRECISION,
    max_price    DOUBLE PRECISION,
    min_price    DOUBLE PRECISION
);

CREATE TABLE ticker_latest (
    symbol      VARCHAR(20),
    price       DOUBLE PRECISION,
    change_pct  DOUBLE PRECISION,
    high_24h    DOUBLE PRECISION,
    low_24h     DOUBLE PRECISION,
    volume_24h  DOUBLE PRECISION,
    event_time  TIMESTAMP
);
```

### 5. Install Python dependencies
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 6. Run the pipeline

Open 3 terminals:

**Terminal 1 — Start producer:**
```bash
python producer.py
```

**Terminal 2 — Start Spark job:**
```bash
python spark_job.py
```

**Terminal 3 — Start dashboard:**
```bash
streamlit run dashboard.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## Project Structure

```
binance-pipeline/
├── docker-compose.yml   # Kafka + Zookeeper + PostgreSQL
├── producer.py          # Binance WebSocket → Kafka
├── spark_job.py         # Kafka → Spark Streaming → PostgreSQL
├── dashboard.py         # Streamlit real-time dashboard
├── requirements.txt     # Python dependencies
└── README.md
```

---

## Key Concepts Demonstrated

- **Apache Kafka** — distributed message broker with topic partitioning
- **Spark Structured Streaming** — micro-batch processing with watermarking for late data
- **Window Aggregation** — 1-minute tumbling windows for OHLCV-style metrics
- **Docker Compose** — multi-container orchestration
- **Real-time dashboard** — live data visualization with auto-refresh

---

## Author

**Yuwanda Aji PAngestu** — Information Systems Student @ Universitas Negeri Surabaya

