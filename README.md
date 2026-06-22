# Real-Time Crypto Market Anomaly Detection & Dashboard

A fully Dockerised, end-to-end **real-time streaming pipeline** that monitors live cryptocurrency
trades and flags market-manipulation patterns as they happen. The entire stack starts with a single
`docker compose up -d --build` command.

```
Binance WebSocket → Python producer → Kafka → PyFlink → PostgreSQL → Grafana
```

The pipeline:

- Reads live trade data from the **Binance WebSocket** (free, no API key required)
- Publishes trades to **Apache Kafka** (6 partitions, one per symbol)
- Processes them with **PyFlink** — 1-min & 5-min OHLC tumbling windows + 3 CEP anomaly detectors
- Stores results in **PostgreSQL** (3 tables)
- Visualises everything on a live **Grafana** dashboard (auto-refreshes every 5 seconds)

**Symbols monitored:** BTCUSDT · ETHUSDT · BNBUSDT · SOLUSDT · DOGEUSDT · XRPUSDT

**Anomaly patterns detected (Flink CEP):**

| Pattern | Detection logic | Real-world meaning |
|---|---|---|
| `WASH_TRADE` | price swing ≥ 0.3% over a rolling 30 s window **and** volume ≥ 1.5× average | self-dealing to inflate apparent volume |
| `PUMP_AND_DUMP` | rise ≥ 0.4% from baseline, then drop ≥ 0.3% from peak, within 4 minutes | coordinated ramp-up then sell-off |
| `VOLUME_SPIKE` | trades in last 10 s ≥ 2× the 10-min rolling average (30 s cooldown) | flash crash / coordinated activity |

---

## Architecture

| Layer | Technology | Role |
|---|---|---|
| Data source | Binance WebSocket | live, key-free stream of individual trades |
| Ingestion | Python producer (`websocket-client`, `confluent-kafka`) | normalises trades to JSON, produces to Kafka |
| Broker | Apache Kafka 3.7 (KRaft, no ZooKeeper) | `raw-trades` topic, 6 partitions, 2-hour retention |
| Stream processor | Apache Flink 2.2 (PyFlink DataStream) | event-time OHLC windows + 3 stateful CEP detectors |
| Database | PostgreSQL 15 | `ohlc_candles`, `anomaly_events`, `price_ticker` |
| Frontend | Grafana 10.2 | live dashboard, 5-second auto-refresh |
| Monitoring | Kafka UI | topics, partitions, consumer offsets |

A detailed architecture diagram and the full write-up are in
[`RTBDP_Report_Kodanda_Challa.docx`](RTBDP_Report_Kodanda_Challa.docx).

---

## Prerequisites

The **only** requirement is **Docker Desktop** installed and running — everything else (Java, Python,
PyFlink, Kafka, PostgreSQL, Grafana) runs inside containers.

- Download: https://www.docker.com/products/docker-desktop
- Verify it is available:
  ```bash
  docker --version # Docker version 29.5.3, build d1c06ef
  docker compose version # Docker Compose version v5.1.4
  ```
- An internet connection is required on the **first** run to pull base images and build the custom
  images (downloads PyFlink + the Kafka connector JAR). First build ≈ 5–10 minutes; subsequent
  starts take under 30 seconds (images are cached).

---

## How to run — one command

```bash
# 1. clone and enter the repository
cd RTBDP_Project

# 2. build images and start all 6 services in dependency order
docker compose up -d --build

# 3. wait ~2–3 minutes, then confirm all containers are up
docker compose ps
```

Expected services: `kafka` (healthy), `postgres` (healthy), `grafana`, `kafka-ui`, `producer`,
`flink-processor`.

### View the dashboard

| URL | What | Login |
|---|---|---|
| http://localhost:3000 | **Grafana dashboard** (loads automatically) | `admin` / `admin` |
| http://localhost:8080 | **Kafka UI** — topics & live messages | none |

> **Note:** OHLC candles appear after the first 1-minute window closes (wait 1–2 minutes).
> Anomaly alerts typically appear within 5–10 minutes of startup.

Use the **Symbol** dropdown and **candle size** (1min / 5min) variables at the top of the dashboard
to drill into any pair, and the time picker (top-right) to change the range.

### Stop

```bash
docker compose down        # stop, keep data
docker compose down -v     # stop and wipe all data (clean reset)
docker compose up -d       # restart fast (no rebuild)
```

---

## Inspecting the data (optional)

```bash
# list tables
docker exec postgres psql -U flink -d crypto_market -c "\dt"

# current price for every symbol (latest ticker)
docker exec postgres psql -U flink -d crypto_market -c \
  "SELECT symbol, price, price_change_pct, volume_1min, updated_at \
   FROM price_ticker ORDER BY symbol;"

# latest OHLC candles
docker exec postgres psql -U flink -d crypto_market -c \
  "SELECT symbol, window_size, open_price, high_price, low_price, close_price, trade_count \
   FROM ohlc_candles ORDER BY created_at DESC LIMIT 10;"

# anomalies in the last hour
docker exec postgres psql -U flink -d crypto_market -c \
  "SELECT symbol, pattern_type, severity, description FROM anomaly_events \
   WHERE detected_at >= NOW() - INTERVAL '1 hour' ORDER BY detected_at DESC LIMIT 20;"
```

Logs:

```bash
docker compose logs -f            # all services
docker logs producer --tail 30    # trade ingestion + throughput stats
docker logs flink-processor --tail 30   # OHLC + CEP alerts
```


---

## Project structure

```
RTBDP_Project/
├── docker-compose.yml                        6 services, one-command startup
├── README.md                                 this file
├── postgres/
│   └── init.sql                              creates the 3 tables on first boot
├── grafana/
│   └── provisioning/
│       ├── datasources/postgres.yml          auto-connects Grafana to PostgreSQL
│       └── dashboards/
│           ├── dashboards.yml                dashboard provider config
│           └── crypto_dashboard.json         the complete dashboard definition
├── producer/
│   ├── Dockerfile                            python:3.11-slim + websocket + kafka
│   ├── requirements.txt
│   └── binance_producer.py                   Binance WebSocket → Kafka publisher
└── flink/
    ├── Dockerfile                            eclipse-temurin:17 + Python 3.11 + PyFlink
    ├── requirements.txt
    └── crypto_processor.py                   Kafka → OHLC windows + CEP → PostgreSQL

```

---

## How it works — key design choices

- **Event time, not arrival time.** Trades are timestamped by the exchange. A bounded-out-of-orderness
  watermark (3 s) absorbs late events before a window is sealed, and source idleness (10 s) stops a
  quiet pair from freezing all windows.
- **Partition per symbol.** The producer pins each symbol to a fixed Kafka partition, preserving
  per-symbol ordering for the detectors and giving a direct path to horizontal scaling (raise Flink
  parallelism to match the partition count).
- **Bounded state.** Each CEP detector keeps per-symbol Flink keyed state (rolling price/volume
  history, or a pump-and-dump state machine), explicitly capped so memory stays constant.
- **Fault tolerance.** Flink checkpoints all operator state to disk every 30 s and the Kafka consumer
  commits offsets under a fixed group ID, so a crash resumes from the last checkpoint.
- **Store aggregates, not the raw stream.** Only compact OHLC candles and alert rows are written to
  PostgreSQL; the raw trade stream is never dumped to the database.
- **Self-healing startup.** Health checks gate dependent services; the producer and processor poll for
  Kafka readiness and topic existence; the WebSocket auto-reconnects and flushes on shutdown.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `docker: command not found` | Docker Desktop isn't installed/running. |
| `postgres` shows unhealthy | `docker compose down -v` then `docker compose up -d --build` (wipes the old volume). |
| Dashboard shows "No data" | Wait 2–3 min — the first OHLC candle appears only after the first window closes. Check `docker compose ps`. |
| Port already in use (5432 / 9092 / 3000 / 8080) | Another app is using it (e.g. a local PostgreSQL or Grafana). Stop it, or change the port in `docker-compose.yml`. |
| `flink-processor` exits immediately | The `raw-trades` topic doesn't exist yet (producer must start first). `docker compose restart flink-processor`. |
| Build fails with a network error | First build downloads ~2–3 GB. Check your connection and retry `docker compose up -d --build`. |