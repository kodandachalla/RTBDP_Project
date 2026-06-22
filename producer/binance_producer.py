# Binance WebSocket → Kafka Producer
#
# Startup sequence:
#   1. Wait until Kafka is reachable.
#   2. Create the "raw-trades" topic with custom settings (idempotent).
#   3. Open a Binance combined-stream WebSocket (no API key required).
#   4. Publish every incoming trade event as JSON to Kafka.
#   5. Reconnect automatically if the WebSocket drops.
#   6. Log throughput stats every 10 seconds.
#
# Usage:
#   python binance_producer.py            # production mode — writes to Kafka
#   python binance_producer.py --dry-run  # prints to console only, no Kafka

import json
import time
import logging
import signal
import sys
import argparse
import os
from datetime import datetime, timezone

import websocket
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")


# Connection details come from environment variables so the same image runs
# locally (falls back to localhost) and inside Docker (where docker-compose.yml
# sets KAFKA_BOOTSTRAP=kafka:19092 using the internal container hostname).
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_NAME        = "raw-trades"
TOPIC_PARTITIONS  = 6   # one partition per symbol; Flink can read all 6 in parallel
TOPIC_REPLICATION = 1   # single-broker cluster cannot replicate beyond 1
# Retain 2 hours of trades; older messages are automatically purged by Kafka.
TOPIC_RETENTION_MS = 7_200_000

# Six high-volatility pairs chosen to maximise anomaly detection events during
# a short demo.  Binance WebSocket requires lowercase symbol names.
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "dogeusdt", "xrpusdt"]

# Combined-stream URL opens one persistent WebSocket for all symbols simultaneously.
# The @trade suffix requests individual trade events (not order book or kline data).
WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)

# linger.ms + batch.size: wait up to 20 ms to fill a 64 KB batch before
# sending, reducing network round-trips at high throughput.
# lz4 compression cuts bandwidth and Kafka storage with negligible CPU overhead.
PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks":              "1",      # leader-only ack — balances speed vs durability
    "linger.ms":         20,
    "batch.size":        65536,
    "compression.type":  "lz4",
    "retries":           5,
    "retry.backoff.ms":  500,
}


class Stats:
    """Counts sent messages and prints throughput every 10 seconds."""

    def __init__(self):
        self.total   = 0
        self.errors  = 0
        self.window  = 0
        self.last_ts = time.time()

    def tick(self):
        self.total  += 1
        self.window += 1
        now = time.time()
        if now - self.last_ts >= 10:
            rate = self.window / (now - self.last_ts)
            log.info(f"[stats]  {self.total:>6,} total sent  |  {rate:>5.1f} msg/s  |  errors={self.errors}")
            self.window  = 0
            self.last_ts = now

    def error(self):
        self.errors += 1


stats = Stats()


def wait_for_kafka(bootstrap: str, retries: int = 30, delay: int = 5) -> None:
    """
    Block until Kafka is reachable, polling every `delay` seconds.

    Docker starts all containers roughly simultaneously even with depends_on.
    Kafka takes 20–30 seconds to elect a KRaft controller and become ready,
    so this guard prevents the producer from crashing on startup.
    """
    log.info(f"Waiting for Kafka at {bootstrap} ...")
    for attempt in range(1, retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 3000})
            admin.list_topics(timeout=3)
            log.info("✓ Kafka is ready!")
            return
        except Exception as exc:
            log.warning(f"  Attempt {attempt}/{retries} — not ready: {exc}")
            time.sleep(delay)
    raise RuntimeError(
        f"Kafka at {bootstrap} did not become ready after {retries} attempts. "
        "Make sure Docker is running: docker compose up -d"
    )


def create_topic_if_missing(bootstrap: str) -> None:
    """
    Create the 'raw-trades' topic with explicit settings if it doesn't exist yet.

    Auto-topic creation is disabled in docker-compose.yml so the producer always
    controls partition count and retention rather than inheriting broker defaults.
    """
    admin    = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics

    if TOPIC_NAME in existing:
        log.info(f"Topic '{TOPIC_NAME}' already exists — skipping creation.")
        return

    new_topic = NewTopic(
        TOPIC_NAME,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=TOPIC_REPLICATION,
        config={
            "retention.ms":     str(TOPIC_RETENTION_MS),
            "cleanup.policy":   "delete",
            "compression.type": "lz4",
        },
    )
    futures = admin.create_topics([new_topic])
    for topic, future in futures.items():
        future.result()  # raises immediately if topic creation failed
        log.info(f"✓ Created topic '{topic}'  (partitions={TOPIC_PARTITIONS}, retention=2h, compression=lz4)")


def delivery_report(err, msg):
    """
    Async callback invoked by the Kafka producer after each message is
    acknowledged or permanently fails.  Called from within producer.poll().
    """
    if err is not None:
        log.error(f"Delivery failed  topic={msg.topic()}  key={msg.key()}: {err}")
        stats.error()


def parse_binance_trade(raw: str) -> dict | None:
    """
    Convert a raw Binance WebSocket JSON string into a clean trade dict.

    Binance combined-stream format:
    {
      "stream": "btcusdt@trade",
      "data": {
        "e": "trade",         -- event type (always "trade" here)
        "s": "BTCUSDT",      -- symbol (uppercase)
        "t": 3482910,        -- trade ID
        "p": "43210.50",     -- price as STRING (preserves decimal precision)
        "q": "0.00123",      -- quantity as STRING
        "T": 1700000001000,  -- trade execution timestamp (Unix ms)
        "m": false           -- true = seller was aggressor, false = buyer
      }
    }

    Returns a cleaned dict with typed fields, or None for non-trade events
    (e.g. subscription confirmations) so the caller can skip them cleanly.
    """
    try:
        wrapper = json.loads(raw)
        # Combined streams nest the payload under "data"; single streams do not.
        data = wrapper.get("data", wrapper)

        if data.get("e") != "trade":
            return None

        return {
            "symbol":        data["s"],
            "trade_id":      data["t"],
            "price":         float(data["p"]),
            "quantity":      float(data["q"]),
            "trade_time_ms": data["T"],
            "trade_time":    datetime.fromtimestamp(
                                 data["T"] / 1000, tz=timezone.utc
                             ).isoformat(),
            "is_buyer_mm":   data["m"],
        }
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        log.debug(f"Parse error: {exc} — raw={raw[:80]}")
        return None


def make_on_message(producer: Producer | None, dry_run: bool):
    """
    Factory that closes over `producer` and `dry_run` to produce the
    WebSocketApp on_message callback (which only accepts ws and message args).

    Each symbol is assigned to a fixed Kafka partition rather than relying on
    hash-based routing because with only 6 keys, hash collisions could cause
    some partitions to receive two symbols while others receive none.
    """
    SYMBOL_PARTITION = {
        "BTCUSDT": 0, "ETHUSDT": 1, "BNBUSDT": 2,
        "SOLUSDT": 3, "DOGEUSDT": 4, "XRPUSDT": 5,
    }

    def on_message(ws, raw):
        trade = parse_binance_trade(raw)
        if trade is None:
            return

        if dry_run:
            log.info(
                f"[dry-run]  {trade['symbol']:<10}  "
                f"price={trade['price']:>12,.2f}  "
                f"qty={trade['quantity']:>12.6f}"
            )
            stats.tick()
            return

        producer.produce(
            topic     = TOPIC_NAME,
            key       = trade["symbol"].encode("utf-8"),
            value     = json.dumps(trade).encode("utf-8"),
            partition = SYMBOL_PARTITION.get(trade["symbol"], 0),
            callback  = delivery_report,
        )
        # poll(0) is non-blocking: it triggers any pending delivery_report
        # callbacks without waiting for new Kafka messages to arrive.
        producer.poll(0)
        stats.tick()

    return on_message


def on_error(ws, error):
    log.error(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    log.warning(f"WebSocket closed — code={close_status_code}  msg={close_msg}")


def on_open(ws):
    log.info(f"✓ WebSocket connected — streaming {[s.upper() for s in SYMBOLS]}")


# Module-level reference so the SIGINT handler can flush the producer without
# it being passed as an argument (signal handlers only accept sig and frame).
_producer_ref = None


def handle_sigint(sig, frame):
    """Flush buffered Kafka messages before exiting on Ctrl-C (SIGINT)."""
    log.info("Shutting down — flushing remaining messages ...")
    if _producer_ref is not None:
        _producer_ref.flush(timeout=10)
        log.info(f"✓ Flushed.  Total messages sent: {stats.total:,}")
    log.info("Goodbye!")
    sys.exit(0)


signal.signal(signal.SIGINT, handle_sigint)


def main():
    global _producer_ref

    parser = argparse.ArgumentParser(description="Binance WebSocket → Kafka producer")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print trades to console only — no Kafka connection",
    )
    args = parser.parse_args()

    producer = None

    if not args.dry_run:
        wait_for_kafka(KAFKA_BOOTSTRAP)
        create_topic_if_missing(KAFKA_BOOTSTRAP)
        producer      = Producer(PRODUCER_CONFIG)
        _producer_ref = producer
        log.info(f"✓ Kafka producer ready — writing to topic '{TOPIC_NAME}'")
    else:
        log.info("DRY-RUN mode — trades will be printed to console only")

    log.info(f"Connecting to Binance WebSocket ...")
    log.info(f"  Symbols: {[s.upper() for s in SYMBOLS]}")

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open    = on_open,
        on_message = make_on_message(producer, args.dry_run),
        on_error   = on_error,
        on_close   = on_close,
    )

    reconnect_delay = 5  # seconds to wait between reconnect attempts after a drop

    while True:
        try:
            # run_forever() blocks until the connection closes or errors out.
            ws.run_forever(
                ping_interval=20,  # send a keep-alive ping every 20 seconds
                ping_timeout=10,   # treat the connection as dead if pong takes > 10 s
            )
        except KeyboardInterrupt:
            handle_sigint(None, None)
        except Exception as exc:
            log.error(f"Unexpected WebSocket error: {exc}")

        if producer is not None:
            producer.flush(timeout=5)

        log.warning(f"Reconnecting in {reconnect_delay} seconds ...")
        time.sleep(reconnect_delay)


if __name__ == "__main__":
    main()
