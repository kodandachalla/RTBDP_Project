# PyFlink stream processor — Crypto Market Anomaly Detection
#
# Pipeline topology:
#   Kafka "raw-trades"
#       → ParseTradeMap          (JSON string → typed tuple)
#       → filter(not None)       (drop malformed messages)
#       → key_by(symbol)         (one independent sub-stream per trading pair)
#           ├── WashTradeDetector    → anomaly_events  (PostgreSQL)
#           ├── PumpDumpDetector     → anomaly_events  (PostgreSQL)
#           ├── VolumeSpikeDetector  → anomaly_events  (PostgreSQL)
#           ├── 1-min OHLC window    → ohlc_candles    (PostgreSQL)
#           └── 5-min OHLC window    → ohlc_candles    (PostgreSQL)
#
# Start everything: docker compose up -d --build

import json
import logging
import time
import os
from datetime import datetime, timezone

import psycopg2
from pyflink.common import WatermarkStrategy, Duration, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import (
    MapFunction,
    ProcessWindowFunction,
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("flink-processor")


# All connection details come from environment variables so the same image works
# locally (falls back to localhost defaults) and inside Docker (where
# docker-compose.yml injects container hostnames like "kafka" and "postgres").

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = "raw-trades"
# A fixed group ID means a restarted Flink job resumes from the last committed
# Kafka offset instead of replaying from scratch.
KAFKA_GROUP_ID  = "flink-crypto-processor"

PG_HOST     = os.getenv("PG_HOST",     "127.0.0.1")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "crypto_market")
PG_USER     = os.getenv("PG_USER",     "flink")
PG_PASSWORD = os.getenv("PG_PASSWORD", "flink_password")

# Detection thresholds are deliberately sensitive so anomalies fire within
# minutes on volatile meme coins (DOGE, XRP) during a live demo.
# Production thresholds would require hours of data to trigger.
WASH_PRICE_SWING_PCT    = 0.3   # % price oscillation across the rolling 30-sec window
WASH_VOLUME_MULTIPLIER  = 1.5   # current trade volume must exceed this × rolling avg
PUMP_RISE_PCT           = 0.4   # % rise from baseline required to enter pump phase
PUMP_DROP_PCT           = 0.3   # % drop from peak required to confirm the dump phase
VOLUME_SPIKE_MULTIPLIER = 2.0   # trades-per-10s must exceed this × 10-min rolling avg


# Each Flink operator (OHLC window function, CEP detector) creates its own
# PGWriter inside open() rather than sharing one, because Flink operators run
# as independent parallel tasks in separate threads.

class PGWriter:
    """Manages a single PostgreSQL connection and all INSERT/UPSERT statements."""

    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD,
        )
        # autocommit=True makes every INSERT immediately visible to Grafana
        # without requiring an explicit conn.commit() after each write.
        self.conn.autocommit = True
        log.info("[postgres] Connected")

    def ensure_connected(self):
        """Re-open the connection if it was lost (e.g. PostgreSQL restarted)."""
        if self.conn is None or self.conn.closed:
            self.connect()

    def write_ohlc(self, symbol, window_start, window_end, window_size,
                   open_price, high_price, low_price, close_price,
                   trade_volume, trade_count):
        """Insert one OHLC candle produced by OHLCWindowFunction."""
        self.ensure_connected()
        sql = """
            INSERT INTO ohlc_candles (
                symbol, window_start, window_end, window_size,
                open_price, high_price, low_price, close_price,
                trade_volume, trade_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                symbol, window_start, window_end, window_size,
                open_price, high_price, low_price, close_price,
                trade_volume, trade_count,
            ))

    def write_anomaly(self, symbol, detected_at, pattern_type, severity,
                      price_start, price_end, price_change_pct,
                      volume_ratio, description):
        """
        Insert one anomaly alert written by a CEP detector.
        pattern_type: 'WASH_TRADE' | 'PUMP_AND_DUMP' | 'VOLUME_SPIKE'
        severity:     'MEDIUM' | 'HIGH'
        """
        self.ensure_connected()
        sql = """
            INSERT INTO anomaly_events (
                symbol, detected_at, pattern_type, severity,
                price_start, price_end, price_change_pct,
                volume_ratio, description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                symbol, detected_at, pattern_type, severity,
                price_start, price_end, price_change_pct,
                volume_ratio, description,
            ))

    def write_ticker(self, symbol, price, change_pct, volume):
        """
        Upsert the latest price into price_ticker.
        ON CONFLICT keeps exactly one row per symbol so Grafana stat panels
        always display the most recent price without row accumulation.
        """
        self.ensure_connected()
        sql = """
            INSERT INTO price_ticker (
                symbol, price, price_change_pct, volume_1min, updated_at
            ) VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (symbol) DO UPDATE SET
                price            = EXCLUDED.price,
                price_change_pct = EXCLUDED.price_change_pct,
                volume_1min      = EXCLUDED.volume_1min,
                updated_at       = NOW()
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (symbol, price, change_pct, volume))


# First operator in the pipeline after the Kafka source.
# Converts raw JSON strings into typed tuples so Flink can serialise data
# efficiently between operators.
#
# Input  (Kafka message): '{"symbol":"BTCUSDT","price":43210.5,...}'
# Output (typed tuple):   ("BTCUSDT", 43210.5, 0.00123, 1700000001000)

class ParseTradeMap(MapFunction):
    """Deserializes a Kafka JSON message into a (symbol, price, qty, ts_ms) tuple."""

    def map(self, raw_json: str):
        try:
            d = json.loads(raw_json)
            return (
                d["symbol"],
                float(d["price"]),
                float(d["quantity"]),
                int(d["trade_time_ms"]),
            )
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            log.warning(f"[parse] Bad message skipped: {exc}")
            # Return None so the downstream .filter() drops the trade cleanly
            # without the pipeline ever seeing a partially-parsed record.
            return None


# Flink calls process() once per (symbol, window) pair when the window closes,
# passing all trades that arrived inside that window.
#
# Tumbling windows are fixed-size and non-overlapping — every trade belongs to
# exactly one window.  Event time (the timestamp inside the trade) is used
# rather than wall-clock time so late-arriving messages land in the correct candle.

class OHLCWindowFunction(ProcessWindowFunction):
    """Computes an OHLC candle from all trades in a closed tumbling window."""

    def __init__(self, window_label: str):
        # "1min" or "5min" — stored in the DB so Grafana can filter by candle size.
        self.window_label = window_label
        self.pg = None

    def open(self, runtime_context):
        # Create the DB connection inside open() (not __init__) so it is
        # established after Flink has initialised the task and the network is available.
        self.pg = PGWriter()
        self.pg.connect()

    def process(self, key: str, context, elements):
        """
        key      = trading pair symbol, e.g. "BTCUSDT"
        context  = window metadata (start/end timestamps in Unix ms)
        elements = all (symbol, price, qty, ts_ms) tuples inside this window
        """
        trades = list(elements)
        if not trades:
            return

        prices = [t[1] for t in trades]
        qtys   = [t[2] for t in trades]

        open_price  = prices[0]
        close_price = prices[-1]
        high_price  = max(prices)
        low_price   = min(prices)
        volume      = sum(qtys)
        trade_count = len(trades)

        # context.window().start/end are Unix milliseconds; PostgreSQL expects
        # timezone-aware datetime objects, so we convert explicitly.
        window_start = datetime.fromtimestamp(context.window().start / 1000, tz=timezone.utc)
        window_end   = datetime.fromtimestamp(context.window().end   / 1000, tz=timezone.utc)

        try:
            self.pg.write_ohlc(
                key, window_start, window_end, self.window_label,
                open_price, high_price, low_price, close_price,
                volume, trade_count,
            )
            change_pct = ((close_price - open_price) / open_price * 100) if open_price > 0 else 0.0
            self.pg.write_ticker(key, close_price, change_pct, volume)
            log.info(
                f"[{self.window_label}] {key}  "
                f"O={open_price:.2f}  H={high_price:.2f}  "
                f"L={low_price:.2f}  C={close_price:.2f}  "
                f"vol={volume:.4f}  n={trade_count}"
            )
        except Exception as exc:
            log.error(f"[ohlc] DB write failed: {exc}")

        # process() must be a Python generator per Flink's API contract.
        # We yield nothing because output goes directly to PostgreSQL, not
        # to a downstream Flink operator.
        return
        yield


# Wash trading: a trader repeatedly buys and sells the same asset to themselves
# to create artificial volume and make the coin appear actively traded.
#
# Detection logic (rolling 30-second window, ~60 trade entries):
#   Fire an alert when BOTH conditions hold simultaneously:
#     1. Price oscillation (high − low) / low  ≥ WASH_PRICE_SWING_PCT
#     2. Current trade quantity ≥ WASH_VOLUME_MULTIPLIER × rolling avg quantity
#
# Flink ListState persists the rolling history per symbol key and survives
# restarts because Flink checkpoints state to disk every 30 seconds.

class WashTradeDetector(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.pg = PGWriter()
        self.pg.connect()
        # Separate state lists per symbol; Flink persists them between calls.
        self.price_history = runtime_context.get_list_state(
            ListStateDescriptor("wash_prices", Types.FLOAT())
        )
        self.vol_history = runtime_context.get_list_state(
            ListStateDescriptor("wash_volumes", Types.FLOAT())
        )

    def process_element(self, trade, ctx):
        symbol, price, quantity, ts_ms = trade

        prices  = list(self.price_history.get() or [])
        volumes = list(self.vol_history.get()   or [])

        prices.append(price)
        volumes.append(quantity)

        # Cap at 60 entries to keep state memory bounded (~30 seconds of trades).
        prices  = prices[-60:]
        volumes = volumes[-60:]

        self.price_history.update(prices)
        self.vol_history.update(volumes)

        # Require at least 20 data points before evaluating to avoid false
        # positives on the first few trades after job startup.
        if len(prices) < 20:
            return

        high      = max(prices)
        low       = min(prices)
        swing_pct = ((high - low) / low * 100) if low > 0 else 0.0

        avg_vol      = sum(volumes) / len(volumes)
        volume_ratio = quantity / avg_vol if avg_vol > 0 else 0.0

        if swing_pct >= WASH_PRICE_SWING_PCT and volume_ratio >= WASH_VOLUME_MULTIPLIER:
            detected_at = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            severity    = "HIGH" if swing_pct > 1.5 else "MEDIUM"
            description = (
                f"Price swung {swing_pct:.2f}% "
                f"(high={high:.2f}, low={low:.2f}) "
                f"with volume {volume_ratio:.1f}x average "
                f"across last {len(prices)} trades"
            )
            try:
                self.pg.write_anomaly(
                    symbol, detected_at, "WASH_TRADE", severity,
                    low, high, swing_pct, volume_ratio, description,
                )
                log.warning(f"[CEP] WASH_TRADE {symbol}: {description}")
            except Exception as exc:
                log.error(f"[CEP] wash trade DB error: {exc}")


# Pump & dump: coordinated buyers drive price up quickly (pump) then sell all
# at once while retail buyers are still entering (dump), leaving them with an
# asset that immediately crashes back down.
#
# Two-phase state machine per symbol:
#
#   WATCHING       → record baseline price; move to PUMP_CONFIRMED immediately
#   PUMP_CONFIRMED → track peak price; if rise ≥ PUMP_RISE_PCT AND subsequent
#                    drop ≥ PUMP_DROP_PCT AND all within 4 minutes → fire alert
#                    → reset state machine.
#                    If the 4-minute window expires without a dump → reset only.

class PumpDumpDetector(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.pg = PGWriter()
        self.pg.connect()
        # Four ValueState objects track the state machine variables per symbol.
        self.phase = runtime_context.get_state(
            ValueStateDescriptor("pd_phase", Types.STRING())
        )
        self.start_price = runtime_context.get_state(
            ValueStateDescriptor("pd_start_price", Types.FLOAT())
        )
        self.start_time = runtime_context.get_state(
            ValueStateDescriptor("pd_start_time", Types.LONG())
        )
        self.peak_price = runtime_context.get_state(
            ValueStateDescriptor("pd_peak_price", Types.FLOAT())
        )

    def _reset(self, price, ts_ms):
        """Restart the state machine with the current price as the new baseline."""
        self.phase.update("WATCHING")
        self.start_price.update(price)
        self.start_time.update(ts_ms)
        self.peak_price.update(price)

    def process_element(self, trade, ctx):
        symbol, price, quantity, ts_ms = trade

        # .value() returns None until state has been written; fall back to safe defaults.
        phase       = self.phase.value()       or "WATCHING"
        start_price = self.start_price.value() or price
        start_time  = self.start_time.value()  or ts_ms
        peak_price  = self.peak_price.value()  or price

        elapsed_ms = ts_ms - start_time

        if phase == "WATCHING":
            # Capture baseline and immediately begin tracking the potential pump.
            self.start_price.update(price)
            self.start_time.update(ts_ms)
            self.peak_price.update(price)
            self.phase.update("PUMP_CONFIRMED")

        elif phase == "PUMP_CONFIRMED":
            new_peak = max(peak_price, price)
            self.peak_price.update(new_peak)

            rise_pct = ((new_peak - start_price) / start_price * 100) if start_price > 0 else 0.0
            drop_pct = ((new_peak - price) / new_peak * 100)           if new_peak > 0     else 0.0

            if rise_pct >= PUMP_RISE_PCT and drop_pct >= PUMP_DROP_PCT and elapsed_ms <= 240_000:
                detected_at = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                severity    = "HIGH" if (rise_pct + drop_pct) > 3.0 else "MEDIUM"
                description = (
                    f"Pump +{rise_pct:.2f}% to {new_peak:.2f}, "
                    f"then dump -{drop_pct:.2f}% to {price:.2f} "
                    f"in {elapsed_ms / 1000:.0f}s"
                )
                try:
                    self.pg.write_anomaly(
                        symbol, detected_at, "PUMP_AND_DUMP", severity,
                        start_price, price, -drop_pct,
                        rise_pct / drop_pct if drop_pct > 0 else 0.0,
                        description,
                    )
                    log.warning(f"[CEP] PUMP_AND_DUMP {symbol}: {description}")
                except Exception as exc:
                    log.error(f"[CEP] pump dump DB error: {exc}")

                self._reset(price, ts_ms)

            elif elapsed_ms > 240_000:
                # Pattern didn't complete within 4 minutes — reset without firing.
                self._reset(price, ts_ms)


# A sudden burst of trades far above baseline can signal a flash crash, a whale
# executing a large order, coordinated manipulation, or breaking news.
#
# Detection logic:
#   1. Count trades that arrived in the last 10 seconds (sliding count window).
#   2. Maintain a rolling 10-minute history of those counts (max 60 snapshots).
#   3. Alert when current 10-sec count ≥ VOLUME_SPIKE_MULTIPLIER × rolling avg.
#   4. Enforce a 30-second cooldown per symbol to avoid flooding the alerts
#      table during sustained high-volume periods.

class VolumeSpikeDetector(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.pg = PGWriter()
        self.pg.connect()
        # One trade-count snapshot per ~10 seconds; capped at 60 entries = 10 min baseline.
        self.count_history = runtime_context.get_list_state(
            ListStateDescriptor("vs_counts", Types.LONG())
        )
        # Timestamps of trades within the current 10-second sliding window.
        self.current_window_ts = runtime_context.get_list_state(
            ListStateDescriptor("vs_window_ts", Types.LONG())
        )
        # Timestamp of the last alert; used to enforce the 30-second cooldown.
        self.last_alert_ts = runtime_context.get_state(
            ValueStateDescriptor("vs_last_alert", Types.LONG())
        )

    def process_element(self, trade, ctx):
        symbol, price, quantity, ts_ms = trade

        # Slide the 10-second window forward by dropping timestamps older than 10 s.
        current = list(self.current_window_ts.get() or [])
        current.append(ts_ms)
        current = [t for t in current if ts_ms - t <= 10_000]
        self.current_window_ts.update(current)
        current_count = len(current)

        history = list(self.count_history.get() or [])
        history.append(current_count)
        history = history[-60:]  # keep the rolling 10-minute baseline
        self.count_history.update(history)

        # Require at least 1 minute of history (6 snapshots × 10 s) before
        # evaluating to avoid false positives immediately after job startup.
        if len(history) < 6:
            return

        avg_count    = sum(history) / len(history)
        volume_ratio = current_count / avg_count if avg_count > 0 else 0.0
        last_alert   = self.last_alert_ts.value() or 0
        cooldown_ok  = (ts_ms - last_alert) > 30_000

        if volume_ratio >= VOLUME_SPIKE_MULTIPLIER and cooldown_ok:
            self.last_alert_ts.update(ts_ms)
            detected_at = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            severity    = "HIGH" if volume_ratio > 5.0 else "MEDIUM"
            description = (
                f"{current_count} trades in last 10s "
                f"vs avg {avg_count:.1f} "
                f"({volume_ratio:.1f}x normal — 10-min rolling baseline)"
            )
            try:
                self.pg.write_anomaly(
                    symbol, detected_at, "VOLUME_SPIKE", severity,
                    None, price, None, volume_ratio, description,
                )
                log.warning(f"[CEP] VOLUME_SPIKE {symbol}: {description}")
            except Exception as exc:
                log.error(f"[CEP] volume spike DB error: {exc}")


def wait_for_kafka_topic(bootstrap: str, topic: str,
                         retries: int = 30, delay: int = 5) -> None:
    """
    Block until the Kafka topic exists, polling every `delay` seconds.

    The "raw-trades" topic is created by the producer on first start.
    Docker starts all services roughly simultaneously, so the Flink processor
    can reach this point before the producer has created the topic.
    KafkaSource would fail immediately if the topic doesn't exist yet.
    """
    from confluent_kafka.admin import AdminClient

    log.info(f"Waiting for Kafka topic '{topic}' ...")
    admin = AdminClient({"bootstrap.servers": bootstrap})

    for attempt in range(1, retries + 1):
        try:
            if topic in admin.list_topics(timeout=5).topics:
                log.info(f"✓ Topic '{topic}' found — starting Flink pipeline")
                return
            log.warning(f"  [{attempt}/{retries}] Topic not found yet — is the producer running?")
        except Exception as exc:
            log.warning(f"  [{attempt}/{retries}] Kafka not reachable: {exc}")
        time.sleep(delay)

    raise RuntimeError(
        f"Topic '{topic}' was not found after {retries} attempts. "
        "Make sure the producer container is running."
    )


def main():
    """
    Build and submit the Flink streaming pipeline.

    All .map() / .filter() / .key_by() / .window() / .process() calls only
    construct a logical pipeline graph — no data flows yet.  env.execute()
    compiles the graph, starts the Flink mini-cluster, and blocks indefinitely
    while the streaming job runs.
    """
    wait_for_kafka_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    env = StreamExecutionEnvironment.get_execution_environment()

    # PyFlink is a Python wrapper around Java; it needs the Kafka connector JAR
    # to communicate with Kafka.  The JAR is downloaded into PyFlink's own lib/
    # folder during the Docker build (see flink/Dockerfile), so we locate it
    # dynamically rather than hardcoding a path that may differ per environment.
    import pyflink
    kafka_jar = os.path.join(
        os.path.dirname(pyflink.__file__), "lib",
        "flink-sql-connector-kafka-4.0.1-2.0.jar",
    )
    # add_jars() requires a file URI; replace backslashes for Windows compatibility.
    jar_uri = "file:///" + kafka_jar.replace("\\", "/").replace(" ", "%20")
    env.add_jars(jar_uri)

    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # Parallelism 1 keeps state management simple for a single-machine deployment.
    # In production, set this to the number of Kafka partitions.
    env.set_parallelism(1)
    # Save all operator state to disk every 30 s so a crash can resume from the
    # last checkpoint instead of replaying all of Kafka history.
    env.get_checkpoint_config().set_checkpoint_interval(30_000)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        # latest() skips messages produced before this job started.
        # Switch to earliest() to reprocess all retained trades on restart.
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Watermarks advance the event-time clock so Flink knows when to close windows.
    # bounded_out_of_orderness(3s) waits up to 3 seconds for late-arriving messages
    # before sealing a window, handling minor network jitter.
    # with_idleness(10s) prevents a quiet Kafka partition (e.g. BNBUSDT during
    # low activity) from stalling the global watermark and blocking all windows.
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(3))
        .with_timestamp_assigner(lambda trade, _: trade[3])  # trade[3] = ts_ms
        .with_idleness(Duration.of_seconds(10))
    )

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="Binance Kafka Source",
    )

    trade_stream = (
        raw_stream
        .map(
            ParseTradeMap(),
            # Explicit output_type is required so Flink can serialise tuples
            # when shipping them between operators across the network.
            output_type=Types.TUPLE([
                Types.STRING(),  # symbol
                Types.FLOAT(),   # price
                Types.FLOAT(),   # quantity
                Types.LONG(),    # trade_time_ms
            ]),
        )
        .filter(lambda t: t is not None)
        .name("Parse JSON trades")
    )

    # key_by() partitions the stream by symbol so every downstream operator
    # maintains independent state for BTCUSDT, ETHUSDT, DOGEUSDT, etc.
    keyed_stream = trade_stream.key_by(lambda trade: trade[0])

    # Each CEP detector reads from keyed_stream independently.
    keyed_stream.process(WashTradeDetector()).name("Wash Trade CEP")
    keyed_stream.process(PumpDumpDetector()).name("Pump and Dump CEP")
    keyed_stream.process(VolumeSpikeDetector()).name("Volume Spike CEP")

    # 1-minute candles: used by Grafana for the short-term price chart.
    (
        keyed_stream
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(OHLCWindowFunction("1min"), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
        .name("1-min OHLC Windows")
    )

    # 5-minute candles: Grafana lets the user toggle between 1-min and 5-min
    # granularity via a dashboard variable (WHERE window_size = $candle_size).
    (
        keyed_stream
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .process(OHLCWindowFunction("5min"), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
        .name("5-min OHLC Windows")
    )

    log.info("=" * 60)
    log.info("  Crypto Market Anomaly Detection — Flink Pipeline")
    log.info("=" * 60)
    log.info(f"  Kafka:      {KAFKA_BOOTSTRAP}  topic={KAFKA_TOPIC}")
    log.info(f"  PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    log.info(f"  Windows:    1-min and 5-min OHLC tumbling windows")
    log.info(f"  CEP:        WASH_TRADE | PUMP_AND_DUMP | VOLUME_SPIKE")
    log.info("=" * 60)

    # Submits the pipeline graph to the mini-cluster and blocks until cancelled.
    env.execute("Crypto Market Anomaly Detection Pipeline")


if __name__ == "__main__":
    main()
