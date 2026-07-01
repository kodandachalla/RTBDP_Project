# RTBDP Course Notes — Docker · Kafka · Flink

*A teaching companion to your project: **Real-Time Crypto Market Anomaly Detection**.*
Every concept below is tied to the real code you wrote (`docker-compose.yml`,
`producer/binance_producer.py`, `flink/crypto_processor.py`, `postgres/init.sql`).
Read it once top-to-bottom; then use the **Exam prep** section (Part 6) to self-test.

---

## Part 0 — The big picture first

### What does "real-time big data processing" actually mean?

Classic ("batch") data processing collects data into a big pile, then runs a job over the whole pile
(e.g. "compute yesterday's sales at midnight"). **Stream processing** flips this: data arrives
continuously and never ends, and you process each event *as it arrives*, producing results within
seconds.

Three properties define our problem (the "3 Vs" you should be able to recite):

- **Volume** — hundreds of trades per second; too much to "just store and query later".
- **Velocity** — the data is a never-ending stream; the job runs forever.
- **(Variety / Veracity)** — events can arrive late or out of order, so "correctness" needs care.

### The pipeline as a sentence

> A **producer** reads live trades and writes them to **Kafka**; **Flink** reads from Kafka,
> aggregates and detects anomalies, and writes results to **PostgreSQL**; **Grafana** reads
> PostgreSQL and draws a live dashboard. **Docker** packages and runs all of it.

```
Binance WS ──▶ Producer ──▶ Kafka ──▶ Flink ──▶ PostgreSQL ──▶ Grafana ──▶ Browser
 (source)     (ingestion)  (broker)  (processor)  (database)   (frontend)
```

This maps exactly onto the **course reference architecture**: *ingestion → broker → stream
processor → (database) → frontend*. Keep this diagram in your head — every concept below lives at
one of these arrows.

### Why each piece exists (the "why can't we skip it?" test)

| Piece | What breaks without it |
|---|---|
| **Kafka** | Producer and Flink would be tightly coupled; if Flink is slow/down, trades are lost. Kafka is a **buffer + decoupler + replay log**. |
| **Flink** | You'd have raw trades but no candles, no anomaly detection, no windows, no event-time correctness. Flink is the **brain**. |
| **PostgreSQL** | Grafana can't easily query a Kafka topic for "last hour of alerts". The DB gives **queryable, indexed, persisted results**. |
| **Grafana** | No human-facing output. It's the **frontend**. |
| **Docker** | "Works on my machine" hell: 5 services, specific Java/Python versions, networking. Docker makes it **one command, reproducible**. |

---

## Part 1 — Docker & Docker Compose

### 1.1 The problem Docker solves

Your Flink job needs **Java 17 + Python 3.11 + PyFlink 2.2 + a specific Kafka connector JAR**. Your
producer needs **Python 3.11 + websocket-client + confluent-kafka**. Installing all of that on a
laptop natively is fragile and non-reproducible (recall the course warning about Spark on Windows).

**Docker** packages an app *plus its entire environment* (OS libraries, runtime, dependencies) into
an **image**. A running instance of an image is a **container** — an isolated, lightweight process
that behaves identically on any machine with Docker.

> Analogy: an **image** is a class; a **container** is an object (instance) of that class. You
> `build` an image once, then `run` many containers from it.

### 1.2 Image vs container vs Dockerfile

- **Dockerfile** — the recipe. A text file of build steps.
- **Image** — the compiled, immutable result of building a Dockerfile (a stack of read-only layers).
- **Container** — a running image, with a thin writable layer on top.

Your `producer/Dockerfile`, annotated:

```dockerfile
FROM python:3.11-slim          # 1. base image: a minimal OS + Python 3.11
WORKDIR /app                   # 2. set the working directory inside the container
COPY producer/requirements.txt .   # 3. copy deps list FIRST (layer-caching trick, see 1.3)
RUN pip install --no-cache-dir -r requirements.txt   # 4. install dependencies
COPY producer/binance_producer.py .  # 5. copy the actual source code
CMD ["python", "binance_producer.py"]  # 6. the command run when the container starts
```

Each line is a **layer**. Layers are cached.

### 1.3 Why copy `requirements.txt` before the source code?

This is a classic exam-worthy detail you got right. Docker caches layers and **re-runs a layer only
if it or a layer above it changed**. If you copied the source code *before* installing dependencies,
then every time you edited one line of Python, Docker would re-run `pip install` (slow). By copying
`requirements.txt` first, the expensive `pip install` layer is reused as long as your dependencies
don't change — only the cheap `COPY source` layer re-runs.

### 1.4 The Flink Dockerfile teaches "native dependencies"

`flink/Dockerfile` is more involved because PyFlink is **Python wrapping Java**:

```dockerfile
FROM eclipse-temurin:17-jdk-jammy   # needs a full JDK — PyFlink compiles Java classes at runtime
# ... installs Python 3.11 via the deadsnakes PPA (Ubuntu 22.04 ships only 3.10) ...
RUN pip install --no-cache-dir -r requirements.txt
# downloads the Kafka connector JAR INTO PyFlink's own lib/ folder so Flink finds it automatically:
RUN wget ... flink-sql-connector-kafka-4.0.1-2.0.jar -O "$PYFLINK_LIB/..."
CMD ["python", "crypto_processor.py"]
```

Lesson: **a stream processor is a hybrid runtime.** The JAR is the bridge that lets the Java engine
talk to Kafka; without it the job dies at startup with "Could not find class KafkaSource".

### 1.5 Docker Compose — orchestrating many containers

One container is easy. You have **six** that must start in the right order and talk to each other.
`docker-compose.yml` is a single declarative file describing all services, their networks, volumes,
and dependencies. One command brings the whole system up:

```bash
docker compose up -d --build   # build images, then start all services detached
docker compose ps              # list services + health
docker compose logs -f flink-processor   # follow one service's logs
docker compose down            # stop (keep data);  add -v to also delete volumes
```

### 1.6 The three Compose concepts you must understand

**(a) Networking — services find each other by name.** Your compose file defines a bridge network
`crypto_net`. Every service on it can reach another using its **service name as a hostname**. That's
why your Flink config says:

```yaml
flink-processor:
  environment:
    KAFKA_BOOTSTRAP: "kafka:19092"   # "kafka" = the service name, resolves inside Docker
    PG_HOST: "postgres"              # "postgres" = the service name
```

> **The single most common beginner bug**, which your comments call out explicitly: inside a
> container you must use `kafka:19092`, **not** `localhost:9092`. `localhost` inside the producer
> container means *the producer container itself*, not the host machine. The host port mapping
> `9092:9092` is only for tools running on *your laptop*.

**(b) Volumes — data that survives restarts.** Containers are ephemeral; their writable layer is
deleted when removed. To persist Kafka logs and Postgres rows you mount **named volumes**:

```yaml
volumes: [kafka_data, postgres_data, grafana_data]
# ...
kafka:
  volumes:
    - kafka_data:/var/lib/kafka/data   # topic messages survive `docker compose down`
```

`docker compose down` keeps volumes; `docker compose down -v` **wipes** them (your troubleshooting
note about recreating the Postgres DB uses exactly this).

**(c) Startup ordering — `depends_on` + healthchecks.** Containers start almost simultaneously, but
Kafka takes ~30 s to be ready. Starting Flink before Kafka is up = crash. You solved this two ways:

```yaml
kafka:
  healthcheck:                       # Kafka is "healthy" only once it answers kafka-topics --list
    test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"]
    start_period: 30s
flink-processor:
  depends_on:
    kafka:    { condition: service_healthy }   # wait for Kafka's healthcheck to pass
    postgres: { condition: service_healthy }
    producer: { condition: service_started }   # just needs the topic-creating producer to have started
```

> **Subtle but important:** `depends_on` only controls *container start order*. It does **not**
> know your application is ready. A healthcheck does. And even with both, your Python code *also*
> polls (`wait_for_kafka`, `wait_for_kafka_topic`) — belt-and-suspenders, because distributed
> systems must never assume a dependency is up.

### ✅ Check yourself (Docker)
1. Why does `KAFKA_BOOTSTRAP` differ between the producer (`kafka:19092`) and a tool on your laptop (`localhost:9092`)?
2. What's the difference between `docker compose down` and `docker compose down -v`?
3. Why install dependencies before copying source code in a Dockerfile?

---

## Part 2 — Apache Kafka

### 2.1 What Kafka *is*

Kafka is a **distributed, append-only commit log** that you use as a **message broker**. Producers
*append* messages; consumers *read* them at their own pace. Think of it as a durable, replayable
tape that many readers can scan independently.

Why not just have the producer call Flink directly (HTTP)?

- **Decoupling** — producer and Flink don't need to be up at the same time or run at the same speed.
- **Buffering / back-pressure** — bursts are absorbed by Kafka instead of overwhelming Flink.
- **Replay** — because messages are retained, a restarted/ crashed consumer can re-read.
- **Fan-out** — multiple independent consumers (Flink, Kafka UI, a future ML job) read the same data.

### 2.2 The vocabulary (all visible in your project)

| Term | Meaning | In your project |
|---|---|---|
| **Broker** | A Kafka server that stores data and serves clients | the single `kafka` container |
| **Topic** | A named stream of messages (like a table/folder) | `raw-trades` |
| **Partition** | A topic is split into ordered partitions for parallelism | 6 partitions (one per symbol) |
| **Offset** | The position (0,1,2,…) of a message within a partition | committed by Flink's consumer group |
| **Producer** | Writes messages to a topic | `binance_producer.py` |
| **Consumer** | Reads messages from a topic | the Flink Kafka source |
| **Consumer group** | A set of consumers sharing the work, tracking offsets together | `flink-crypto-processor` |
| **Retention** | How long messages are kept before deletion | 2 hours |

### 2.3 Partitions — the heart of Kafka's scalability (and your ordering guarantee)

A topic is divided into **partitions**. Two facts you must internalize:

1. **Ordering is guaranteed only *within* a partition**, never across partitions.
2. **Each partition is consumed by at most one consumer in a group** — so partitions are the unit of
   parallelism.

You chose **6 partitions, one per symbol**, and pinned each symbol to a fixed partition:

```python
# producer/binance_producer.py
SYMBOL_PARTITION = {
    "BTCUSDT": 0, "ETHUSDT": 1, "BNBUSDT": 2,
    "SOLUSDT": 3, "DOGEUSDT": 4, "XRPUSDT": 5,
}
producer.produce(topic=TOPIC_NAME,
                 key=trade["symbol"].encode(),
                 value=json.dumps(trade).encode(),
                 partition=SYMBOL_PARTITION.get(trade["symbol"], 0),  # explicit partition
                 callback=delivery_report)
```

> **Why pin explicitly instead of letting Kafka hash the key?** With only 6 keys, hash collisions
> could dump two symbols into one partition and leave another empty. Explicit assignment guarantees a
> clean 1-symbol-per-partition layout — so all BTC trades stay strictly ordered, and the job can be
> parallelised up to 6 ways later. This is the reasoning the examiner wants to hear.

### 2.4 Creating a topic with intent (not auto-creation)

You **disabled** auto-topic-creation in compose (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`) so the
producer controls the topic's shape explicitly via the **Admin API**:

```python
new_topic = NewTopic(
    TOPIC_NAME,
    num_partitions=6,
    replication_factor=1,                 # single broker → can't replicate beyond 1
    config={"retention.ms": "7200000",    # keep 2 hours
            "cleanup.policy": "delete",
            "compression.type": "lz4"},
)
admin.create_topics([new_topic])
```

Lesson: leaving Kafka to "auto-create" gives you broker-default partition counts and retention —
fine for a toy, wrong for a designed system. **Explicit beats implicit.**

### 2.5 The Producer API — three details you got right

```python
PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "1",            # (a) durability vs speed
    "linger.ms": 20,        # (b) batching
    "batch.size": 65536,
    "compression.type": "lz4",
    "retries": 5,
}
```

- **(a) `acks`** — the durability dial. `acks=0` (fire-and-forget, fast, may lose data), `acks=1`
  (leader confirms — your choice, a balance), `acks=all` (all replicas confirm — safest, slowest).
  With a single broker, `acks=1` is the sensible maximum.
- **(b) `linger.ms` + `batch.size`** — instead of one network call per trade, wait up to 20 ms to
  fill a 64 KB batch. **Throughput optimization**: fewer, bigger requests. `lz4` compression then
  shrinks the batch on the wire and on disk.
- **(c) `poll()` after `produce()`** — `produce()` is *asynchronous*: it just enqueues. The actual
  send + delivery callbacks happen during `poll()`. Your code calls `producer.poll(0)` right after
  producing (the course hint "in Python, remember to call poll() or writing may hang").

```python
producer.produce(...); producer.poll(0)   # non-blocking: triggers delivery callbacks
# ...and on shutdown:
producer.flush(timeout=10)                 # block until all buffered messages are sent
```

### 2.6 Offsets and consumer groups — how Flink resumes after a crash

A **consumer group** tracks, per partition, the **offset** of the last message it processed. Your
Flink source uses a *fixed* group id:

```python
KAFKA_GROUP_ID = "flink-crypto-processor"   # fixed → restart resumes from last committed offset
KafkaSource.builder()
    .set_group_id(KAFKA_GROUP_ID)
    .set_starting_offsets(KafkaOffsetsInitializer.latest())  # only NEW messages on first run
```

- **Fixed group id** → a restarted job continues where it left off (doesn't reprocess everything,
  doesn't skip).
- `latest()` → on the *very first* start, ignore the backlog and read only new trades. (`earliest()`
  would replay all retained history — useful to reprocess after a logic change.)

### 2.7 KRaft mode (a "what's new" detail)

Older Kafka needed **ZooKeeper** (a separate cluster) to manage metadata and elect leaders. Modern
Kafka uses **KRaft** (Kafka Raft) — Kafka manages its own metadata, no ZooKeeper. Your single node
plays **both** roles:

```yaml
KAFKA_PROCESS_ROLES: "broker,controller"     # this one node is broker AND controller
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
```

If asked "why no ZooKeeper?": *KRaft simplifies the deployment to a single process and is the modern
default.*

### ✅ Check yourself (Kafka)
1. Kafka guarantees ordering within a ____ but not across ____. (fill in)
2. Why did you pin symbols to partitions instead of relying on key hashing?
3. What does a fixed consumer-group id give you when the Flink job restarts?
4. What does `acks=1` trade off, and why is it the max for your setup?

---

## Part 3 — Apache Flink (the core of the project)

Flink is the **stream processor** — it reads the unbounded trade stream from Kafka and turns it into
**OHLC candles** and **anomaly alerts**. This is where most of the course's hard concepts live.

### 3.1 The mental model: a dataflow graph

In Flink you don't write a loop. You **declare a graph of operators** (`source → map → filter →
keyBy → window/process → sink`). Nothing runs until `env.execute()`, which ships the graph to the
engine, which then streams data through it forever.

Your `main()` builds exactly this graph:

```python
raw_stream = env.from_source(kafka_source, watermark_strategy, "Binance Kafka Source")
trade_stream = raw_stream.map(ParseTradeMap(), output_type=...).filter(lambda t: t is not None)
keyed_stream = trade_stream.key_by(lambda trade: trade[0])     # key by symbol
keyed_stream.process(WashTradeDetector())
keyed_stream.process(PumpDumpDetector())
keyed_stream.process(VolumeSpikeDetector())
keyed_stream.window(TumblingEventTimeWindows.of(Time.minutes(1))).process(OHLCWindowFunction("1min"))
keyed_stream.window(TumblingEventTimeWindows.of(Time.minutes(5))).process(OHLCWindowFunction("5min"))
env.execute("Crypto Market Anomaly Detection Pipeline")        # <-- only here does data start flowing
```

Note the **fan-out**: one `keyed_stream` feeds five operators. Each receives every trade
independently.

### 3.2 Source, map, filter — getting clean typed records

- **Source**: `KafkaSource` reads JSON strings from `raw-trades`.
- **Map** (`ParseTradeMap`): converts each JSON string into a **typed tuple**
  `(symbol, price, qty, ts_ms)`. Why typed? Flink serializes data between operators and across the
  network; it needs to know the types (hence `output_type=Types.TUPLE([...])`).
- **Filter**: malformed messages return `None` from the map and are dropped by `.filter(lambda t:
  t is not None)` — so a single bad message never crashes the pipeline.

```python
class ParseTradeMap(MapFunction):
    def map(self, raw_json):
        try:
            d = json.loads(raw_json)
            return (d["symbol"], float(d["price"]), float(d["quantity"]), int(d["trade_time_ms"]))
        except (KeyError, ValueError, json.JSONDecodeError):
            return None   # dropped downstream by .filter()
```

### 3.3 Event time vs processing time (THE key streaming concept)

There are two clocks in stream processing:

- **Processing time** — the wall-clock time when Flink *happens* to handle an event. Easy but wrong:
  it depends on network delays, GC pauses, restarts.
- **Event time** — the time the event *actually happened*, carried inside the event.

For correct candles you must use **event time** — the exchange's trade timestamp — so a trade that
executed at 10:00:59 lands in the 10:00 candle even if it reaches Flink at 10:01:02.

```python
.with_timestamp_assigner(lambda trade, _: trade[3])   # trade[3] = exchange ts in ms = EVENT TIME
```

> If you used processing time, your candles would be subtly wrong whenever the network hiccuped, and
> two runs over the same data could produce different results. Event time makes results
> **deterministic and replayable** — a core course theme.

### 3.4 Watermarks — "how does Flink know a window is finished?"

If events can arrive late/out of order, when is it safe to *close* the 10:00–10:01 window and emit
the candle? Flink answers with **watermarks**: a watermark of time *T* is a promise "I don't expect
any more events with timestamp ≤ T." When the watermark passes a window's end, the window fires.

Your strategy:

```python
watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(3))  # (a) tolerate 3s of lateness
    .with_timestamp_assigner(lambda trade, _: trade[3])
    .with_idleness(Duration.of_seconds(10))                # (b) ignore idle partitions
)
```

- **(a) bounded out-of-orderness (3 s)** — Flink holds a window open 3 s past its end to catch
  slightly-late trades before sealing it. Bigger value = more correctness, more latency. Smaller =
  faster, but late events get dropped.
- **(b) idleness (10 s)** — this one is subtle and you hit it as a real bug. The global watermark is
  the **minimum** across all partitions. If BNB goes quiet, *its* watermark stops advancing, which
  would freeze the global watermark and **stall every symbol's windows**. `with_idleness(10s)` tells
  Flink "if a partition is silent for 10 s, ignore it when computing the watermark." This is in your
  Lessons Learned for a reason — examiners love it.

### 3.5 keyBy — partitioning the stream by symbol

```python
keyed_stream = trade_stream.key_by(lambda trade: trade[0])   # trade[0] = symbol
```

`key_by` reroutes the stream so all events with the same key go to the same operator instance, which
keeps **independent state per key**. After this line, BTC and DOGE are effectively separate
sub-streams — each detector tracks BTC history separately from DOGE history. This is what makes
"rolling average volume" meaningful *per symbol*.

### 3.6 Windows — turning an infinite stream into finite chunks

A **window** groups events by time so you can aggregate. You use **tumbling event-time windows**:
fixed-size, non-overlapping; every trade belongs to exactly one window.

```python
keyed_stream
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))     # [10:00,10:01), [10:01,10:02), ...
  .process(OHLCWindowFunction("1min"))
```

When the watermark passes a window's end, Flink calls `process(key, context, elements)` once with
**all trades in that window** for that symbol, and you compute the candle:

```python
def process(self, key, context, elements):
    trades = list(elements)
    prices = [t[1] for t in trades]
    open_price, close_price = prices[0], prices[-1]   # O = first, C = last (event order)
    high_price, low_price   = max(prices), min(prices)
    volume = sum(t[2] for t in trades)
    self.pg.write_ohlc(key, ..., open_price, high_price, low_price, close_price, volume, len(trades))
```

Window-type vocabulary worth knowing for the exam:

| Window | Shape | Example |
|---|---|---|
| **Tumbling** | fixed size, no overlap | your 1-min / 5-min candles |
| **Sliding** | fixed size, overlapping (slides by a step) | "5-min average updated every 1 min" |
| **Session** | gap-based, dynamic size | "group activity until 30s of silence" |

### 3.7 State — Flink's superpower

To detect a pump-and-dump you must *remember* the baseline price and the peak across many events.
Flink gives each keyed operator **managed state** that it persists and restores automatically.

Two state types you used:

- **`ValueState<T>`** — a single value per key (e.g. the current phase, the peak price).
- **`ListState<T>`** — a list per key (e.g. the rolling history of recent prices).

```python
# WashTradeDetector.open()
self.price_history = runtime_context.get_list_state(ListStateDescriptor("wash_prices", Types.FLOAT()))
self.vol_history   = runtime_context.get_list_state(ListStateDescriptor("wash_volumes", Types.FLOAT()))
```

> **Bounded state is mandatory.** An unbounded list grows until the job runs out of memory. You cap
> history to the last 60 entries, so memory stays constant no matter how long the job runs:
> ```python
> prices  = prices[-60:]     # keep ~30s of trades
> volumes = volumes[-60:]
> ```
> This is the "efficiency / scalability" point in the rubric.

### 3.8 The three CEP detectors, by mechanism

**Complex Event Processing (CEP)** = detecting *patterns over sequences of events*, not single
events. Each detector is a `KeyedProcessFunction` — it sees one event at a time, updates per-symbol
state, and emits an alert when a pattern completes.

**(a) Wash trade — a sliding statistical test (ListState).**
Keep the last ~60 prices and volumes; fire when price oscillation is high *and* current volume is
well above the rolling average *at the same time*:

```python
swing_pct    = (max(prices) - min(prices)) / min(prices) * 100
volume_ratio = quantity / (sum(volumes) / len(volumes))
if swing_pct >= 0.3 and volume_ratio >= 1.5:
    write WASH_TRADE alert
```

**(b) Pump & dump — a state machine (ValueState).**
This is true CEP: a *sequence* (rise → drop) within a time bound.

```
WATCHING ──(record baseline)──▶ PUMP_CONFIRMED ──(rise≥0.4% then drop≥0.3% within 4 min)──▶ ALERT, reset
                                              └──(4 min elapse, no dump)──▶ reset
```

```python
if phase == "PUMP_CONFIRMED":
    new_peak = max(peak_price, price)
    rise_pct = (new_peak - start_price) / start_price * 100
    drop_pct = (new_peak - price)       / new_peak     * 100
    if rise_pct >= 0.4 and drop_pct >= 0.3 and elapsed_ms <= 240_000:
        write PUMP_AND_DUMP alert; self._reset(price, ts_ms)
```

**(c) Volume spike — sliding count window + cooldown (ListState + ValueState).**
Count trades in the last 10 s; compare to a 10-minute rolling baseline; fire if ≥ 2× — but a 30 s
**cooldown** stops alert spam during sustained bursts:

```python
current = [t for t in current if ts_ms - t <= 10_000]   # slide the 10s window
volume_ratio = len(current) / (sum(history) / len(history))
if volume_ratio >= 2.0 and (ts_ms - last_alert) > 30_000:
    write VOLUME_SPIKE alert; self.last_alert_ts.update(ts_ms)
```

> **Why `open()` and not `__init__()` for the DB connection and state?** Flink operators are
> *serialized* and shipped to worker tasks. `__init__` runs on the client during graph construction;
> `open()` runs *on the task, after the network/runtime exist*. So state handles and the Postgres
> connection are created in `open()`. (Also note: each operator gets its **own** `PGWriter` because
> operators run as independent parallel tasks/threads.)

### 3.9 Checkpointing — fault tolerance

```python
env.get_checkpoint_config().set_checkpoint_interval(30_000)   # snapshot all state every 30s
```

A **checkpoint** is a consistent snapshot of *all* operator state + the Kafka offsets that produced
it, written to disk. If the job crashes, Flink restarts from the last checkpoint: state is restored
and Kafka is rewound to the matching offsets. Combined with the fixed consumer-group id, this gives
**exactly-the-right resume** instead of "replay everything" or "lose progress."

### 3.10 The sink — writing to PostgreSQL

Your operators write straight to Postgres via `psycopg2` (the course-recommended pattern when a DB
drives Grafana). Two design notes worth citing:

- **`autocommit = True`** so each insert is immediately visible to Grafana.
- **Upsert for the ticker** so there's exactly one "latest price" row per symbol (no unbounded growth):
  ```sql
  INSERT INTO price_ticker (...) VALUES (...)
  ON CONFLICT (symbol) DO UPDATE SET price = EXCLUDED.price, ...   -- keep ONE row per symbol
  ```
- **Store aggregates, not the raw stream** — only candles + alerts + latest price go to the DB. (The
  course hint: "do not dump the whole input stream.")

### ✅ Check yourself (Flink)
1. Difference between event time and processing time — and why candles need event time?
2. In one sentence, what is a watermark, and what does `with_idleness` prevent?
3. Why is `key_by(symbol)` necessary for per-symbol rolling averages to be correct?
4. Which state type would you use for "the peak price so far" vs "the last 60 prices"?
5. Why is the DB connection opened in `open()` rather than `__init__()`?
6. What exactly does a checkpoint snapshot, and how does restart use it?

---

## Part 4 — PostgreSQL & Grafana (sink + frontend, briefly)

**PostgreSQL** is the queryable results store. Your `init.sql` runs once on first boot (via
`/docker-entrypoint-initdb.d/`) and creates three tables, each shaped for how Grafana queries it:

- `ohlc_candles` — one row per (symbol, window); indexed on `(symbol, window_start DESC)` because
  Grafana always filters by symbol and recent time.
- `anomaly_events` — one row per detection (timestamp, symbol, pattern, severity, description).
- `price_ticker` — exactly one row per symbol (the upsert target).

> **Indexes** matter: without `idx_ohlc_symbol_window`, Grafana would full-scan the table every 5 s.
> With it, Postgres jumps straight to the rows. This is the "result correctness + efficiency" angle.

**Grafana** is the frontend. It connects to Postgres (auto-provisioned via the mounted
`grafana/provisioning/` files — no manual setup), and each panel is a **SQL query** with **dashboard
variables** (`$symbol`, `$candle_size`) substituted from dropdowns. It polls every 5 s and re-renders
— that's your "live" dashboard.

The whole frontend is therefore *just SQL over the results tables* — which is exactly why writing
clean aggregates (not raw trades) into well-indexed tables was the right design.

---

## Part 5 — Putting it together: trace one trade end-to-end

Follow a single BTC trade executed at `12:00:30.250`:

1. **Binance** emits `{s:"BTCUSDT", p:"64210.5", q:"0.01", T:1718...250}` on the WebSocket.
2. **Producer** parses it, and `produce()`s JSON to `raw-trades` **partition 0** (BTC's partition),
   keyed `BTCUSDT`; `poll(0)` flushes the delivery callback.
3. **Kafka** appends it to partition 0 at the next offset; it'll be retained for 2 hours.
4. **Flink source** consumes it (group `flink-crypto-processor`), assigns **event time** =
   `12:00:30.250`, and contributes to the **watermark**.
5. **map → filter** turns it into `("BTCUSDT", 64210.5, 0.01, ...)`; **`key_by`** routes it to the
   BTC instance of every downstream operator.
6. It updates the **1-min window** `[12:00,12:01)` and the **5-min window**, and is fed to all three
   **CEP detectors**, each updating BTC's state.
7. When the watermark passes `12:01:00 + 3s`, the 1-min window **fires** → an OHLC candle is computed
   and **INSERTed** into `ohlc_candles`; the ticker is upserted. If a detector's pattern completed,
   an alert is **INSERTed** into `anomaly_events`.
8. Within 5 s, **Grafana** polls Postgres and the new candle / alert appears on your dashboard.
9. Every 30 s, **Flink checkpoints** state + offsets, so a crash here resumes cleanly.

If you can narrate this trace from memory, you understand the project.

---

## Part 6 — Exam prep: likely questions & crisp answers

**Q: Why Kafka at all — why not produce straight into Flink?**
Decoupling, buffering against bursts, replay after failure, and fan-out to multiple consumers. Kafka
is the durable shock-absorber between ingestion and processing.

**Q: Why Flink over Spark here?** (course hint)
Flink suits complex aggregations and pattern matching (CEP) and chained aggregations, and behaves
better on Windows (no Hadoop `winutils`). Spark Structured Streaming would also work but is stronger
for ML/simple aggregations. For event-time CEP, Flink is the natural pick.

**Q: Event time vs processing time?**
Event time = when the trade happened (timestamp in the data); processing time = when Flink saw it.
Event time gives correct, deterministic, replayable windows despite network delay/out-of-order data.

**Q: What is a watermark? What problem does idleness solve?**
A watermark is Flink's estimate that "no events older than T remain," used to decide when to close
windows. The global watermark is the min across partitions; `with_idleness` stops a *quiet* partition
from freezing all windows.

**Q: How do you guarantee per-symbol ordering and parallelism?**
6 partitions, one symbol each, explicitly pinned by the producer; Kafka guarantees order within a
partition; Flink `key_by(symbol)` keeps independent state; parallelism can scale to 6.

**Q: How is the system fault tolerant / handles late data / out-of-order data?**
Late/out-of-order: event-time + 3 s bounded-out-of-orderness watermark. Faults: 30 s Flink
checkpoints of state + Kafka offsets, fixed consumer-group id, healthchecks + reconnect logic. Result
correctness: candles use event order; bounded state; cooldowns prevent duplicate alerts.

**Q: How does it scale? Is it efficient?**
Scale by raising Flink parallelism to the partition count (6) — no code change. Efficiency:
producer batching + lz4 compression; only compact aggregates stored in Postgres (never the raw
stream); indexed tables for fast Grafana queries; bounded operator state.

**Q: What would you improve?**
Express OHLC in Flink Table/SQL and use the CEP library's pattern API; run on a real Flink cluster
with parallelism = partitions; adaptive per-symbol thresholds; a schema registry; automated
replay-based tests.

**Q: Why Docker Compose, and what does `depends_on` actually guarantee?**
One-command reproducible multi-service startup with shared networking and persistent volumes.
`depends_on` only orders container *starts*; readiness needs healthchecks (and your code still polls).

---

### One-line summary to carry into the exam
> *Live trades → Kafka (decouple, partition per symbol) → Flink (event-time windows + stateful CEP,
> checkpointed) → PostgreSQL (compact, indexed results) → Grafana (SQL panels, 5 s refresh), all
> wired together and made reproducible by Docker Compose.*
