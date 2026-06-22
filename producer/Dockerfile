# =============================================================================
# PRODUCER DOCKERFILE — Binance WebSocket → Kafka Producer
# Builds when you run: docker compose up -d --build
# =============================================================================

# python:3.11-slim chosen over 3.12 because PyFlink (used elsewhere in this
# stack) has dependency issues on 3.12.  The "slim" variant omits build tools
# and man pages, keeping the image ~50 MB vs ~300 MB for the full image.
FROM python:3.11-slim

# Set the working directory; all subsequent COPY and RUN paths are relative to this.
WORKDIR /app

# Copy requirements before source code so Docker can cache the pip install layer.
# A code-only change won't re-trigger pip install on rebuild.
COPY producer/requirements.txt .

# Install dependencies without caching downloaded wheels to save image space.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer script that opens a Binance WebSocket and publishes trades to Kafka.
COPY producer/binance_producer.py .

# Run the producer; it stays alive indefinitely, streaming trade events into Kafka.
CMD ["python", "binance_producer.py"]
