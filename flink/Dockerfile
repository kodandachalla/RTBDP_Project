# =============================================================================
# FLINK DOCKERFILE — PyFlink Stream Processor
# Builds when you run: docker compose up -d --build
# =============================================================================

# eclipse-temurin:17-jdk-jammy provides Java 17 on Ubuntu 22.04.
# A full JDK (not JRE) is required because PyFlink compiles operator classes at runtime.
FROM eclipse-temurin:17-jdk-jammy

# Ubuntu 22.04 ships Python 3.10; apache-flink 2.2.0 requires 3.11 (3.12 is unsupported).
# deadsnakes PPA is the standard Ubuntu source for out-of-band Python releases.
RUN apt-get update && apt-get install -y \
    software-properties-common \
    wget \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    # python3.11-distutils is needed by pip; python3-pip bootstraps the pip binary.
    && apt-get install -y python3.11 python3.11-distutils python3-pip \
    # Remove apt cache to keep the image layer small.
    && rm -rf /var/lib/apt/lists/*

# Point the system python / python3 symlinks at 3.11 so pip and PyFlink pick
# it up without requiring explicit version suffixes in scripts or commands.
RUN update-alternatives --install /usr/bin/python  python  /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Fail fast during build if Java or Python versions are not what the code expects.
RUN java -version && python --version

# Set the working directory; all subsequent COPY and RUN paths are relative to this.
WORKDIR /app

# Copy requirements first so Docker caches the pip layer — a source-only change
# won't re-run pip install on rebuild.
COPY flink/requirements.txt .

# Install Python dependencies without caching downloaded wheels to save image space.
RUN pip install --no-cache-dir -r requirements.txt

# Download the Flink Kafka connector JAR into PyFlink's own lib/ folder.
# Without this JAR the job fails at startup with "Could not find class KafkaSource".
# Placing it inside pyflink/lib/ lets PyFlink locate it automatically at runtime.
RUN PYFLINK_LIB=$(python -c "import pyflink, os; print(os.path.join(os.path.dirname(pyflink.__file__), 'lib'))") \
    && wget -q \
       "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar" \
       -O "$PYFLINK_LIB/flink-sql-connector-kafka-4.0.1-2.0.jar" \
    # Print the downloaded file size to confirm the JAR is not corrupted / truncated.
    && echo "Kafka JAR downloaded: $(ls -lh $PYFLINK_LIB/flink-sql-connector-kafka-4.0.1-2.0.jar)"

# Copy the stream-processing script that reads from Kafka and writes to PostgreSQL.
COPY flink/crypto_processor.py .

# Start the PyFlink job; the script runs as a standalone mini-cluster (no separate
# JobManager / TaskManager processes needed for this single-job setup).
CMD ["python", "crypto_processor.py"]
