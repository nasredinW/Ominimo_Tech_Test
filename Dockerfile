FROM apache/airflow:2.7.3-python3.9

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER root

# Create application directories with proper permissions
RUN mkdir -p /app/src \
    && mkdir -p /app/metadata \
    && mkdir -p /app/data/input \
    && mkdir -p /app/data/output/events \
    && mkdir -p /app/data/output/discards \
    && mkdir -p /app/data/output/clients \
    && mkdir -p /app/airflow/logs/pipeline \
    && mkdir -p /app/logs \
    && chmod -R 777 /app

# Entrypoint script to initialize permissions on startup
RUN printf '#!/bin/bash\n# Ensure mounted volumes have correct permissions\nchmod -R 777 /app/data 2>/dev/null || true\nchmod -R 777 /app/metadata 2>/dev/null || true\nchmod -R 777 /app/src 2>/dev/null || true\nchmod -R 777 /app/airflow 2>/dev/null || true\n# Start the main process\nexec "$@"\n' > /entrypoint.sh && chmod +x /entrypoint.sh

# Install PySpark as airflow user
USER airflow
RUN pip install --no-cache-dir pyspark

USER root
