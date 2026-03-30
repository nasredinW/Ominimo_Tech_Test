FROM apache/airflow:2.7.3-python3.9

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}" \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create application directories; prefer least-privilege (no chmod 777)
# Make them owned by airflow and group-writable (group 0) to play nicely with bind mounts.
RUN mkdir -p /app/src /app/metadata /app/data/input /app/data/output /app/logs /app/airflow /app/dags \
    && usermod -aG 0 airflow \
    && chown -R airflow:0 /app \
    && chmod -R g+rwX /app \
    && find /app -type d -exec chmod g+s {} + \
    && find /app/src -name "*.py" -exec chmod +x {} +

# Install PySpark as airflow user
USER airflow
RUN pip install pyspark boto3 botocore
RUN pip install apache-airflow-providers-amazon
