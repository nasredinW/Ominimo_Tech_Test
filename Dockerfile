FROM python:3.9

RUN apt-get update \
	&& apt-get install -y --no-install-recommends openjdk-21-jre-headless \
	&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN pip install --no-cache-dir pyspark

WORKDIR /app
COPY . .

CMD ["python", "src/main.py"]