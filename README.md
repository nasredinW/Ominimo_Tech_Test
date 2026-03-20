# Dynamic Spark Pipeline

Config-driven PySpark pipeline that reads source data, applies validations and transformations, then writes outputs to one or more sinks.

## Project Structure

```text
.
├── .dockerignore
├── .env
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── dags/
│   └── dynamic_spark_pipeline_dag.py
├── data/
│   ├── input.json
│   └── output/
├── metadata/
│   └── config.json
└── src/
		├── engine.py
		├── logger.py
		├── main.py
		├── transformer.py
		├── validations.py
		├── validator.py
		└── writer.py
```

## How It Works

1. `src/main.py` creates a Spark session and loads `metadata/config.json`.
2. `src/engine.py` executes the first dataflow in `config["dataflows"]`:
	 - reads all `sources`
	 - executes `transformations` in order
	 - writes `sinks`
3. Validation step (`validate_fields`) splits records into:
	 - `validation_ok`
	 - `validation_ko`
4. Transformation step (`add_fields`) can currently add fields via `current_timestamp`.

## Configuration Contract

The pipeline expects a JSON config with this top-level structure:

```json
{
	"dataflows": [
		{
			"name": "string",
			"sources": [
				{
					"name": "string",
					"format": "JSON|PARQUET|...",
					"path": "input path"
				}
			],
			"transformations": [
				{
					"name": "validation",
					"type": "validate_fields",
					"params": {
						"input": "source_name",
						"validations": [
							{
								"field": "column_name",
								"validations": ["notEmpty", "notNull"]
							}
						]
					}
				},
				{
					"name": "add_ingestion_date",
					"type": "add_fields",
					"params": {
						"input": "validation_ok",
						"addFields": [
							{
								"name": "ingestion_dt",
								"function": "current_timestamp"
							}
						]
					}
				}
			],
			"sinks": [
				{
					"name": "raw-ok",
					"input": "add_ingestion_date",
					"format": "JSON",
					"saveMode": "OVERWRITE",
					"paths": ["output path"]
				}
			]
		}
	]
}
```

## Validation Functions

Implemented in `src/validations.py`:

- `notEmpty`: value is not null and not empty string.
- `notNull`: value is not null.

Mapped via `VALIDATION_MAP` and consumed by `src/validator.py`.

## Run Locally

Prerequisites:

- Python 3.9+
- Java runtime compatible with your PySpark version

Install dependencies:

```bash
pip install pyspark
```

Run:

```bash
python src/main.py
```

## Run with Docker

The Docker image already includes Java (`openjdk-21-jre-headless`) and sets `JAVA_HOME`, so PySpark can start correctly.

Build image:

```bash
docker build -t dynamic-pipeline .
```

Run container:

```bash
docker run --rm -v "$(pwd)/data:/app/data" dynamic-pipeline
```

If your config uses absolute `/data/...` paths, mount to `/data` instead:

```bash
docker run --rm -v "$(pwd)/data:/data" dynamic-pipeline
```

## Airflow Orchestration + Docker Compose

A complete orchestration setup with Airflow scheduler, webserver, PostgreSQL backend, and integrated Spark runtime.

### Features

- **Airflow DAG** (`dags/dynamic_spark_pipeline_dag.py`):
  - Daily scheduling at 2 AM (configurable via `schedule_interval`)
  - Pre-flight config validation
  - Pipeline execution with streaming logs
  - Output validation (checks `_SUCCESS` markers)
  - Automated statistics logging
  - Retry logic (2 retries on failure)

- **Docker Compose Stack**:
  - PostgreSQL 15 for Airflow metadata
  - Airflow Webserver on `http://localhost:8080`
  - Airflow Scheduler with continuous task monitoring
  - Spark-ready Python runtime

- **Logging & Monitoring**:
  - Rotating file logs (10 MB max per file, 5 backups)
  - Console + file output to `/var/log/airflow/pipeline.log`
  - Task-level logging in Airflow UI
  - Pipeline execution statistics

### Quick Start

1. **Start the stack**:
   ```bash
   docker-compose up -d
   ```
   First run initializes the database (~30 seconds).

2. **Verify services**:
   ```bash
   docker-compose ps
   ```
   All services should show "healthy" within 2–3 minutes.

3. **Access Airflow UI**:
   - Navigate to `http://localhost:8080`
   - Login: `admin` / `admin`
   - Find DAG: `dynamic_spark_pipeline`

4. **Manually trigger the DAG**:
   ```bash
   docker-compose exec airflow-scheduler airflow dags trigger dynamic_spark_pipeline
   ```

5. **View logs**:
   ```bash
   # Scheduler logs
   docker-compose logs -f airflow-scheduler
   
   # Webserver logs
   docker-compose logs -f airflow-webserver
   
   # Pipeline output
   docker-compose exec airflow-scheduler tail -f /app/airflow/logs/dynamic_spark_pipeline/*/run/*.log
   ```

6. **Stop the stack**:
   ```bash
   docker-compose down
   ```
   (Data persists in volumes; re-run `docker-compose up -d` to resume)

### Configuration

Edit `.env` for environment variables or `docker-compose.yml` to adjust:
- Schedule interval (default: `0 2 * * *`)
- Log rotation size (default: 10 MB)
- Database credentials and ports

### Troubleshooting

**Webserver not responding**: Wait 30–60 seconds for PostgreSQL migration. Check logs:
```bash
docker-compose logs airflow-webserver
```

**Task fails with "config file not found"**: Verify `metadata/config.json` exists at the repo root.

**Out of disk space**: Clean up old Docker logs:
```bash
docker-compose down -v  # Warning: deletes all volumes
```

### Customization

- **Change schedule**: Edit `schedule_interval` in `dags/dynamic_spark_pipeline_dag.py`
- **Add alerts**: Add email or Slack notifications to the DAG (see Airflow documentation)
- **Scale Spark**: Replace `spark-master` service with a full Spark cluster image (e.g., `bitnami/spark`)
- **Additional transformations**: Modify `metadata/config.json` dataflow definitions