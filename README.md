# Dynamic Spark Pipeline

Config-driven PySpark pipeline that reads source data, applies validations and transformations, then writes outputs to one or more sinks.

## Project Structure

```text
.
├── Dockerfile
├── metadata/
│   └── config.json
└── src/
		├── engine.py
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

next : prepare Airflow DAG for orchestration