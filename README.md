# Dynamic Spark Pipeline

**100% Metadata-Driven | Zero Code Changes for New Features | Handler Registry Pattern**

Config-driven PySpark pipeline that reads source data, applies validations and transformations, then writes outputs to one or more sinks. Fully extensible without modifying the engine.

## ⭐ Key Features

✓ **Truly Dynamic** - All logic defined in JSON configs  
✓ **No Code Changes** - Add new transformation types without modifying `engine.py`  
✓ **Handler Registry** - Extensible transformation framework  
✓ **Type-Safe** - Metadata-driven validation at runtime  
✓ **Production-Ready** - Docker, Airflow, and Spark integrated  

## Project Structure

```text
.
├── .dockerignore
├── .env
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── ARCHITECTURE_SOLUTION.md          ← Start here for architecture overview
├── DYNAMIC_ARCHITECTURE.md           ← Handler registry guide & examples
├── dags/
│   └── dynamic_spark_pipeline_dag.py
├── data/
│   ├── input.json
│   └── output/
├── metadata/
│   ├── config.json                   (motor policy validation)
│   └── config_clients.json           (advanced features example)
├── src/
│   ├── engine.py                     (refactored: dynamic dispatch)
│   ├── logger.py
│   ├── main.py
│   ├── reader.py
│   ├── transformer.py                (deprecated: legacy support)
│   ├── transformers_registry.py      (NEW: handler registry)
│   ├── validations.py
│   ├── validator.py
│   └── writer.py
└── verify_dynamic_architecture.py    (test & demo script)
```

## How It Works

**Dynamic Handler Registry Pattern:**

1. `src/main.py` creates a Spark session and loads config
2. `src/engine.py` reads the dataflow definition:
   - Reads all `sources`
   - For each `transformation`:
     - Looks up handler from `transformers_registry` 
     - Instantiates handler with dataframe and params
     - Executes handler and stores result
   - Writes all `sinks`
3. Handlers are completely decoupled from engine - add new types anytime!

**Example dataflow:**

```json
{
  "transformations": [
    {
      "name": "filter_active",
      "type": "filter_rows",
      "params": {
        "input": "raw_data",
        "condition": "status = 'active'"
      }
    },
    {
      "name": "with_age_group",
      "type": "derive_column",
      "params": {
        "input": "filter_active",
        "column": "age_group",
        "expression": "CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END"
      }
    }
  ]
}
```

No code changes needed! Just define the config.

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
					"format": "JSON|PARQUET|CSV|...",
					"path": "input path",
					"options": {
						"delimiter": ";",
						"header": "true",
						"inferSchema": "true"
					}
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
								"rules": [{"name": "notEmpty"}, {"name": "notNull"}]
							}
						]
					}
				}
			],
			"sinks": [
				{
					"name": "raw-ok",
					"input": "validation_ok",
					"format": "JSON",
					"saveMode": "OVERWRITE",
					"paths": ["output path"]
				}
			]
		}
	]
}
```

## Available Transformation Types

All transformation types are defined in `src/transformers_registry.py`. Add new types WITHOUT code changes!

| Type | Description | Example |
|------|-------------|---------|
| `validate_fields` | Split into validation_ok/validation_ko | See config.json |
| `add_fields` | Add computed columns | Add current_timestamp |
| `filter_rows` | SQL WHERE conditions | `"age >= 18 AND status='active'"` |
| `derive_column` | SQL expressions (CASE WHEN) | `"CASE WHEN age<18 THEN 'minor' ELSE 'adult' END"` |
| `select_columns` | Project specific columns | `["name", "email", "age"]` |
| `rename_columns` | Rename columns | Map old→new names |
| `drop_columns` | Remove columns | List column names to drop |

**See [DYNAMIC_ARCHITECTURE.md](DYNAMIC_ARCHITECTURE.md) for detailed examples and guide to adding new types.**

## Architecture Overview

**See [ARCHITECTURE_SOLUTION.md](ARCHITECTURE_SOLUTION.md) for complete architecture documentation.**

### Handler Registry Pattern

- **Extensible**: Add new transformation types via handler registration
- **Metadata-Driven**: All logic in JSON configs
- **Zero Engine Changes**: Engine never modified for new transformation types
- **Clean Separation**: Each handler handles one transformation type

### Quick Example: Adding New Transformation Type

**1. Create handler in `src/transformers_registry.py`:**
```python
class MyNewHandler(TransformationHandler):
    def execute(self):
        my_param = self.params.get("my_param")
        # transform dataframe...
        return result_df
```

**2. Register in the registry:**
```python
TRANSFORMATION_REGISTRY["my_type"] = MyNewHandler
```

**3. Use in config:**
```json
{"type": "my_type", "params": {"input": "source", "my_param": "value"}}
```

**Done! Zero changes to `engine.py`.**

## Run Locally

Prerequisites:

- Python 3.9+
- Java runtime compatible with your PySpark version

Install dependencies:

```bash
pip install pyspark
```

Verify dynamic architecture:

```bash
python3 verify_dynamic_architecture.py
```

This will test all 7 transformation handlers and demonstrate the registry pattern works.

Run the pipeline:

```bash
python3 src/main.py
```

## Run with Docker

The Docker image already includes Java and PySpark, so it runs out of the box.

Build image:

```bash
docker build -t dynamic-pipeline .
```

Run container:

```bash
docker run --rm -v "$(pwd)/data:/app/data" dynamic-pipeline
```

## Orchestration with Airflow & Docker Compose

Run the full stack (PostgreSQL, Airflow, Spark):

```bash
docker-compose up
```

Access Airflow UI: http://localhost:8080  
DAG: `dynamic_spark_pipeline_dag`

See the DAG automatically execute the pipeline when config changes.


🔧 **Implementation Details**:
- [src/transformers_registry.py](src/transformers_registry.py) - All transformation handlers
- [src/engine.py](src/engine.py) - Dynamic dispatch engine
- [metadata/config.json](metadata/config.json) - Motor policy example config
- [metadata/config_clients.json](metadata/config_clients.json) - Advanced features example

✅ **Verification**:
- [verify_dynamic_architecture.py](verify_dynamic_architecture.py) - Live tests of handler registry

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