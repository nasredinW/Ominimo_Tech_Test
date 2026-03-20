"""Airflow DAG to orchestrate the dynamic Spark pipeline with logging and alerts."""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults

# Configure logger
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "Nasredine ouelseti",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
}


def check_config_exists(**context):
    """Verify config file exists before running pipeline."""
    config_path = Path("/app/metadata/config.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path) as f:
        config = json.load(f)
    logger.info(f"Config loaded. Dataflows: {[df['name'] for df in config.get('dataflows', [])]}")
    context['task_instance'].xcom_push(key='config', value=config)


def validate_pipeline_outputs(**context):
    """Check that pipeline outputs exist and log metrics."""
    output_paths = [
        Path("/app/data/output/events/motor_policy/_SUCCESS"),
        Path("/app/data/output/discards/motor_policy/_SUCCESS"),
    ]
    missing = [p for p in output_paths if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing output markers: {missing}")
    logger.info("✓ All pipeline outputs validated successfully")


def log_pipeline_stats(**context):
    """Log pipeline execution statistics."""
    try:
        events_path = Path("/app/data/output/events/motor_policy")
        discards_path = Path("/app/data/output/discards/motor_policy")
        
        events_files = list(events_path.glob("part-*.json")) if events_path.exists() else []
        discards_files = list(discards_path.glob("part-*.json")) if discards_path.exists() else []
        
        logger.info(f"Pipeline Stats: {len(events_files)} valid records, {len(discards_files)} discarded")
        context['task_instance'].xcom_push(key='stats', value={
            'valid_records': len(events_files),
            'discarded_records': len(discards_files),
        })
    except Exception as e:
        logger.warning(f"Could not gather stats: {e}")


with DAG(
    dag_id="dynamic_spark_pipeline",
    description="Orchestrate config-driven PySpark data pipeline with logging and alerts",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    tags=["spark", "pyspark", "data-pipeline"],
    doc_md="""
## Dynamic Spark Pipeline DAG

This DAG orchestrates a config-driven PySpark pipeline that:
- Reads source data from JSON
- Applies field validations and transformations
- Writes valid records and discards to separate outputs

### Tasks:
1. **check_config** - Validates config file exists
2. **run_pipeline** - Executes the PySpark job
3. **validate_outputs** - Verifies output markers
4. **log_stats** - Logs execution metrics
""",
) as dag:
    
    check_config = PythonOperator(
        task_id="check_config",
        python_callable=check_config_exists,
        provide_context=True,
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="""
        set -e
        cd /app
        python src/main.py 2>&1 | tee -a /var/log/airflow/pipeline.log
        echo "Pipeline execution completed"
        """,
        retries=2,
    )

    validate_outputs = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_pipeline_outputs,
        provide_context=True,
        trigger_rule="all_done",  # Run even if previous failed
    )

    log_stats = PythonOperator(
        task_id="log_stats",
        python_callable=log_pipeline_stats,
        provide_context=True,
    )

    # Define task dependencies
    check_config >> run_pipeline >> validate_outputs >> log_stats
