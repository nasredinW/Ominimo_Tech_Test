"""Airflow DAG for dynamic, config-driven PySpark data pipeline orchestration.

Features:
- Auto-detects dataflows from configuration
- Customizable via environment variables and DAG parameters
- Inter-task communication via XCom
- Comprehensive logging with dataflow-level metrics
"""

import json
import logging
import os
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURATION - All customizable via environment variables
# ==============================================================================

def _get_env(key: str, default: str) -> str:
    """Get environment variable with fallback."""
    return os.getenv(key, default)


# Paths
APP_HOME = _get_env("APP_HOME", "/app")
CONFIG_PATH = _get_env("CONFIG_PATH", f"{APP_HOME}/metadata/config.json")
DATA_PATH = _get_env("DATA_PATH", f"{APP_HOME}/data")
LOG_PATH = _get_env("LOG_PATH", f"{APP_HOME}/airflow/logs/pipeline")
SCRIPT_PATH = _get_env("SCRIPT_PATH", f"{APP_HOME}/src/main.py")

# Scheduling
SCHEDULE_INTERVAL = _get_env("SCHEDULE_INTERVAL", "0 2 * * *")
DAG_ID = _get_env("DAG_ID", "dynamic_spark_pipeline")

# Retry config
DEFAULT_RETRIES = int(_get_env("DEFAULT_RETRIES", "2"))
RETRY_DELAY_MINUTES = int(_get_env("RETRY_DELAY_MINUTES", "5"))

DEFAULT_ARGS = {
    "owner": _get_env("DAG_OWNER", "Nasredine Ouelseti"),
    "depends_on_past": False,
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
    "start_date": datetime(2026, 1, 1),
}


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================


def load_config(config_file: str) -> dict:
    """Load and validate JSON configuration."""
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path) as f:
        config = json.load(f)
    
    if "dataflows" not in config:
        raise ValueError("Config must contain 'dataflows' key")
    
    return config


def get_dataflows(config: dict) -> list:
    """Extract dataflow names from configuration."""
    return [
        df.get("name", f"dataflow_{i}") 
        for i, df in enumerate(config.get("dataflows", []))
    ]


def _resolve_sink_path(sink_path: str) -> Path:
    """Resolve sink path relative to APP_HOME."""
    path = Path(sink_path)
    return path if path.is_absolute() else Path(APP_HOME) / sink_path


def _count_sink_files(sink_path: Path) -> int:
    """Count JSON output files in sink directory."""
    return len(list(sink_path.glob("part-*.json"))) if sink_path.exists() else 0


def check_config_exists(**context):
    """Validate config file and extract dataflows."""
    try:
        config = load_config(CONFIG_PATH)
        dataflows = get_dataflows(config)
        
        logger.info(f"✓ Config loaded: {CONFIG_PATH}")
        logger.info(f"✓ Dataflows: {dataflows}")
        
        # Pass config to downstream tasks
        ti = context['task_instance']
        ti.xcom_push(key='config', value=config)
        ti.xcom_push(key='dataflows', value=dataflows)
        
        return {"status": "success", "dataflows": dataflows}
    
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.error(f"✗ Config validation failed: {e}")
        raise


def validate_pipeline_outputs(**context):
    """Validate pipeline outputs for all dataflows."""
    try:
        ti = context['task_instance']
        config = ti.xcom_pull(key='config', task_ids='check_config')
        
        if not config:
            logger.warning("No config found from check_config task")
            return {"status": "warning", "message": "No config", "total_files": 0}
        
        logger.info("=" * 80)
        logger.info("VALIDATING PIPELINE OUTPUTS")
        logger.info("=" * 80)
        
        output_stats = {}
        total_files = 0
        
        for dataflow in config.get("dataflows", []):
            df_name = dataflow.get("name", "unknown")
            df_stats = {}
            
            logger.info(f"Dataflow: {df_name}")
            
            for sink in dataflow.get("sinks", []):
                sink_name = sink.get("name", "unknown")
                sink_paths = sink.get("paths", [])
                
                file_count = 0
                for sink_path in sink_paths:
                    full_path = _resolve_sink_path(sink_path)
                    file_count += _count_sink_files(full_path)
                    status = "✓" if full_path.exists() else "✗"
                    logger.info(f"  {status} {sink_name}: {full_path}")
                
                df_stats[sink_name] = file_count
                total_files += file_count
                logger.info(f"    [{sink_name}] {file_count} file(s)")
            
            output_stats[df_name] = df_stats
        
        logger.info("=" * 80)
        logger.info(f"✓ Total output files: {total_files}")
        logger.info("=" * 80)
        
        ti.xcom_push(key='output_stats', value=output_stats)
        
        return {
            "status": "success" if total_files > 0 else "warning",
            "total_files": total_files,
            "stats": output_stats,
        }
    
    except Exception as e:
        logger.error(f"✗ Output validation failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e), "total_files": 0}


def log_pipeline_stats(**context):
    """Log aggregated pipeline execution statistics."""
    try:
        ti = context['task_instance']
        stats = ti.xcom_pull(key='output_stats', task_ids='validate_outputs')
        
        if not stats:
            logger.warning("No output statistics available")
            return
        
        logger.info("=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        
        total = 0
        for df_name, sinks_stats in stats.items():
            logger.info(f"  {df_name}")
            df_total = 0
            
            if isinstance(sinks_stats, dict):
                for sink_name, count in sinks_stats.items():
                    logger.info(f"    - {sink_name:25} | {count:4d} file(s)")
                    df_total += count
            
            logger.info(f"    Subtotal: {df_total:4d}")
            total += df_total
        
        logger.info("-" * 80)
        logger.info(f"  TOTAL: {total:4d} file(s)")
        logger.info("=" * 80)
        
        ti.xcom_push(key='final_stats', value={
            'total_output': total,
            'dataflow_stats': stats,
        })
    
    except Exception as e:
        logger.warning(f"Could not gather stats: {e}")


# ==============================================================================
# DAG DEFINITION
# ==============================================================================

_BASH_SCRIPT = f"""
set -e
LOG_DIR_BACKUP="/tmp/airflow_logs"

echo "Starting pipeline: {CONFIG_PATH}"

# Create and clean directories
mkdir -p "{LOG_PATH}" "{DATA_PATH}/output" 2>/dev/null || true
chmod -R 777 "{DATA_PATH}/output" 2>/dev/null || true
rm -rf "{DATA_PATH}/output" 2>/dev/null && \\
  mkdir -p "{DATA_PATH}/output/events" "{DATA_PATH}/output/discards" 2>/dev/null || true

# Set effective log path
export LOG_PATH_EFFECTIVE="{LOG_PATH}"
[ -w "{LOG_PATH}" ] || export LOG_PATH_EFFECTIVE="$LOG_DIR_BACKUP"

# Execute pipeline
cd "{APP_HOME}" && python "{SCRIPT_PATH}" 2>&1 || exit 1
echo "✓ Pipeline completed successfully"
"""

with DAG(
    dag_id=DAG_ID,
    description="Config-driven PySpark pipeline with dynamic dataflows",
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=["spark", "pyspark", "data-pipeline", "dynamic"],
    params={
        "config_path": CONFIG_PATH,
        "data_path": DATA_PATH,
        "enable_validation": True,
        "enable_stats_logging": True,
    },
    doc_md=f"""
## Dynamic Spark Pipeline DAG

Orchestrates a config-driven PySpark pipeline with automatic dataflow detection.

**Configuration:**
- `APP_HOME`: {APP_HOME}
- `CONFIG_PATH`: {CONFIG_PATH}
- `DATA_PATH`: {DATA_PATH}
- `SCHEDULE`: {SCHEDULE_INTERVAL}

**Environment Variables:**
- `APP_HOME` - Application root (default: /app)
- `CONFIG_PATH` - Config file path
- `DATA_PATH` - Data directory path
- `SCHEDULE_INTERVAL` - Cron schedule
- `DAG_OWNER` - DAG owner

**Tasks:**
1. **check_config** - Validates configuration
2. **run_pipeline** - Executes PySpark job
3. **validate_outputs** - Verifies output files
4. **log_stats** - Reports statistics
""",
) as dag:
    
    check_config = PythonOperator(
        task_id="check_config",
        python_callable=check_config_exists,
        provide_context=True,
        doc="Validate config file and extract dataflows",
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=_BASH_SCRIPT,
        retries=DEFAULT_RETRIES,
        pool="default_pool",
        queue="default",
        doc="Execute PySpark transformation pipeline",
    )

    validate_outputs = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_pipeline_outputs,
        provide_context=True,
        trigger_rule="all_done",
        doc="Verify output files for all dataflows",
    )

    log_stats = PythonOperator(
        task_id="log_stats",
        python_callable=log_pipeline_stats,
        provide_context=True,
        trigger_rule="all_done",
        doc="Log execution statistics",
    )

    # Task dependencies
    check_config >> run_pipeline >> validate_outputs >> log_stats

