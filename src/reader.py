import os


def _env_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _should_log_row_counts() -> bool:
    """Whether to log expensive Spark row counts.

    Default is enabled to preserve current behavior.
    Disable with: PIPELINE_LOG_ROW_COUNTS=false
    """

    flag = os.getenv("PIPELINE_LOG_ROW_COUNTS")
    return True if flag is None else _env_truthy(flag)


def read_source(spark, source_config):
    if not isinstance(source_config, dict):
        raise TypeError("source_config must be a dictionary")

    source_name = source_config.get("name", "unnamed")
    source_path = source_config.get("path")
    source_format = source_config.get("format")
    source_options = source_config.get("options", {})

    if not source_path:
        raise ValueError(f"Source '{source_name}' is missing required field 'path'")
    if not source_format:
        raise ValueError(f"Source '{source_name}' is missing required field 'format'")

    try:

        # Detect storage type
        storage_type = "S3" if source_path.startswith("s3a://") else "Local"
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Reading {storage_type} source '{source_name}' from: {source_path}")
        
        reader = spark.read.format(source_format)
        
        # Apply options if provided
        if source_options:
            reader = reader.options(**source_options)
        
        df = reader.load(source_path)
        if _should_log_row_counts():
            logger.info(f"✓ Successfully read {storage_type} source '{source_name}' ({df.count()} records)")
        else:
            logger.info(f"✓ Successfully read {storage_type} source '{source_name}'")
        
        return df
    except Exception as exc:
        raise RuntimeError(
            f"Failed reading source '{source_name}' with format '{source_format}' from '{source_path}'"
        ) from exc
