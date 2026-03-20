def read_source(spark, source_config):
    if not isinstance(source_config, dict):
        raise TypeError("source_config must be a dictionary")

    source_name = source_config.get("name", "unnamed")
    source_path = source_config.get("path")
    source_format = source_config.get("format")

    if not source_path:
        raise ValueError(f"Source '{source_name}' is missing required field 'path'")
    if not source_format:
        raise ValueError(f"Source '{source_name}' is missing required field 'format'")

    try:
        return spark.read.format(source_format).load(source_path)
    except Exception as exc:
        raise RuntimeError(
            f"Failed reading source '{source_name}' with format '{source_format}' from '{source_path}'"
        ) from exc