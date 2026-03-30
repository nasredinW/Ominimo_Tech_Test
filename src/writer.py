import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse


_SAFE_FS_CHARS_RE = re.compile(r"[^A-Za-z0-9._=\-]+")


def _env_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_run_id() -> str:
    """Best-effort run id for output versioning.

    Priority:
    - PIPELINE_RUN_ID (explicit override)
    - AIRFLOW_CTX_DAG_RUN_ID / AIRFLOW_CTX_EXECUTION_DATE (when running inside Airflow)
    - current UTC timestamp
    """

    candidates = [
        os.getenv("PIPELINE_RUN_ID"),
        os.getenv("AIRFLOW_CTX_DAG_RUN_ID"),
        os.getenv("AIRFLOW_CTX_EXECUTION_DATE"),
    ]
    for candidate in candidates:
        if candidate:
            safe = _SAFE_FS_CHARS_RE.sub("_", candidate)
            safe = safe.strip("_.-")
            if safe:
                return safe

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _versioning_enabled_for_sink(sink: dict) -> bool:
    """Enable result versioning by default for production-like behavior.

    - Can be disabled with OUTPUT_VERSIONING=0/false
    - Can be explicitly set per sink via: versioning: true/false or versioning: {enabled: true/false}
    """

    env_flag = os.getenv("OUTPUT_VERSIONING")
    if env_flag is not None and not _env_truthy(env_flag):
        return False

    cfg = sink.get("versioning")
    if isinstance(cfg, bool):
        return cfg
    if isinstance(cfg, dict):
        enabled = cfg.get("enabled")
        if isinstance(enabled, bool):
            return enabled

    return True


def _join_uri(base_uri: str, *parts: str) -> str:
    base_uri = base_uri.rstrip("/")
    cleaned = [p.strip("/") for p in parts if p]
    return "/".join([base_uri] + cleaned) if cleaned else base_uri


def _write_latest_marker_hadoop(spark, base_uri: str, version_uri: str, sink_name: str) -> None:
    """Write <base_uri>/LATEST with the version uri using Hadoop FS.

    Works for any filesystem Spark/Hadoop is configured for (s3a://, abfs://, gs://, file:/, ...).
    Best-effort: failures do not fail the pipeline.
    """

    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()

        uri = jvm.java.net.URI(base_uri)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        latest_path = jvm.org.apache.hadoop.fs.Path(_join_uri(base_uri, "LATEST"))
        out = fs.create(latest_path, True)
        try:
            data = (version_uri + "\n").encode("utf-8")
            inp = jvm.java.io.ByteArrayInputStream(data)
            jvm.org.apache.hadoop.io.IOUtils.copyBytes(inp, out, conf, True)
        finally:
            out.close()
    except Exception as exc:
        print(f"[{sink_name}] Warning: could not write LATEST marker via Hadoop FS: {exc}")


def _update_latest_pointer_local(base_dir: str, version_id: str, sink_name: str) -> None:
    """Create/refresh <base_dir>/_latest -> _versions/<version_id> (best-effort)."""

    import shutil

    latest_link = os.path.join(base_dir, "_latest")
    target_rel = os.path.join("_versions", version_id)

    try:
        if os.path.islink(latest_link) or os.path.isfile(latest_link):
            os.unlink(latest_link)
        elif os.path.isdir(latest_link):
            shutil.rmtree(latest_link, ignore_errors=True)
    except Exception as exc:
        print(f"[{sink_name}] Warning: could not remove existing _latest pointer: {exc}")

    try:
        os.symlink(target_rel, latest_link)
    except Exception as exc:
        try:
            with open(os.path.join(base_dir, "LATEST"), "w", encoding="utf-8") as f:
                f.write(f"{target_rel}\n")
        except Exception:
            pass
        print(f"[{sink_name}] Warning: could not create _latest symlink ({exc}); wrote LATEST file if possible")


def write_output(df, sink):
    if not isinstance(sink, dict):
        raise TypeError("sink must be a dictionary")

    sink_name = sink.get("name", "unnamed")
    sink_paths = sink.get("paths")
    sink_mode = sink.get("saveMode")
    sink_format = sink.get("format")
    sink_options = sink.get("options") or {}
    sink_partition_by = sink.get("partitionBy")

    if not sink_paths or not isinstance(sink_paths, list):
        raise ValueError(f"Sink '{sink_name}' must define a non-empty list in 'paths'")
    if not sink_mode:
        raise ValueError(f"Sink '{sink_name}' is missing required field 'saveMode'")
    if not sink_format:
        raise ValueError(f"Sink '{sink_name}' is missing required field 'format'")
    if not isinstance(sink_options, dict):
        raise TypeError(f"Sink '{sink_name}' field 'options' must be a dictionary")
    if sink_partition_by is not None and not isinstance(sink_partition_by, list):
        raise TypeError(f"Sink '{sink_name}' field 'partitionBy' must be a list of column names")

    for path in sink_paths:
        if not path:
            raise ValueError(f"Sink '{sink_name}' contains an empty path entry")

        parsed = urlparse(path)
        scheme = parsed.scheme
        is_local = scheme in {"", "file"}
        base_uri = f"file:{parsed.path}" if scheme == "file" else path
        base_local_path = parsed.path if scheme == "file" else path

        versioning_enabled = _versioning_enabled_for_sink(sink)
        version_id = _get_run_id() if versioning_enabled else None

        if versioning_enabled:
            effective_uri = _join_uri(base_uri, "_versions", version_id)
            effective_local_path = _join_uri(base_local_path, "_versions", version_id) if is_local else None
        else:
            effective_uri = base_uri
            effective_local_path = base_local_path if is_local else None
        
        if is_local:
            # Ensure directories exist for local filesystem targets.
            try:
                os.makedirs(effective_local_path, exist_ok=True)
            except Exception as exc:
                raise RuntimeError(
                    f"Sink '{sink_name}' cannot create local output directory '{effective_local_path}'"
                ) from exc

        try:
            writer = df.write.mode(sink_mode).format(sink_format)
            if sink_options:
                writer = writer.options(**sink_options)
            if sink_partition_by:
                writer = writer.partitionBy(*sink_partition_by)

            writer.save(effective_uri)
            if versioning_enabled:
                if is_local:
                    _update_latest_pointer_local(base_local_path, version_id, sink_name)
                else:
                    _write_latest_marker_hadoop(df.sparkSession, base_uri, effective_uri, sink_name)

                print(f"[{sink_name}] ✓ Successfully wrote version '{version_id}' to {effective_uri}")
            else:
                print(f"[{sink_name}] ✓ Successfully wrote to {base_uri}")
        except Exception as exc:
            error_msg = str(exc)

            if is_local and "Permission denied" in error_msg:
                uid = os.geteuid() if hasattr(os, "geteuid") else "unknown"
                gid = os.getegid() if hasattr(os, "getegid") else "unknown"
                raise RuntimeError(
                    f"Failed writing sink '{sink_name}' to '{base_uri}'. Permission denied. "
                    f"This is typically a bind-mount ownership mismatch (container uid={uid}, gid={gid}). "
                    f"If using docker-compose, run: docker-compose run --rm fix-permissions"
                ) from exc
            
            raise RuntimeError(
                f"Failed writing sink '{sink_name}' to path '{base_uri}' "
                f"with format '{sink_format}' and mode '{sink_mode}'. "
                f"Error: {error_msg}"
            ) from exc
