import os
from urllib.parse import urlparse


def write_output(df, sink):
    if not isinstance(sink, dict):
        raise TypeError("sink must be a dictionary")

    sink_name = sink.get("name", "unnamed")
    sink_paths = sink.get("paths")
    sink_mode = sink.get("saveMode")
    sink_format = sink.get("format")

    if not sink_paths or not isinstance(sink_paths, list):
        raise ValueError(f"Sink '{sink_name}' must define a non-empty list in 'paths'")
    if not sink_mode:
        raise ValueError(f"Sink '{sink_name}' is missing required field 'saveMode'")
    if not sink_format:
        raise ValueError(f"Sink '{sink_name}' is missing required field 'format'")

    for path in sink_paths:
        if not path:
            raise ValueError(f"Sink '{sink_name}' contains an empty path entry")

        parsed = urlparse(path)
        is_local = parsed.scheme in {"", "file"}
        if is_local:
            local_path = parsed.path if parsed.scheme == "file" else path
            try:
                os.makedirs(local_path, exist_ok=True)
            except Exception as exc:
                raise RuntimeError(
                    f"Sink '{sink_name}' cannot create local output directory '{local_path}'"
                ) from exc

        try:
            df.write.mode(sink_mode).format(sink_format).save(path)
        except Exception as exc:
            raise RuntimeError(
                f"Failed writing sink '{sink_name}' to path '{path}' "
                f"with format '{sink_format}' and mode '{sink_mode}'"
            ) from exc
