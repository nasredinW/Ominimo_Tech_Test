import os
import shutil
import time
import subprocess
from urllib.parse import urlparse


def _aggressive_cleanup(local_path, sink_name):
    """Attempt multiple strategies to clean up a directory."""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Strategy 1: chmod with find to handle deeply nested permissions
            result = subprocess.run(
                ["find", local_path, "-type", "f", "-exec", "chmod", "666", "{}", "+"],
                capture_output=True,
                timeout=10
            )
            result = subprocess.run(
                ["find", local_path, "-type", "d", "-exec", "chmod", "777", "{}", "+"],
                capture_output=True,
                timeout=10
            )
            print(f"[{sink_name}] Attempt {attempt + 1}/{max_retries}: chmod via find on {local_path}")
        except Exception as e:
            print(f"[{sink_name}] find chmod failed (attempt {attempt + 1}): {e}")
        
        try:
            # Strategy 2: rm -rf via shell
            result = subprocess.run(
                ["rm", "-rf", local_path],
                capture_output=True,
                timeout=10
            )
            if result.returncode == 0:
                print(f"[{sink_name}] Successfully removed {local_path} via rm -rf")
                time.sleep(0.5)  # Allow filesystem to stabilize
                return True
            else:
                print(f"[{sink_name}] rm -rf failed with code {result.returncode}")
        except Exception as e:
            print(f"[{sink_name}] Shell rm failed (attempt {attempt + 1}): {e}")
        
        try:
            # Strategy 3: Python shutil.rmtree with strict error handling first
            if os.path.exists(local_path):
                try:
                    shutil.rmtree(local_path, ignore_errors=False)
                    print(f"[{sink_name}] Successfully removed {local_path} via shutil.rmtree")
                    time.sleep(0.5)  # Allow filesystem to stabilize
                    return True
                except (OSError, PermissionError) as perm_error:
                    # If permission denied, try with ignore_errors=True to skip individual file errors
                    print(f"[{sink_name}] shutil.rmtree strict mode failed: {perm_error}")
                    print(f"[{sink_name}] Retrying with ignore_errors=True...")
                    shutil.rmtree(local_path, ignore_errors=True)
                    if not os.path.exists(local_path):
                        print(f"[{sink_name}] Successfully removed {local_path} (with ignored errors)")
                        time.sleep(0.5)
                        return True
        except Exception as e:
            print(f"[{sink_name}] shutil.rmtree failed (attempt {attempt + 1}): {e}")
        
        try:
            # Strategy 4: Use find to delete individual files
            result = subprocess.run(
                ["find", local_path, "-type", "f", "-delete"],
                capture_output=True,
                timeout=10
            )
            result = subprocess.run(
                ["find", local_path, "-type", "d", "-empty", "-delete"],
                capture_output=True,
                timeout=10
            )
            if not os.path.exists(local_path) or not os.listdir(local_path):
                print(f"[{sink_name}] Successfully cleared {local_path} via find delete")
                time.sleep(0.5)
                return True
        except Exception as e:
            print(f"[{sink_name}] find delete failed (attempt {attempt + 1}): {e}")
        
        if attempt < max_retries - 1:
            time.sleep(1)  # Wait before retry
    
    # If all strategies failed, return False
    return False


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
        local_path = parsed.path if parsed.scheme == "file" else path
        
        if is_local:
            # For OVERWRITE mode on local filesystem, clean up the directory first
            # to avoid Spark permission issues trying to delete files
            if sink_mode.upper() == "OVERWRITE" and os.path.exists(local_path):
                print(f"[{sink_name}] Pre-emptively cleaning output directory: {local_path}")
                _aggressive_cleanup(local_path, sink_name)
            
            # Ensure parent directories exist
            try:
                os.makedirs(local_path, exist_ok=True)
            except Exception as exc:
                raise RuntimeError(
                    f"Sink '{sink_name}' cannot create local output directory '{local_path}'"
                ) from exc

        try:
            df.write.mode(sink_mode).format(sink_format).save(path)
            print(f"[{sink_name}] ✓ Successfully wrote to {path}")
        except Exception as exc:
            error_msg = str(exc)
            
            # If it's an overwrite/clearance error on local filesystem, try harder cleanup
            if "Unable to clear output directory" in error_msg and is_local:
                print(f"[{sink_name}] Write failed with directory clear error. Attempting aggressive cleanup...")
                if _aggressive_cleanup(local_path, sink_name):
                    try:
                        os.makedirs(local_path, exist_ok=True)
                        # Retry write
                        df.write.mode(sink_mode).format(sink_format).save(path)
                        print(f"[{sink_name}] ✓ Successfully wrote to {path} (after aggressive cleanup)")
                        return
                    except Exception as retry_exc:
                        raise RuntimeError(
                            f"Failed writing sink '{sink_name}' to path '{path}' "
                            f"(even after aggressive cleanup). Format: '{sink_format}', Mode: '{sink_mode}'. "
                            f"Original error: {error_msg}, Retry error: {str(retry_exc)}"
                        ) from retry_exc
                else:
                    raise RuntimeError(
                        f"Could not clean output directory '{local_path}' for sink '{sink_name}'. "
                        f"Original error: {error_msg}"
                    ) from exc
            
            raise RuntimeError(
                f"Failed writing sink '{sink_name}' to path '{path}' "
                f"with format '{sink_format}' and mode '{sink_mode}'. "
                f"Error: {error_msg}"
            ) from exc
