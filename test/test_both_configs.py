#!/usr/bin/env python3
"""
Test both metadata configs one by one
"""

import json
import subprocess
import sys
from pathlib import Path
import shutil


def run_config(config_path, config_name):
    """Run a config and return results"""
    print(f"\n{'='*70}")
    print(f"TEST {config_name}")
    print(f"{'='*70}")
    print(f"Config file: {config_path}\n")
    
    # Backup current config only if we're switching configs
    backup_path = "metadata/config.json.bak"
    need_restore = False
    
    try:
        # Copy test config to main config location (only if different)
        if config_path != "metadata/config.json":
            if Path("metadata/config.json").exists():
                shutil.copy("metadata/config.json", backup_path)
                need_restore = True
            shutil.copy(config_path, "metadata/config.json")
        
        # Display config content
        with open("metadata/config.json") as f:
            config = json.load(f)
        
        print(f"Config Summary:")
        print(f"  - Dataflows: {len(config.get('dataflows', []))}")
        for i, flow in enumerate(config.get('dataflows', []), 1):
            print(f"    Flow {i}: {flow.get('name', 'unnamed')}")
            print(f"      Sources: {len(flow.get('sources', []))}")
            for src in flow.get('sources', []):
                print(f"        - {src.get('name')} ({src.get('format')})")
            print(f"      Transformations: {len(flow.get('transformations', []))}")
            for t in flow.get('transformations', []):
                print(f"        - {t.get('name')} ({t.get('type')})")
            print(f"      Sinks: {len(flow.get('sinks', []))}")
            for sink in flow.get('sinks', []):
                print(f"        - {sink.get('name')} ({sink.get('format')})")
        
        print(f"\nRunning pipeline...")
        result = subprocess.run(
            [sys.executable, "src/main.py"],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            print("✅ PASSED - Pipeline executed successfully")
            
            # Check output files
            output_dir = Path("data/output")
            if output_dir.exists():
                output_files = list(output_dir.rglob("*.json")) + list(output_dir.rglob("*.parquet"))
                print(f"\n📁 Output files generated: {len(output_files)} files")
                
                # Find the most recent outputs
                for sink in flow.get('sinks', []):
                    sink_name = sink.get('name')
                    sink_paths = sink.get('paths', [])
                    for path in sink_paths:
                        if Path(path).exists():
                            files = list(Path(path).glob("part-*"))
                            if files:
                                print(f"  ✓ {sink_name}: {len(files)} file(s)")
                                # Show sample
                                first_file = files[0]
                                if first_file.suffix == ".json":
                                    with open(first_file) as f:
                                        first_record = f.readline()
                                    if first_record:
                                        print(f"    Sample: {first_record[:80]}...")
        else:
            print(f"❌ FAILED - Pipeline execution failed")
            print(f"\nSTDOUT:\n{result.stdout[-500:]}")
            print(f"\nSTDERR:\n{result.stderr[-500:]}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR - {type(e).__name__}: {e}")
        return False
    finally:
        # Restore backup only if we created one
        if need_restore and Path(backup_path).exists():
            shutil.move(backup_path, "metadata/config.json")


def main():
    print(f"\n{'='*70}")
    print("TESTING BOTH METADATA CONFIGS")
    print(f"{'='*70}")
    
    configs = [
        ("metadata/config.json", "CONFIG 1: Motor Policy Validation"),
        ("metadata/config_clients.json", "CONFIG 2: Clients (CSV with Advanced Features)"),
    ]
    
    results = []
    for config_path, config_name in configs:
        if not Path(config_path).exists():
            print(f"\n⚠️  Config not found: {config_path}")
            results.append((config_name, False))
            continue
        
        success = run_config(config_path, config_name)
        results.append((config_name, success))
    
    # Summary
    print(f"\n{'='*70}")
    print("TEST SUMMARY")
    print(f"{'='*70}")
    
    for config_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{status} - {config_name}")
    
    all_passed = all(success for _, success in results)
    print(f"\n{'='*70}")
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
    else:
        print("⚠️  SOME TESTS FAILED")
    print(f"{'='*70}\n")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
