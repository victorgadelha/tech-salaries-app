import subprocess
import sys

def run_script(script_path):
    result = subprocess.run(["uv", "run", script_path])
    if result.returncode != 0:
        sys.exit(1)

def main():
    scripts = [
        "src/01_ingest_bronze.py",
        "src/02_transform_silver.py",
        "src/03_aggregate_gold.py"
    ]
    
    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    main()

    