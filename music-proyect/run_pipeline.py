import os
import sys
import shutil
import logging
import subprocess
from datetime import datetime

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")
ALERTS_LOG = os.path.join("logs", "alerts.log")

PROCESSED_DIR = os.path.join("data", "processed")
WAREHOUSE_DB = os.path.join("data", "warehouse", "music_dw.sqlite")

INGEST_SCRIPT = os.path.join("src", "ingest.py")
TRANSFORM_SCRIPT = os.path.join("src", "processing", "transform.py")
LOAD_DW_SCRIPT = os.path.join("src", "warehouse", "load_dw.py")


# Ensure the logs directory exists
def ensure_logs_dir():
    os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging to file and console
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
            logging.StreamHandler()
        ],
    )

# Write an alert to the alerts log when a pipeline step fails
def write_alert(step_name: str, message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    alert_text = f"{timestamp} | PIPELINE FAILED | step={step_name} | {message}\n"

    with open(ALERTS_LOG, "a", encoding="utf-8") as f:
        f.write(alert_text)

    logging.error("ALERT: %s", alert_text.strip())


# Run a pipeline step as a subprocess and check for errors
def run_step(name: str, cmd: list[str]):
    logging.info(f"Running step: {name} ")

    result = subprocess.run(cmd, capture_output=True, text=True)

    # Log standard output if present
    if result.stdout:
        logging.info(result.stdout)

    # Log errors if present
    if result.stderr:
        logging.warning(result.stderr)

    # Stop the pipeline if the step failed
    if result.returncode != 0:
        err_msg = result.stderr.strip() if result.stderr else f"Exit code {result.returncode}"
        write_alert(name, err_msg)
        raise SystemExit(result.returncode)


    logging.info(f"Step finished successfully: {name}")


# Delete derived outputs to avoid duplicated 
def clean_outputs():
    # Remove processed data (will be regenerated)
    if os.path.exists(PROCESSED_DIR):
        shutil.rmtree(PROCESSED_DIR)

    # Remove Data Warehouse database (will be recreated)
    if os.path.exists(WAREHOUSE_DB):
        os.remove(WAREHOUSE_DB)


def main():

    ensure_logs_dir()
    setup_logging()

    # delete derived outputs before running
    if "--clean" in sys.argv:
        clean_outputs()

    # 1: Ingestion 
    if os.path.exists(INGEST_SCRIPT):
        run_step("ingest", [sys.executable, INGEST_SCRIPT])

    # 2: Transform 
    run_step("transform", [sys.executable, TRANSFORM_SCRIPT])

    #3: Load data into the Data Warehouse (SQLite)
    run_step("load_dw", [sys.executable, LOAD_DW_SCRIPT])

    print("SUCCESS: pipeline finished")


if __name__ == "__main__":
    main()
