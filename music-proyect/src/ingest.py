import os
import logging
import csv

RAW_DIR = os.path.join("data", "raw")
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "ingest.log")

REQUIRED_FILES = [
    "dataset.csv",
    "mxmh_survey_results.csv",
]

# Minimum expected columns (to detect if the CSV is incorrect)
REQUIRED_COLUMNS = {
    "dataset.csv": ["filename", "label"],
    "mxmh_survey_results.csv": ["Age", "Fav genre"],
}


# If folders do not exist, create them
def ensure_folders():
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR)
        print("Folder created:", RAW_DIR)
    else:
        print("Folder already exists:", RAW_DIR)

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        print("Folder created:", LOG_DIR)
    else:
        print("Folder already exists:", LOG_DIR)

# Set up logging to file and console
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logging.info("Logging initialized")

# Check if a file exists and is not empty
def file_exists_and_not_empty(path: str) -> bool:
    if not os.path.exists(path):
        logging.error(f"Missing file: {path}")
        return False

    size = os.path.getsize(path)
    if size == 0:
        logging.error(f"File is empty: {path}")
        return False

    logging.info(f"Found file: {path} ({size} bytes)")
    return True

# Validate that required columns exist in a CSV file
def validate_columns(csv_path: str, required_cols: list[str]) -> bool:
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)

        if header is None:
            logging.error(f"No header found in CSV: {csv_path}")
            return False

        missing = [c for c in required_cols if c not in header]
        if missing:
            logging.error(f"CSV {os.path.basename(csv_path)} missing columns: {missing}")
            logging.info(f"Columns found: {header}")
            return False

        logging.info(f"Columns validated for {os.path.basename(csv_path)}")
        return True

    except UnicodeDecodeError:
        logging.error(
            f"Encoding error reading {csv_path}. Try saving it as UTF-8."
        )
        return False
    except Exception as e:
        logging.error(f"Error validating columns for {csv_path}: {e}")
        return False


def validate_all_files() -> bool:
    ok = True

    for filename in REQUIRED_FILES:
        path = os.path.join(RAW_DIR, filename)

        # 1) Exists and is not empty
        if not file_exists_and_not_empty(path):
            ok = False
            continue

        # 2) Columns check
        required_cols = REQUIRED_COLUMNS.get(filename, [])
        if required_cols:
            if not validate_columns(path, required_cols):
                ok = False

    return ok


def main():
    ensure_folders()
    setup_logging()

    logging.info("Starting ingestion validation (local raw data mode)...")
    ok = validate_all_files()

    if ok:
        logging.info("Ingestion validation finished successfully")
        print("SUCCESS: Raw datasets are ready.")
    else:
        logging.error("Ingestion validation finished with errors")
        print("ERROR: Check logs/ingest.log for details.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
