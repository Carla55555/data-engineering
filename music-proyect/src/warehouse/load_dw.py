import os
import logging
import sqlite3
import pandas as pd

PROCESSED_DIR = os.path.join("data", "processed")
WAREHOUSE_DIR = os.path.join("data", "warehouse")
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "warehouse.log")

MUSIC_BY_GENRE_PATH = os.path.join(PROCESSED_DIR, "music_features_by_genre.csv")
MENTAL_BY_GENRE_PATH = os.path.join(PROCESSED_DIR, "mental_health_by_genre.csv")

DB_PATH = os.path.join(WAREHOUSE_DIR, "music_dw.sqlite")

# If folders do not exist, create them
def ensure_dirs():
    if not os.path.exists(WAREHOUSE_DIR):
        os.makedirs(WAREHOUSE_DIR)
        print("Folder created:", WAREHOUSE_DIR)
    else:
        print("Folder already exists:", WAREHOUSE_DIR)

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
        handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
    )
    logging.info("Logging initialized for warehouse step")


# Connect to SQLite database (creates file if it doesn't exist)
def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    #enforce foreign keys 
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def create_schema(conn: sqlite3.Connection):
    #Create a galaxy schema, there are 3 tables: dim_genre, fact_music_features, fact_mental_health
    
    cur = conn.cursor()

    # Dimension table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_genre (
            genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre TEXT NOT NULL UNIQUE
        );
    """)

    # Facts tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_music_features (
            genre_id INTEGER NOT NULL,
            feature_name TEXT NOT NULL,
            feature_value REAL,
            PRIMARY KEY (genre_id, feature_name),
            FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_mental_health (
            genre_id INTEGER NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL,
            PRIMARY KEY (genre_id, metric_name),
            FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
        );
    """)

    conn.commit()
    logging.info("Schema created / validated")

#Insert genres into dim_genre (ignore if already exists)
def upsert_genres(conn: sqlite3.Connection, genres: list[str]):
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO dim_genre (genre) VALUES (?);",
        [(g,) for g in genres]
    )
    conn.commit()
    logging.info(f"Upserted genres into dim_genre: {len(genres)} candidates")


# Create a dictionary(genre -> genre_id) that converts, for example: "rock" → 1, "jazz" → 2. Because in fact tables you don't store text, you store genre_id
def get_genre_id_map(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    rows = cur.execute("SELECT genre_id, genre FROM dim_genre;").fetchall()
    return {genre: genre_id for (genre_id, genre) in rows}

#Empty the fact tables before reloading them, so that if you run it twice, duplicate data is not saved.
def clear_facts(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DELETE FROM fact_music_features;")
    cur.execute("DELETE FROM fact_mental_health;")
    conn.commit()
    logging.info("Cleared fact tables (fact_music_features, fact_mental_health)")


def load_music_facts(conn: sqlite3.Connection, music_df: pd.DataFrame, genre_id_map: dict):
    rows_to_insert = []
    feature_cols = [c for c in music_df.columns if c != "genre"]

    for _, row in music_df.iterrows():
        #Transform genre to genre_id
        genre = row["genre"]
        genre_id = genre_id_map.get(genre)
        if genre_id is None:
            continue

        for c in feature_cols:
            val = row[c]
            # Keep NaN as None for SQLite
            if pd.isna(val):
                val = None
            rows_to_insert.append((genre_id, c, val))

    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO fact_music_features (genre_id, feature_name, feature_value) VALUES (?, ?, ?);",
        rows_to_insert
    )
    conn.commit()
    logging.info(f"Loaded music facts: {len(rows_to_insert)} rows")

#the same that load_music_facts but for mental health indicators
def load_mental_facts(conn: sqlite3.Connection, mental_df: pd.DataFrame, genre_id_map: dict):
    rows_to_insert = []
    metric_cols = [c for c in mental_df.columns if c != "genre"]

    for _, row in mental_df.iterrows():
        genre = row["genre"]
        genre_id = genre_id_map.get(genre)
        if genre_id is None:
            continue

        for c in metric_cols:
            val = row[c]
            if pd.isna(val):
                val = None
            rows_to_insert.append((genre_id, c, val))

    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO fact_mental_health (genre_id, metric_name, metric_value) VALUES (?, ?, ?);",
        rows_to_insert
    )
    conn.commit()
    logging.info(f"Loaded mental health facts: {len(rows_to_insert)} rows")


def main():
    ensure_dirs()
    setup_logging()

    # 1) Check inputs exist
    if not os.path.exists(MUSIC_BY_GENRE_PATH):
        logging.error(f"Missing processed file: {MUSIC_BY_GENRE_PATH}")
        raise SystemExit(1)

    if not os.path.exists(MENTAL_BY_GENRE_PATH):
        logging.error(f"Missing processed file: {MENTAL_BY_GENRE_PATH}")
        raise SystemExit(1)

    logging.info("Starting warehouse load (SQLite)...")

    # 2) Read processed CSVs
    music_df = pd.read_csv(MUSIC_BY_GENRE_PATH)
    mental_df = pd.read_csv(MENTAL_BY_GENRE_PATH)

    # Basic validation: must have genre column
    if "genre" not in music_df.columns:
        logging.error("music_features_by_genre.csv missing 'genre' column")
        raise SystemExit(1)
    if "genre" not in mental_df.columns:
        logging.error("mental_health_by_genre.csv missing 'genre' column")
        raise SystemExit(1)

    logging.info(f"Loaded processed tables: music={music_df.shape}, mental={mental_df.shape}")

    # 3) Connect DW
    conn = connect_db(DB_PATH)

    try:
        # 4) Create schema
        create_schema(conn)

        # 5) Insert genres (union from both tables)
        all_genres = sorted(set(music_df["genre"].dropna().astype(str)) | set(mental_df["genre"].dropna().astype(str)))
        upsert_genres(conn, all_genres)

        # 6) Get mapping genre -> id
        genre_id_map = get_genre_id_map(conn)
        logging.info(f"dim_genre size: {len(genre_id_map)}")

        # 7) Clear facts and load fresh (so reruns don't duplicate)
        clear_facts(conn)

        # 8) Load facts
        load_music_facts(conn, music_df, genre_id_map)
        load_mental_facts(conn, mental_df, genre_id_map)

        logging.info(f"Warehouse load finished successfully, DB at: {DB_PATH}")
        print("SUCCESS: Data Warehouse created/updated:", DB_PATH)

    finally:
        conn.close()


if __name__ == "__main__":
    main()