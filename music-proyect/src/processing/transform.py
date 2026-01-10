import os
import logging
import pandas as pd

RAW_DIR = os.path.join("data", "raw")
PROCESSED_DIR = os.path.join("data", "processed")
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "transform.log")

MUSIC_PATH = os.path.join(RAW_DIR, "dataset.csv")
SURVEY_PATH = os.path.join(RAW_DIR, "mxmh_survey_results.csv")

OUT_MUSIC_BY_GENRE = os.path.join(PROCESSED_DIR, "music_features_by_genre.csv")
OUT_MENTAL_BY_GENRE = os.path.join(PROCESSED_DIR, "mental_health_by_genre.csv")


# If folders do not exist, create them
def ensure_dirs():
    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)
        print("Folder created:", PROCESSED_DIR)
    else:
        print("Folder already exists:", PROCESSED_DIR)

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
    logging.info("Logging initialized for transform step")


#1)Cleaning/Normalization

def normalize_genre(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.lower()

    mapping = {
        "hip hop": "hip-hop",
        "hiphop": "hip-hop",
        "hip-hop": "hip-hop",
        "r&b": "rnb",
        "rnb": "rnb",
        "electronic": "edm",
        "edm": "edm",
    }
    return x.replace(mapping)

def clean_numeric_ranges(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    #Apply basic numeric range cleaning.
    #Values outside the allowed range are set to NaN.
    
    for col, (min_v, max_v) in rules.items():
        if col in df.columns:
            before_na = df[col].isna().sum()

            # Out-of-range values become NaN
            df.loc[(df[col] < min_v) | (df[col] > max_v), col] = pd.NA

            after_na = df[col].isna().sum()
            logging.info(
                f"Range check {col}: allowed [{min_v},{max_v}] | NaNs {before_na} -> {after_na}"
            )
    return df


#Data cleaning for music dataset
def clean_music(music_df: pd.DataFrame) -> pd.DataFrame:
    #Check that the required column (label) exists         the column that says the genre is called label
    required = ["label"]
    for col in required:
        if col not in music_df.columns:
            raise ValueError(f"Music dataset is missing required column: {col}")

    # Drop rows with missing genre 
    music_df = music_df.dropna(subset=["label"]).copy()

    # Normalize genre name  
    music_df["genre"] = normalize_genre(music_df["label"])

    # only numeric features 
    numeric_cols = music_df.select_dtypes(include="number").columns.tolist()

    # Some datasets may have numeric columns as strings, convert it if is possible
    for c in music_df.columns:
        if c not in ["filename", "label", "genre"] and c not in numeric_cols:
            # attempt numeric conversion silently
            music_df[c] = pd.to_numeric(music_df[c], errors="coerce")

    numeric_cols = music_df.select_dtypes(include="number").columns.tolist()

    # Final table: genre and numeric features
    keep = ["genre"] + numeric_cols
    music_df = music_df[keep].copy()

    logging.info(f"Music cleaned: {music_df.shape[0]} rows, {music_df.shape[1]} cols")
    return music_df


#Data cleaning for mental health survey dataset
def clean_survey(survey_df: pd.DataFrame) -> pd.DataFrame:
    #Check that the required column (fav genre) exists
    required = ["Fav genre"]
    for col in required:
        if col not in survey_df.columns:
            raise ValueError(f"Survey dataset is missing required column: {col}")

    # Drop rows with missing favorite genre
    survey_df = survey_df.dropna(subset=["Fav genre"]).copy()

    # Normalize genre name
    survey_df["genre"] = normalize_genre(survey_df["Fav genre"])

    # Convert relevant mental health columns to numeric if they exist
    mh_cols = ["Anxiety", "Depression", "Insomnia", "OCD", "Hours per day", "Age"]
    for c in mh_cols:
        if c in survey_df.columns:
            survey_df[c] = pd.to_numeric(survey_df[c], errors="coerce")

    # Clean numeric ranges for mental health indicators
    range_rules = {
        "Age": (0, 120),
        "Hours per day": (0, 24),
        "Anxiety": (0, 10),
        "Depression": (0, 10),
        "Insomnia": (0, 10),
        "OCD": (0, 10),
    }
    survey_df = clean_numeric_ranges(survey_df, range_rules)

    logging.info(f"Survey cleaned: {survey_df.shape[0]} rows, {survey_df.shape[1]} cols")
    return survey_df


#2)Group by genre

def group_music_by_genre(music_df: pd.DataFrame) -> pd.DataFrame:
    #group numeric music features by genre (mean)
    grouped = music_df.groupby("genre", as_index=False).mean(numeric_only=True)
    grouped = grouped.sort_values("genre").reset_index(drop=True)
    logging.info(f"Music aggregated by genre: {grouped.shape[0]} genres")
    return grouped


def group_mental_by_genre(survey_df: pd.DataFrame) -> pd.DataFrame:
    #group mental health indicators by genre (mean)
    cols = ["genre"]

    # Choose only numeric columns, the ones that are relevant
    candidates = ["Age", "Hours per day", "Anxiety", "Depression", "Insomnia", "OCD"]
    cols += [c for c in candidates if c in survey_df.columns]

    tmp = survey_df[cols].copy()
    grouped = tmp.groupby("genre", as_index=False).mean(numeric_only=True)
    grouped = grouped.sort_values("genre").reset_index(drop=True)
    logging.info(f"Mental health aggregated by genre: {grouped.shape[0]} genres")
    return grouped


def main():
    ensure_dirs()
    setup_logging()

    logging.info("Starting transform step (clean + aggregate)...")

    if not os.path.exists(MUSIC_PATH):
        logging.error(f"Missing file: {MUSIC_PATH}")
        raise SystemExit(1)

    if not os.path.exists(SURVEY_PATH):
        logging.error(f"Missing file: {SURVEY_PATH}")
        raise SystemExit(1)

    # 1) Load
    music_raw = pd.read_csv(MUSIC_PATH)
    survey_raw = pd.read_csv(SURVEY_PATH)
    logging.info(f"Loaded music raw: {music_raw.shape}, survey raw: {survey_raw.shape}")

    # 2) Clean
    music_clean = clean_music(music_raw)
    survey_clean = clean_survey(survey_raw)

    # 3) Group by genre
    music_by_genre = group_music_by_genre(music_clean)
    mental_by_genre = group_mental_by_genre(survey_clean)

    # 4) Save processed outputs
    music_by_genre.to_csv(OUT_MUSIC_BY_GENRE, index=False)
    mental_by_genre.to_csv(OUT_MENTAL_BY_GENRE, index=False)

    logging.info(f"Saved: {OUT_MUSIC_BY_GENRE}")
    logging.info(f"Saved: {OUT_MENTAL_BY_GENRE}")
    logging.info("Transform step finished successfully")

    print("SUCCESS: processed tables created in data/processed/")


if __name__ == "__main__":
    main()