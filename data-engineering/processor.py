# import os
# import re
# from isort import file
# import pandas as pd

# DOWNLOAD_DIR = "downloads"
# PROCESSED_DIR = "processed"
# OUTPUT_PATH = os.path.join(PROCESSED_DIR, "mean_trip_time_by_quarter.csv")

# # Extract year and quarter from filename
# def get_year_quarter(filename: str):
#     m = re.search(r"Divvy_Trips_(\d{4})_(Q[1-4])", filename)
#     if m:
#         return int(m.group(1)), m.group(2)
#     return None, None


# def main():
#     # 1) Comprobar que existe downloads 
#     if not os.path.isdir(DOWNLOAD_DIR):
#         print(f"No existe la carpeta '{DOWNLOAD_DIR}'. Ejecuta primero: python problem1.py")
#         return

#     # 2) Crear carpeta processed si no existe
#     os.makedirs(PROCESSED_DIR, exist_ok=True)

#     results = []  # aquí guardaremos filas con year, quarter, mean

#     # 3) Recorrer todos los CSV que haya en downloads
#     for file in os.listdir(DOWNLOAD_DIR):
#         if not file.lower().endswith(".csv"):
#             continue

#         path = os.path.join(DOWNLOAD_DIR, file)

#         # 4) Leer el CSV
#         df = pd.read_csv(path)

#         # 5) Calcular duración media
#         # En estos datasets suele existir "tripduration" y normalmente está en segundos.
#         if "tripduration" in df.columns:
#             duration_min = pd.to_numeric(df["tripduration"], errors="coerce") / 60
#         elif "started_at" in df.columns and "ended_at" in df.columns:
#             start = pd.to_datetime(df["started_at"], errors="coerce")
#             end = pd.to_datetime(df["ended_at"], errors="coerce")
#             duration_min = (end - start).dt.total_seconds() / 60
#         else:
#             print(f"Saltando {file}: no se puede calcular la duración")
#             continue

#         # Convertimos a número por si hay valores raros. Los que no se puedan convertir -> NaN
#         duration_sec = pd.to_numeric(df["tripduration"], errors="coerce")

#         # Quitamos nulos
#         duration_sec = duration_sec.dropna()

#         # Pasamos a minutos
#         duration_min = duration_sec / 60

#         mean_minutes = duration_min.mean()

#         # 6) Sacar year y quarter del nombre del archivo
#         year, quarter = get_year_quarter(file)

#         results.append({
#             "year": year,
#             "quarter": quarter,
#             "mean_trip_time_minutes": mean_minutes,
#             "file": file
#         })

#         print(f"OK {file} -> mean = {mean_minutes:.2f} min")

#     # 7) Guardar resultados en CSV dentro de processed
#     out = pd.DataFrame(results)

#     # Ordenar por año y quarter si se pudo extraer bien
#     quarter_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
#     if "year" in out.columns and "quarter" in out.columns:
#         out["q_num"] = out["quarter"].map(quarter_order)
#         out = out.sort_values(["year", "q_num"]).drop(columns=["q_num"])

#     out.to_csv(OUTPUT_PATH, index=False)
#     print("\nGuardado:", OUTPUT_PATH)
#     print(out.to_string(index=False))


# if __name__ == "__main__":
#     main()


import os
import re
import pandas as pd

DOWNLOAD_DIR = "downloads"
PROCESSED_DIR = "processed"
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "mean_trip_time_by_quarter.csv")

# Extract year and quarter from filename
def get_year_quarter(filename: str):
    m = re.search(r"Divvy_Trips_(\d{4})_(Q[1-4])", filename)
    if m:
        return int(m.group(1)), m.group(2)
    return None, None


def main():
    # Check that the downloads folder exists
    if not os.path.isdir(DOWNLOAD_DIR):
        print(f"No existe '{DOWNLOAD_DIR}'. Ejecuta primero: python problem1.py")
        return

    # Create processed if it does not exist
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    results = []

    # Read all CSV files in downloads
    for file in os.listdir(DOWNLOAD_DIR):
        if not file.lower().endswith(".csv"):
            continue

        path = os.path.join(DOWNLOAD_DIR, file)
        df = pd.read_csv(path, low_memory=False)

        # Calculate mean trip duration in minutes
        if "tripduration" in df.columns:
            duration_min = pd.to_numeric(df["tripduration"], errors="coerce") / 60

        elif "01 - Rental Details Duration In Seconds Uncapped" in df.columns:
            # Q2 doen't have tripduration, but has this other column in seconds
            duration_sec = (
                df["01 - Rental Details Duration In Seconds Uncapped"]
                .astype(str)
                .str.replace(",", "", regex=False)
            )
            duration_min = pd.to_numeric(duration_sec, errors="coerce") / 60

        #some Q files dont have tripduration, have started_at and ended_at columns
        elif "started_at" in df.columns and "ended_at" in df.columns:
            start = pd.to_datetime(df["started_at"], errors="coerce")
            end = pd.to_datetime(df["ended_at"], errors="coerce")
            duration_min = (end - start).dt.total_seconds() / 60

        else:
            # If there is no way to calculate duration, we skip the file.
            print(f"Saltando {file}: no se puede calcular la duración (faltan columnas)")
            continue

        # Clean nulls
        duration_min = duration_min.dropna()

        # Remove invalid values
        duration_min = duration_min[(duration_min >= 0) & (duration_min <= 24 * 60)]

        if duration_min.empty:
            print(f"Saltando {file}: duración vacía tras limpiar datos")
            continue

        mean_minutes = duration_min.mean()

        year, quarter = get_year_quarter(file)

        results.append({
            "year": year,
            "quarter": quarter,
            "mean_trip_time_minutes": mean_minutes,
            "file": file
        })

        print(f"OK {file} -> mean = {mean_minutes:.2f} min")

    # Save results to CSV in processed
    out = pd.DataFrame(results)

    # Order by year and quarter
    quarter_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    if not out.empty:
        out["q_num"] = out["quarter"].map(quarter_order)
        out = out.sort_values(["year", "q_num"]).drop(columns=["q_num"])

    out.to_csv(OUTPUT_PATH, index=False)
    print("\nGuardado:", OUTPUT_PATH)
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
