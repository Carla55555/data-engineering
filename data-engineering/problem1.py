import os
import logging
import requests
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_DIR = "downloads"

# If the folder does not exist, create it
def ensure_download_dir():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print("Folder created:", DOWNLOAD_DIR)
    else:
        print("Folder already exists:", DOWNLOAD_DIR)


# Downloads a zip file and saves it in DOWNLOAD_DIR.
def download_file(url: str):
    # Extract the file name from the URL (everything after the last "/")
    filename = url.rsplit("/", 1)[-1]
    # Build the full path where the file will be saved
    path = os.path.join(DOWNLOAD_DIR, filename)
    try:
        # Send HTTP request to download the file
        r = requests.get(url, timeout=20)
        # Raise an exception if status code is not 200 (e.g. 404 not found)
        r.raise_for_status()
        # Save the file content in binary mode
        with open(path, "wb") as f:
            f.write(r.content)
        print("Downloaded:", filename)
        return path
    except Exception as e:
        # If something goes wrong (bad URL, timeout, etc.)
        print("Error downloading:", url, "-", e)
        return None


# Extracts the zip file into DOWNLOAD_DIR and deletes the zip file.
def unzip_and_delete(zip_path: str):
    try:
        # Open the .zip file safely
        with zipfile.ZipFile(zip_path, "r") as z:
            # Extract all contents into the download folder
            z.extractall(DOWNLOAD_DIR)
        # Remove the .zip file after extraction to save space
        os.remove(zip_path)
        print("Extracted and deleted:", os.path.basename(zip_path))
    except Exception as e:
        # If the file is not a valid zip or cannot be opened
        print("Could not open ZIP:", zip_path, "-", e)


# Downloads each URI and processes it (unzip + delete)
def process_all():
    for url in download_uris:
        path = download_file(url)
        if path:  # only if it was downloaded successfully
            unzip_and_delete(path)


def main() -> None:
    # Ensure the target folder exists before downloading
    ensure_download_dir()
    # Loop through all URIs and process them
    process_all()


if __name__ == "__main__":
    main()