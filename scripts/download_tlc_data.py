"""Download NYC TLC trip data and upload to GCS."""

import os
import requests
from google.cloud import storage

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
GCS_BUCKET = os.environ.get("GCS_RAW_BUCKET", "taxi-raw-dev")

DATASETS = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
}


def download_and_upload(trip_type: str, year: int, month: int):
    """Download a single month of data and upload to GCS."""
    prefix = DATASETS[trip_type]
    filename = f"{prefix}_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"

    print(f"Downloading {url}...")
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{trip_type}/{year}/{month:02d}/{filename}")
    blob.upload_from_string(resp.content, content_type="application/octet-stream")
    print(f"Uploaded to gs://{GCS_BUCKET}/{blob.name}")


if __name__ == "__main__":
    import sys
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2024
    months = range(1, 13)

    for month in months:
        for trip_type in ["yellow", "green"]:
            try:
                download_and_upload(trip_type, year, month)
            except Exception as e:
                print(f"Failed {trip_type} {year}-{month}: {e}")
