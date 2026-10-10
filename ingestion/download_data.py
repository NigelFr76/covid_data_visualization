"""Download OWID COVID-19 CSV, falling back to the local cache or test fixture."""

from __future__ import annotations

import logging
import os
import shutil
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
FILE_PATH = os.path.join(DATA_DIR, "owid-covid-data.csv")
FIXTURE_PATH = os.path.join(DATA_DIR, "fixtures", "covid_small.csv")

os.makedirs(DATA_DIR, exist_ok=True)


def resolve_input_path(prefer_download: bool = True) -> str:
    if os.path.exists(FILE_PATH) and not prefer_download:
        return FILE_PATH
    if prefer_download:
        try:
            logger.info("Downloading COVID-19 data from %s", URL)
            urllib.request.urlretrieve(URL, FILE_PATH)
            logger.info("Download complete: %s", FILE_PATH)
            return FILE_PATH
        except Exception as exc:
            logger.warning("Download failed (%s). Using local fallback.", exc)
    if os.path.exists(FILE_PATH):
        logger.info("Using cached CSV: %s", FILE_PATH)
        return FILE_PATH
    if os.path.exists(FIXTURE_PATH):
        logger.info("Using offline fixture: %s", FIXTURE_PATH)
        return FIXTURE_PATH
    raise FileNotFoundError("No OWID CSV cache and no fixture available.")


def download_data() -> str:
    return resolve_input_path(prefer_download=True)


if __name__ == "__main__":
    path = download_data()
    logger.info("Data ready at %s (%s bytes)", path, os.path.getsize(path))
    if os.path.exists(FIXTURE_PATH):
        dest = os.path.join(DATA_DIR, "fixture-covid-data.csv")
        shutil.copyfile(FIXTURE_PATH, dest)
