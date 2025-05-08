#!/usr/bin/env python3
"""
Download photos listed in a CSV and rename them
<participant_code>_<yyyymmdd_HHMMSS>.jpg
On windows, install miniconda first.
Then run this script in the Anaconda Prompt or in VS Code (or other IDE) with the
Python interpreter set to the Anaconda environment.
"""

import os
import pathlib
import re
import requests
import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
SOURCE_CSV    = "photos.csv"           # your CSV file
URL_COL       = "photo_url"
ID_COL        = "participant_code"
DATE_COL      = "diary_date"
TIME_COL      = "meal_consume_time"
OUT_DIR       = pathlib.Path("pics")   # folder to save images
CHUNK_SIZE    = 8192                   # bytes per read
TIMEOUT       = 30                     # seconds per request
# ──────────────────────────────────────────────────────────────────────────────

SAFE = re.compile(r"[^\w\-]")          # keep filenames alphanumeric/‑/_ only


def safe_name(text: str) -> str:
    """Replace unsafe filename chars with '_'."""
    return SAFE.sub("_", text)


def as_timestamp(date_str: str, time_str: str) -> str:
    # note dayfirst=True  ↓↓↓
    return (
        pd.to_datetime(f"{date_str} {time_str}", dayfirst=True, errors="raise")
          .strftime("%Y%m%d_%H%M%S")
    )



def fetch(url: str, dest: pathlib.Path):
    """Stream the file to disk with basic error handling."""
    r = requests.get(url, stream=True, timeout=TIMEOUT)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for block in r.iter_content(CHUNK_SIZE):
            f.write(block)


def main():
    OUT_DIR.mkdir(exist_ok=True)

    df = (
        pd.read_csv(SOURCE_CSV)
          .dropna(subset=[URL_COL])          # skip rows with no photo URL
    )

    for _, row in df.iterrows():
        try:
            sid   = safe_name(str(row[ID_COL]))
            stamp = as_timestamp(row[DATE_COL], row[TIME_COL])
            fname = f"{sid}_{stamp}.jpg"
            path  = OUT_DIR / fname

            fetch(row[URL_COL], path)
            print(f"✔  {fname}")
        except Exception as err:
            print(f"✘  {row.get(ID_COL, '??')} – {err}")


if __name__ == "__main__":
    main()
