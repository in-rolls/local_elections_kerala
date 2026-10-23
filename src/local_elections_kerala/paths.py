"""Repository paths, in one place so every module agrees on them.

The folders follow in-rolls/local_elections docs/layout.md.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RAW = DATA / "raw"
SCRAPE = RAW / "lsg_scrape"
SEC = RAW / "sec"
# Acquisition receipts, attempt logs and coverage for each SEC collection,
# one folder per collection under raw/sec/.
RECORDS = DATA / "search_log" / "sec"
BUILD = DATA / "build" / "sec"
RELEASE = DATA / "release"


def receipt_for(pdf):
    """Where the acquisition receipt for a raw SEC PDF lives."""
    pdf = Path(pdf)
    return RECORDS / pdf.parent.name / f"{pdf.stem}.receipt.json"
