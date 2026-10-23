"""Write data/raw/fetch_log.csv: one row per raw file, per docs/layout.md.

    python -m local_elections_kerala.build.fetch_log          # write
    python -m local_elections_kerala.build.fetch_log --check  # compare only

The SEC PDFs' rows come from their acquisition receipts
(data/search_log/sec/<collection>/<document>.receipt.json), which stay the
record the collector writes. The LSG portal scrape files have no per-file
record: the collector's requests and capture times were not kept, so their
`url` is blank and `method` is `unknown` rather than reconstructed.
"""

import argparse
import csv
import hashlib
import io
import json
import sys

from local_elections_kerala.paths import RAW, ROOT, SCRAPE, SEC, receipt_for

LOG = RAW / "fetch_log.csv"
COLUMNS = [
    "path",
    "url",
    "sha256",
    "bytes",
    "fetched_at_utc",
    "method",
    "referrer",
    "source",
]
SCRAPED = "lsgkerala.gov.in, saved by the original scraper; requests not recorded"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def rows():
    out = []
    for pdf in sorted(SEC.rglob("*.pdf")):
        receipt = json.loads(receipt_for(pdf).read_text(encoding="utf-8"))
        if receipt.get("status") != "ok" or receipt["sha256"] != digest(pdf):
            raise SystemExit(f"{pdf}: no successful receipt for these bytes")
        url = receipt["url"]
        out.append(
            {
                "path": pdf.relative_to(ROOT).as_posix(),
                "url": url,
                "sha256": receipt["sha256"],
                "bytes": receipt["bytes"],
                "fetched_at_utc": receipt["finished_at"],
                "method": "wayback" if "web.archive.org" in url else "live",
                "referrer": receipt.get("source_url", ""),
                "source": "",
            }
        )
    for path in sorted(p for p in SCRAPE.rglob("*") if p.is_file()):
        out.append(
            {
                "path": path.relative_to(ROOT).as_posix(),
                "url": "",
                "sha256": digest(path),
                "bytes": path.stat().st_size,
                "fetched_at_utc": "",
                "method": "unknown",
                "referrer": "",
                "source": SCRAPED,
            }
        )
    return sorted(out, key=lambda r: r["path"])


def render(table):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(table)
    return buffer.getvalue()


def main():
    parser = argparse.ArgumentParser(description="Write the raw-file fetch log.")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    text = render(rows())
    if args.check:
        if not LOG.exists() or LOG.read_text(encoding="utf-8") != text:
            print(f"{LOG.relative_to(ROOT)} is out of date; run without --check")
            return 1
        print(f"{LOG.relative_to(ROOT)}: up to date")
        return 0
    LOG.write_text(text, encoding="utf-8")
    print(f"wrote {LOG.relative_to(ROOT)} ({text.count(chr(10)) - 1} files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
