"""Inventory and parse the saved 2010 LSG ward-list pages offline."""

import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

from bs4 import BeautifulSoup

from local_elections_kerala.parse.parse_html import parse_table
from local_elections_kerala.tables import write_rows

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "data/raw/lsg_scrape/2010_html"
OUT = ROOT / "data/interim/sec/modern"
BODY_ID = re.compile(r"/standcommitee/2010/(\d+)")
FIELDS = [
    "body_id",
    "district_heading_raw",
    "body_heading_raw",
    "source_file",
    "source_row",
    "ward_no_raw",
    "ward_name_raw",
    "elected_member_raw",
    "role_raw",
    "party_raw",
    "reserved_raw",
    "member_url",
]


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def parse_file(path):
    html = path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    anchor = soup.find("a", href=re.compile(r"/electlbrpt/"))
    body_anchor = soup.find("a", href=BODY_ID)
    if anchor is None or body_anchor is None:
        raise ValueError("Missing election body heading or ID")
    body_id = BODY_ID.search(body_anchor["href"]).group(1)
    heading = anchor.get_text(" ", strip=True)
    if " - " not in heading:
        raise ValueError("Body heading lacks district separator")
    district, body = heading.split(" - ", 1)
    return body_id, district, body, parse_table(html)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()
    records = []
    inventory = []
    seen = {}
    for path in sorted(args.source.glob("*.html")):
        item = {"file": str(path.relative_to(ROOT)), "sha256": sha256(path)}
        try:
            body_id, district, body, rows = parse_file(path)
            item.update(body_id=body_id, district=district, body=body, rows=len(rows))
            content = [(x[k] for k in x) for x in rows]
            signature = [tuple(row) for row in content]
            if body_id in seen:
                item["status"] = (
                    "duplicate_identical"
                    if signature == seen[body_id]
                    else "duplicate_conflict"
                )
            else:
                seen[body_id] = signature
                item["status"] = "parsed"
                records.extend(
                    {
                        "body_id": body_id,
                        "district_heading_raw": district,
                        "body_heading_raw": body,
                        "source_file": item["file"],
                        "source_row": i,
                        **row,
                    }
                    for i, row in enumerate(rows, 1)
                )
        except (OSError, ValueError) as exc:
            item.update(status="failed", error=str(exc))
        inventory.append(item)
    args.out.mkdir(parents=True, exist_ok=True)
    output = args.out / "2010_saved_ward_listings.parquet"
    write_rows(output, records, FIELDS)
    report = {
        "source_directory": str(args.source.relative_to(ROOT)),
        "source_files": len(inventory),
        "source_status": dict(Counter(x["status"] for x in inventory)),
        "parsed_body_ids": len(seen),
        "parsed_rows": len(records),
        "output_sha256": sha256(output),
        "files": inventory,
    }
    (args.out / "2010_saved_html.audit.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({k: v for k, v in report.items() if k != "files"}, sort_keys=True))


if __name__ == "__main__":
    main()
