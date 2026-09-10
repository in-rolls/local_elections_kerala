"""Parse saved 2010 ward-list tables offline, retaining file and row provenance."""

import argparse
import json
from pathlib import Path
from urllib.parse import urljoin

import pyarrow as pa
from bs4 import BeautifulSoup
from to_parquet import digest, write_table

HEADERS = ["Ward No.", "Ward Name", "Elected Members", "Role", "Party", "Reservation"]
COLUMNS = [
    "ward_no_raw",
    "ward_name_raw",
    "elected_member_raw",
    "role_raw",
    "party_raw",
    "reservation_raw",
]
SCHEMA = pa.schema(
    [
        ("source_file", pa.string()),
        ("source_row", pa.int32()),
        *[(name, pa.string()) for name in COLUMNS],
        ("member_url", pa.string()),
    ]
)


def parse_table(html):
    """Select the election table by its headers, ignoring navigation tables."""
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find(id="block-zircon-content")
    if content is None:
        raise ValueError("Missing election content block")
    tables = []
    for table in content.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        first = [
            cell.get_text(" ", strip=True)
            for cell in rows[0].find_all(["th", "td"], recursive=False)
        ]
        if first == HEADERS:
            tables.append(rows[1:])
    if len(tables) != 1:
        raise ValueError(f"Expected one ward table, found {len(tables)}")
    records = []
    for number, row in enumerate(tables[0], 1):
        cells = row.find_all(["th", "td"], recursive=False)
        if len(cells) != len(COLUMNS):
            raise ValueError(f"Row {number}: unexpected column count")
        record = {
            name: cell.get_text(" ", strip=True) or None
            for name, cell in zip(COLUMNS, cells, strict=True)
        }
        if not (record["ward_no_raw"] or "").isdigit():
            raise ValueError(f"Row {number}: missing ward number")
        link = cells[2].find("a", href=True)
        record["member_url"] = (
            urljoin("https://lsgkerala.gov.in", link["href"]) if link else None
        )
        records.append(record)
    if not records:
        raise ValueError("Empty ward table")
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    records, receipts, failures = [], [], []
    for path in args.inputs:
        try:
            parsed = parse_table(path.read_text(encoding="utf-8"))
            records.extend(
                row | {"source_file": str(path), "source_row": n}
                for n, row in enumerate(parsed, 1)
            )
            receipts.append(
                {"file": str(path), "sha256": digest(path), "rows": len(parsed)}
            )
        except (ValueError, OSError) as exc:
            failures.append({"file": str(path), "error": str(exc)})
    report = args.out.with_suffix(".manifest.json")
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        json.dumps({"sources": receipts, "failures": failures}, indent=2) + "\n"
    )
    if failures:
        parser.exit(
            1, f"{len(failures)} files failed; output was not replaced; see {report}\n"
        )
    write_table(args.out, pa.Table.from_pylist(records, schema=SCHEMA))
    print(f"{len(records):,} rows from {len(receipts)} source files")


if __name__ == "__main__":
    main()
