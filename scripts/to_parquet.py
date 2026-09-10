"""Export Kerala's historical CSVs without losing duplicate column names."""

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATA = Path(__file__).resolve().parent.parent / "data"
GEO_HEADERS = [
    "Year",
    "LGI Type",
    "District",
    "Block",
    "Municipality",
    "Corporation",
    "Grama Panchayat",
]
DETAIL_HEADERS = [
    "Ward No.",
    "Ward Name",
    "Elected Members",
    "Role",
    "Party",
    "Reservation",
    "Ward No.",
    "Ward Name",
    "Name of Member",
    "Address",
    "Phone",
    "Mobile",
    "Age",
    "Female/Male",
    "Marital Status",
    "Educational Qualification",
    "Occupation",
    "Image",
]
GEO = [
    "year",
    "lgi_type",
    "district_raw",
    "block_raw",
    "municipality_raw",
    "corporation_raw",
    "grama_panchayat_raw",
]
DETAIL = [
    "ward_no_raw",
    "ward_name_raw",
    "elected_member_raw",
    "role_raw",
    "party_raw",
    "reservation_raw",
    "profile_ward_no_raw",
    "profile_ward_name_raw",
    "member_name_raw",
    "address_raw",
    "phone_raw",
    "mobile_raw",
    "age_raw",
    "sex_raw",
    "marital_status_raw",
    "education_raw",
    "occupation_raw",
    "image_url",
]
OLD_DETAIL = ["ward_no_raw", "elected_member_raw", "front_raw", "votes_raw"]
SOURCES = (
    ("lsgi-election-kerala.csv", 2010),
    ("lsgi-election-kerala-2015.csv", 2015),
    ("lsgi-election-kerala-2015-fix.csv", 2015),
    ("lsgi-election-kerala-2020.csv", 2020),
)


def schema(old=False):
    fields = GEO + ([*OLD_DETAIL, "body_heading_raw"] if old else DETAIL)
    return pa.schema(
        [pa.field(n, pa.int16() if n == "year" else pa.string()) for n in fields]
        + [
            pa.field("source_file", pa.string(), nullable=False),
            pa.field("source_row", pa.int32(), nullable=False),
            pa.field(
                "quality_flags",
                pa.list_(pa.field("element", pa.string())),
                nullable=False,
            ),
        ]
    )


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_csv(path):
    """Read by position so both ward/profile column pairs survive."""
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        headers = next(reader, None)
        rows = list(reader)
    if headers is None or not rows or any(len(row) != len(headers) for row in rows):
        raise ValueError(f"{path}: empty or ragged CSV")
    return headers, rows


def flags(row):
    result = []
    if row["lgi_type"] in {"District", "Corporation"}:
        result.append("district_label_carried_from_first_body")
    return result


def modern_rows(path, year):
    header, rows = read_csv(path)
    expected = GEO_HEADERS + DETAIL_HEADERS
    # pandas renamed the profile columns when writing the combined CSV.
    if path.name == "lsgi-election-kerala.csv":
        expected[13:15] = ["Ward No..1", "Ward Name.1"]
    if header != expected:
        raise ValueError(f"{path}: unexpected header")
    result = []
    for number, values in enumerate(rows, 1):
        if values[0] not in {"2010", "2015", "2020"}:
            raise ValueError(f"{path}:{number}: unexpected year")
        if int(values[0]) != year:
            if path.name != "lsgi-election-kerala.csv":
                raise ValueError(f"{path}:{number}: unexpected year in annual source")
            continue
        row = {
            key: value or None for key, value in zip(GEO + DETAIL, values, strict=True)
        }
        row["year"] = year
        row.update(source_file=path.name, source_row=number, quality_flags=flags(row))
        if row["ward_no_raw"] != row["profile_ward_no_raw"]:
            row["quality_flags"].append("ward_profile_mismatch")
        result.append(row)
    if not result:
        raise ValueError(f"{path}: no {year} observations")
    return result


def old_rows(path):
    header, rows = read_csv(path)
    if header != [*GEO_HEADERS, "0", "1", "2", "3"]:
        raise ValueError(f"{path}: unexpected 2005 header")
    result, excluded = [], {"body_heading": 0, "column_header": 0}
    heading = None
    for number, values in enumerate(rows, 1):
        if values[0] != "2005":
            raise ValueError(f"{path}:{number}: unexpected year")
        cells = values[7:]
        if len(set(cells)) == 1 and cells[0]:
            heading = cells[0]
            excluded["body_heading"] += 1
            continue
        if cells == ["Ward No", "Elected Members", "Front", "Votes"]:
            excluded["column_header"] += 1
            continue
        if not re.fullmatch(r"[0-9]+", cells[0]) or heading is None:
            raise ValueError(f"{path}:{number}: unrecognized 2005 row shape")
        row = {
            key: value or None
            for key, value in zip(GEO + OLD_DETAIL, values, strict=True)
        }
        row["year"] = 2005
        row.update(
            body_heading_raw=heading,
            source_file=path.name,
            source_row=number,
            quality_flags=[*flags(row), "reservation_not_collected"],
        )
        result.append(row)
    if not result:
        raise ValueError(f"{path}: no 2005 seat observations")
    return result, excluded


def write_table(path, table):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".parquet.part")
    try:
        pq.write_table(table, temporary, compression="zstd")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def export(data, out, check=False):
    modern = []
    for name, year in SOURCES:
        modern.extend(modern_rows(data / name, year))
    old_name = "lsgi-election-kerala-2005.csv"
    old, excluded = old_rows(data / old_name)
    tables = {
        "kerala_2005.parquet": pa.Table.from_pylist(old, schema=schema(True)),
        "kerala_2010_2020.parquet": pa.Table.from_pylist(modern, schema=schema()),
    }
    source_names = sorted({old_name, *(name for name, _ in SOURCES)})
    manifest = {
        "format_version": 1,
        "sources": [
            {"file": name, "sha256": digest(data / name)} for name in source_names
        ],
        "excluded_2005_structural_rows": excluded,
        "files": [],
    }
    for name, table in tables.items():
        path = out / name
        if check:
            if not table.equals(pq.read_table(path), check_metadata=True):
                raise ValueError(f"{path}: schema or rows differ from source CSVs")
        else:
            write_table(path, table)
        manifest["files"].append(
            {
                "file": name,
                "rows": table.num_rows,
                "sha256": digest(path),
                "schema": [
                    {"name": f.name, "type": str(f.type), "nullable": f.nullable}
                    for f in table.schema
                ],
            }
        )
        print(f"{name}: {table.num_rows:,} records")
    path = out / "MANIFEST.json"
    if check:
        if json.loads(path.read_text()) != manifest:
            raise ValueError(f"{path}: manifest differs from source or export files")
    else:
        temporary = path.with_suffix(".json.part")
        temporary.write_text(json.dumps(manifest, indent=2) + "\n")
        temporary.replace(path)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DATA)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        export(args.data, args.out or args.data / "fin", args.check)
    except (ValueError, OSError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
