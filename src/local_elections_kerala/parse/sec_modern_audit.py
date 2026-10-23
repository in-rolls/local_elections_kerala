"""Audit the retained 2010, 2015, and 2020 LSG profile collections."""

import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from local_elections_kerala.parse.to_parquet import modern_rows
from local_elections_kerala.tables import read_rows, write_rows

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "data"
OUT = DATA / "build/sec/modern"
SOURCES = {
    2010: ["raw/lsg_scrape/2010_2020_combined.csv"],
    2015: [
        "raw/lsg_scrape/2015_part1.csv",
        "raw/lsg_scrape/2015_part2_resumed_malappuram.csv",
    ],
    2020: ["raw/lsg_scrape/2020.csv"],
}
BODY_FIELD = {
    "Block": "block_raw",
    "Corporation": "corporation_raw",
    "Grama Panchayat": "grama_panchayat_raw",
    "Municipality": "municipality_raw",
}
SUFFIX = {
    " Grama Panchayat": "Grama Panchayat",
    " Block Panchayat": "Block",
    " Municipality": "Municipality",
    " Corporation": "Corporation",
    " District Panchayat": "District",
}
COMPARE_FIELDS = [
    "source_file",
    "source_row",
    "district_heading_raw",
    "body_heading_raw",
    "ward_no_raw",
    "saved_member_raw",
    "saved_party_raw",
    "match_status",
    "profile_member_raw",
    "profile_party_raw",
    "member_equal",
    "party_equal",
]


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def normalize(value):
    return re.sub(r"[^A-Z0-9]", "", (value or "").upper())


def body_key(row):
    tier = row["lgi_type"]
    if tier == "District":
        return None
    body = row[BODY_FIELD[tier]]
    return (normalize(row["district_raw"]), tier, normalize(body), row["ward_no_raw"])


def saved_body(heading):
    for suffix, tier in SUFFIX.items():
        if heading.endswith(suffix):
            return tier, heading.removesuffix(suffix)
    return None, None


def main():
    year_rows = {}
    audit = {"years": {}}
    for year, files in SOURCES.items():
        rows = [row for name in files for row in modern_rows(DATA / name, year)]
        year_rows[year] = rows
        key_counts = Counter(body_key(row) for row in rows if body_key(row))
        audit["years"][str(year)] = {
            "sources": [
                {"file": name, "sha256": sha256(DATA / name)} for name in files
            ],
            "rows": len(rows),
            "tiers": dict(Counter(row["lgi_type"] for row in rows)),
            "empty_listing_name": sum(not row["elected_member_raw"] for row in rows),
            "empty_party": sum(not row["party_raw"] for row in rows),
            "empty_reservation": sum(not row["reserved_raw"] for row in rows),
            "listing_profile_ward_mismatch": sum(
                row["ward_no_raw"] != row["profile_ward_no_raw"] for row in rows
            ),
            "woman_reserved_male_profile": sum(
                "Woman" in (row["reserved_raw"] or "")
                and row["winner_woman_raw"] == "Male"
                for row in rows
            ),
            "duplicate_non_district_body_ward_keys": sum(
                count - 1 for count in key_counts.values() if count > 1
            ),
        }
    profile_index = defaultdict(list)
    for row in year_rows[2010]:
        key = body_key(row)
        if key:
            profile_index[key].append(row)
    saved = OUT / "2010_saved_ward_listings.parquet"
    saved_rows = read_rows(saved)
    comparison = []
    for row in saved_rows:
        tier, body = saved_body(row["body_heading_raw"])
        matches = (
            profile_index.get(
                (
                    normalize(row["district_heading_raw"]),
                    tier,
                    normalize(body),
                    row["ward_no_raw"],
                ),
                [],
            )
            if tier
            else []
        )
        result = {
            "source_file": row["source_file"],
            "source_row": row["source_row"],
            "district_heading_raw": row["district_heading_raw"],
            "body_heading_raw": row["body_heading_raw"],
            "ward_no_raw": row["ward_no_raw"],
            "saved_member_raw": row["elected_member_raw"],
            "saved_party_raw": row["party_raw"],
            "match_status": None,
            "profile_member_raw": None,
            "profile_party_raw": None,
            "member_equal": None,
            "party_equal": None,
        }
        if not tier:
            result["match_status"] = "unknown_body_tier"
        elif tier == "District":
            result["match_status"] = "district_label_defect"
        elif not matches:
            result["match_status"] = "no_exact_body_ward_key"
        elif len(matches) > 1:
            result["match_status"] = "duplicate_profile_key"
        else:
            match = matches[0]
            result["match_status"] = "joined"
            result["profile_member_raw"] = match["elected_member_raw"]
            result["profile_party_raw"] = match["party_raw"]
            result["member_equal"] = normalize(row["elected_member_raw"]) == normalize(
                match["elected_member_raw"]
            )
            result["party_equal"] = normalize(row["party_raw"]) == normalize(
                match["party_raw"]
            )
        comparison.append(result)
    output = OUT / "2010_saved_comparison.parquet"
    write_rows(output, comparison, COMPARE_FIELDS)
    audit["2010_saved_html"] = {
        "source_sha256": sha256(saved),
        "comparison_sha256": sha256(output),
        "rows": len(saved_rows),
        "match_status": dict(Counter(row["match_status"] for row in comparison)),
        "member_equal": sum(row["member_equal"] is True for row in comparison),
        "party_equal": sum(row["party_equal"] is True for row in comparison),
    }
    (OUT / "modern_sources.audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(audit, sort_keys=True))


if __name__ == "__main__":
    main()
