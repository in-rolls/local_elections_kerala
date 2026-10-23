"""Compare SEC PDF winners with the retained LSG 2005 winner listing."""

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from local_elections_kerala.tables import read_rows, write_rows

ROOT = Path(__file__).resolve().parents[3]
OLD = ROOT / "data/raw/lsg_scrape/2005.csv"
DERIVED = ROOT / "data/build/sec/2005"
COLUMNS = [
    "document_id",
    "district",
    "lgi_type",
    "pdf_body_no",
    "pdf_body_name",
    "old_body_heading",
    "ward_no",
    "pdf_winner",
    "old_winner",
    "pdf_votes",
    "old_votes",
    "pdf_ward_validated",
    "comparison_status",
]


def norm(value):
    return re.sub(r"[^a-z0-9]", "", value.lower())


def old_winners(path):
    by_body = defaultdict(dict)
    heading = None
    with path.open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            if row["Year"] != "2005":
                raise ValueError("unexpected year in original CSV")
            cells = [row[str(i)] for i in range(4)]
            if len(set(cells)) == 1 and cells[0]:
                heading = cells[0]
            elif cells == ["Ward No", "Elected Members", "Front", "Votes"]:
                continue
            elif row["0"].isdigit() and heading:
                district = heading.rpartition(",")[2].strip()
                key = (district, row["LGI Type"], heading)
                ward_no = int(row["0"])
                if ward_no in by_body[key]:
                    raise ValueError(f"duplicate original ward: {key}, {ward_no}")
                by_body[key][ward_no] = {"name": row["1"], "votes": row["3"]}
            else:
                raise ValueError(f"unrecognized original CSV row: {row}")
    return by_body


def pdf_winners(paths):
    by_body = defaultdict(dict)
    for path in paths:
        for row in read_rows(path):
            if row["status"] != "Elected":
                continue
            tier = (
                "District"
                if row["lgi_type"] == "District Panchayat"
                else row["lgi_type"]
            )
            key = (row["district"], tier, row["document_id"], int(row["body_no"]))
            ward_no = int(row["ward_no"])
            if ward_no in by_body[key]:
                raise ValueError(f"duplicate PDF winner: {key}, {ward_no}")
            by_body[key][ward_no] = row
    return by_body


def find_matches(pdf, old):
    mappings = {}
    ambiguous = {}
    for pdf_key, pdf_wards in pdf.items():
        district, tier, _, _ = pdf_key
        scores = []
        for old_key, old_wards in old.items():
            if old_key[:2] != (district, tier):
                continue
            matches = sum(
                norm(row["candidate_name"]) == norm(old_wards[ward]["name"])
                and row["votes"] == old_wards[ward]["votes"]
                for ward, row in pdf_wards.items()
                if ward in old_wards
            )
            if matches:
                scores.append((matches, old_key))
        scores.sort(reverse=True)
        if (
            scores
            and scores[0][0] >= 2
            and (len(scores) == 1 or scores[0][0] > scores[1][0])
        ):
            mappings[pdf_key] = scores[0][1]
        else:
            ambiguous[pdf_key] = scores[:3]
    duplicates = [key for key, count in Counter(mappings.values()).items() if count > 1]
    for pdf_key, old_key in list(mappings.items()):
        if old_key in duplicates:
            ambiguous[pdf_key] = [(0, old_key)]
            del mappings[pdf_key]
    return mappings, ambiguous


def compare(pdf, old, mappings):
    rows = []
    for pdf_key, wards in pdf.items():
        old_key = mappings.get(pdf_key)
        old_wards = old.get(old_key, {})
        for ward_no, pdf_row in wards.items():
            old_row = old_wards.get(ward_no, {})
            if not old_key:
                status = "body_unmatched"
            elif not old_row:
                status = "ward_missing_from_old"
            elif not old_row["votes"]:
                status = "old_votes_missing"
            elif not old_row["name"]:
                status = "old_winner_missing"
            elif (
                norm(pdf_row["candidate_name"]) == norm(old_row["name"])
                and pdf_row["votes"] == old_row["votes"]
            ):
                status = "match"
            elif pdf_row["votes"] != old_row["votes"]:
                status = "vote_disagreement"
            else:
                status = "name_disagreement"
            rows.append(
                {
                    "document_id": pdf_key[2],
                    "district": pdf_key[0],
                    "lgi_type": pdf_key[1],
                    "pdf_body_no": pdf_key[3],
                    "pdf_body_name": pdf_row["body_name"],
                    "old_body_heading": old_key[2] if old_key else "",
                    "ward_no": ward_no,
                    "pdf_winner": pdf_row["candidate_name"],
                    "old_winner": old_row.get("name", ""),
                    "pdf_votes": pdf_row["votes"],
                    "old_votes": old_row.get("votes", ""),
                    "pdf_ward_validated": pdf_row["ward_validated"],
                    "comparison_status": status,
                }
            )
        for ward_no, old_row in old_wards.items():
            if ward_no not in wards:
                rows.append(
                    {
                        "document_id": pdf_key[2],
                        "district": pdf_key[0],
                        "lgi_type": pdf_key[1],
                        "pdf_body_no": pdf_key[3],
                        "pdf_body_name": "",
                        "old_body_heading": old_key[2],
                        "ward_no": ward_no,
                        "pdf_winner": "",
                        "old_winner": old_row["name"],
                        "pdf_votes": "",
                        "old_votes": old_row["votes"],
                        "pdf_ward_validated": "",
                        "comparison_status": "ward_missing_from_pdf",
                    }
                )
    return rows


def source_label(path):
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf_csv", nargs="+", type=Path)
    parser.add_argument("--old", type=Path, default=OLD)
    parser.add_argument(
        "--out", type=Path, default=DERIVED / "winner_comparison.parquet"
    )
    args = parser.parse_args()
    old = old_winners(args.old)
    pdf = pdf_winners(args.pdf_csv)
    mappings, ambiguous = find_matches(pdf, old)
    rows = compare(pdf, old, mappings)
    write_rows(args.out, rows, COLUMNS)
    counts = Counter(row["comparison_status"] for row in rows)
    report = {
        "source_sha256": {
            str(path.resolve().relative_to(ROOT)): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in [args.old, *args.pdf_csv]
        },
        "pdf_bodies": len(pdf),
        "matched_bodies": len(mappings),
        "unmatched_bodies": [
            {"key": key, "top_scores": scores} for key, scores in ambiguous.items()
        ],
        "comparison_counts": dict(counts),
        "disagreements": [
            row
            for row in rows
            if row["comparison_status"] not in {"match", "body_unmatched"}
        ],
    }
    report["output_sha256"] = hashlib.sha256(args.out.read_bytes()).hexdigest()
    args.out.with_suffix(".audit.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                k: v
                for k, v in report.items()
                if k not in {"unmatched_bodies", "disagreements"}
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
