"""Extract candidate votes from the retained 2020 SEC PDF using Poppler text layout."""

import argparse
import hashlib
import json
import re
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

from local_elections_kerala.tables import write_rows

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "data/raw/sec/modern_candidates/2020_candidates.pdf"
OUT = ROOT / "data/interim/sec/modern"
CANDIDATE_START = re.compile(r"^\s*([A-Z]+)\s+([A-Z]\d{5})\s+")
WARD_CODE = re.compile(r"\b([A-Z]\d{8})\b")
TRAILING_NUMBER = re.compile(r"\s{2,}(\d{1,7})\s*$")
CANDIDATE_FIELDS = [
    "page",
    "page_line",
    "district_raw",
    "lb_code",
    "lb_name_raw",
    "ward_code",
    "ward_name_raw",
    "party_raw",
    "candidate_name_raw",
    "gender_raw",
    "age_raw",
    "votes",
    "field_parse_status",
]
WINNER_FIELDS = [
    "ward_code",
    "candidate_rows",
    "top_votes",
    "top_candidate_page",
    "top_candidate_line",
    "winner_name_raw",
    "winner_party_raw",
    "winner_status",
]


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def extract_text(path):
    result = subprocess.run(
        ["pdftotext", "-layout", "-enc", "UTF-8", str(path), "-"],
        capture_output=True,
        check=True,
    )
    return result.stdout.decode("utf-8")


def parse_text(text):
    records = []
    problems = []
    for page_number, page in enumerate(text.split("\f"), 1):
        lines = page.splitlines()
        starts = [i for i, line in enumerate(lines) if CANDIDATE_START.match(line)]
        if not starts:
            continue
        positions = [
            match.start(1)
            for line in lines
            if (match := TRAILING_NUMBER.search(line)) and len(match.group(1)) <= 5
        ]
        if not positions:
            problems.append({"page": page_number, "error": "no vote column"})
            continue
        vote_column = sorted(positions)[int(0.9 * (len(positions) - 1))]
        for index, first_line in enumerate(starts):
            line = lines[first_line]
            ward_match = WARD_CODE.search(line)
            if not ward_match:
                problems.append(
                    {
                        "page": page_number,
                        "line": first_line + 1,
                        "error": "missing ward code",
                    }
                )
                continue
            stop = starts[index + 1] if index + 1 < len(starts) else len(lines)
            votes = [
                int(match.group(1))
                for candidate_line in lines[first_line:stop]
                if (match := TRAILING_NUMBER.search(candidate_line))
                and abs(match.start(1) - vote_column) <= 9
            ]
            if len(votes) != 1:
                problems.append(
                    {
                        "page": page_number,
                        "line": first_line + 1,
                        "ward_code": ward_match.group(1),
                        "error": "expected one vote",
                        "found": votes,
                    }
                )
                continue
            prefix = re.split(r"\s{2,}", line[: ward_match.start()].strip())
            suffix = re.split(r"\s{2,}", line[ward_match.end() :].strip())
            if len(prefix) < 2 or not suffix:
                problems.append(
                    {
                        "page": page_number,
                        "line": first_line + 1,
                        "error": "missing district/body or ward text",
                    }
                )
                continue
            name = gender = age = party = None
            if len(suffix) >= 4 and suffix[3] in {"M", "F"}:
                party, name, gender = suffix[1:4]
                age_match = re.match(r"\d{2}\b", suffix[4]) if len(suffix) > 4 else None
                age = age_match.group() if age_match else None
            status = "complete" if all((party, name, gender, age)) else "partial"
            records.append(
                {
                    "page": page_number,
                    "page_line": first_line + 1,
                    "district_raw": prefix[0],
                    "lb_code": prefix[1],
                    "lb_name_raw": prefix[2] if len(prefix) > 2 else None,
                    "ward_code": ward_match.group(1),
                    "ward_name_raw": suffix[0],
                    "party_raw": party,
                    "candidate_name_raw": name,
                    "gender_raw": gender,
                    "age_raw": age,
                    "votes": votes[0],
                    "field_parse_status": status,
                }
            )
    return records, problems


def winners(records):
    by_ward = defaultdict(list)
    for row in records:
        by_ward[row["ward_code"]].append(row)
    result = []
    for ward_code, rows in sorted(by_ward.items()):
        highest = max(row["votes"] for row in rows)
        leaders = [row for row in rows if row["votes"] == highest]
        leader = leaders[0] if len(leaders) == 1 else None
        result.append(
            {
                "ward_code": ward_code,
                "candidate_rows": len(rows),
                "top_votes": highest,
                "top_candidate_page": leader["page"] if leader else None,
                "top_candidate_line": leader["page_line"] if leader else None,
                "winner_name_raw": leader["candidate_name_raw"] if leader else None,
                "winner_party_raw": leader["party_raw"] if leader else None,
                "winner_status": "unique_max" if leader else "tied_max",
            }
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()
    if not args.source.exists():
        parser.error(f"missing source PDF: {args.source}")
    extracted_text = extract_text(args.source)
    records, problems = parse_text(extracted_text)
    pdf_pages = extracted_text.count("\f")
    pages_with_candidates = len({row["page"] for row in records})
    if pdf_pages != pages_with_candidates:
        raise ValueError(
            f"Candidate rows found on {pages_with_candidates} of {pdf_pages} PDF pages"
        )
    receipt_path = args.source.with_suffix(".receipt.json")
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("sha256") != sha256(args.source) or (
            receipt.get("page_count") != pdf_pages
        ):
            raise ValueError("PDF does not match its acquisition receipt")
    if problems:
        raise ValueError(
            f"{len(problems)} PDF rows could not be parsed: {problems[:3]}"
        )
    if not records:
        raise ValueError("No candidate rows found")
    winner_rows = winners(records)
    args.out.mkdir(parents=True, exist_ok=True)
    candidates_file = args.out / "2020_candidates.parquet"
    winners_file = args.out / "2020_vote_leaders.parquet"
    write_rows(candidates_file, records, CANDIDATE_FIELDS)
    write_rows(winners_file, winner_rows, WINNER_FIELDS)
    audit = {
        "source_file": str(args.source.resolve().relative_to(ROOT)),
        "source_sha256": sha256(args.source),
        "pdf_pages": pdf_pages,
        "pages_with_candidates": pages_with_candidates,
        "candidate_rows": len(records),
        "ward_rows": len(winner_rows),
        "candidate_field_status": dict(
            Counter(row["field_parse_status"] for row in records)
        ),
        "winner_status": dict(Counter(row["winner_status"] for row in winner_rows)),
        "zero_top_votes": sum(row["top_votes"] == 0 for row in winner_rows),
        "candidates_sha256": sha256(candidates_file),
        "vote_leaders_sha256": sha256(winners_file),
        "parser": "pdftotext -layout -enc UTF-8",
    }
    (args.out / "2020_parse.audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(audit, sort_keys=True))


if __name__ == "__main__":
    main()
