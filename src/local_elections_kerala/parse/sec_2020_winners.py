"""Extract ward codes from retained 2020 SEC winner gazettes and audit coverage."""

import csv
import hashlib
import json
import re
import subprocess
from collections import Counter
from pathlib import Path

from local_elections_kerala.parse.sec_2020_compare import listing_groups
from local_elections_kerala.paths import RECORDS, receipt_for
from local_elections_kerala.tables import read_rows, write_rows

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "data/raw/sec/2020_winners"
MANIFEST = ROOT / "data/search_log/sec/sec_2020_winners.csv"
DERIVED = ROOT / "data/build/sec/2020_winners"
CANDIDATE_LEADERS = ROOT / "data/build/sec/modern/2020_vote_leaders.parquet"
LSG_COMPARISON = ROOT / "data/build/sec/modern/2020_vote_leader_comparison.parquet"
BODY_CROSSWALK = ROOT / "data/build/sec/modern/2020_body_crosswalk.parquet"
LSG_LISTING = ROOT / "data/raw/lsg_scrape/2020.csv"
RURAL_START = re.compile(r"^\s*(?:(\d+)\s+)?([DBG]\d{5})-")
RURAL_NUMBER = re.compile(r"\b(\d{3})-")
URBAN_ROW = re.compile(r"\b([MC]\d{5})-.*?\s+(\d{1,3})-")
CODE_FIELDS = ["source_pdf", "page", "page_line", "ward_code", "number_position"]
COMPARISON_FIELDS = [
    "ward_code",
    "source_report",
    "certified_code_present",
    "candidate_vote_present",
    "candidate_vote_leader_status",
    "lsg_listing_match_status",
    "lsg_listing_present",
    "lsg_listing_source_row",
    "code_comparison_status",
]


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def extract_text(path):
    result = subprocess.run(
        ["pdftotext", "-layout", "-enc", "UTF-8", str(path), "-"],
        capture_output=True,
        check=True,
    )
    return result.stdout.decode("utf-8")


def parse_rural_page(page, page_number, source_pdf):
    lines = page.splitlines()
    rows, failures = [], []
    for index, line in enumerate(lines):
        body = RURAL_START.match(line)
        if not body:
            continue
        number = RURAL_NUMBER.search(line[body.end() :])
        position = "inline"
        if not number and body.group(1):
            for distance in range(1, 4):
                previous = index - distance
                if previous < 0 or RURAL_START.match(lines[previous]):
                    break
                number = RURAL_NUMBER.search(lines[previous])
                if number:
                    position = f"previous_{distance}"
                    break
        if not number and not body.group(1):
            for distance in range(1, 4):
                following = index + distance
                if following >= len(lines) or RURAL_START.match(lines[following]):
                    break
                number = RURAL_NUMBER.search(lines[following])
                if number:
                    position = f"next_{distance}"
                    break
        if not number:
            failures.append(
                {"page": page_number, "line": index + 1, "body_code": body.group(2)}
            )
            continue
        rows.append(
            {
                "source_pdf": source_pdf,
                "page": page_number,
                "page_line": index + 1,
                "ward_code": body.group(2) + number.group(1),
                "number_position": position,
            }
        )
    return rows, failures


def parse_urban_page(page, page_number, source_pdf):
    rows = []
    for index, line in enumerate(page.splitlines(), 1):
        match = URBAN_ROW.search(line)
        if match:
            rows.append(
                {
                    "source_pdf": source_pdf,
                    "page": page_number,
                    "page_line": index,
                    "ward_code": match.group(1) + f"{int(match.group(2)):03d}",
                    "number_position": "inline",
                }
            )
    return rows


def parse_pdf(path, is_urban):
    text = extract_text(path)
    rows, failures = [], []
    source_pdf = str(path.relative_to(ROOT))
    pages = text.split("\f")
    for page_number, page in enumerate(pages, 1):
        if not page.strip():
            continue
        if is_urban:
            rows.extend(parse_urban_page(page, page_number, source_pdf))
        else:
            found, errors = parse_rural_page(page, page_number, source_pdf)
            rows.extend(found)
            failures.extend(errors)
    page_count = text.count("\f")
    if failures:
        raise ValueError(f"{path.name}: {len(failures)} unparsed rows: {failures[:3]}")
    if len({r["ward_code"] for r in rows}) != len(rows):
        raise ValueError(f"{path.name}: duplicate ward codes")
    if not rows:
        raise ValueError(f"{path.name}: no ward codes")
    receipt_path = receipt_for(path)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if receipt.get("sha256") != sha256(path) or receipt.get("page_count") != page_count:
        raise ValueError(f"{path.name}: PDF and acquisition receipt differ")
    return rows, page_count


def compare_codes(code_rows, leader_rows, lsg_rows, crosswalk_rows, listing_rows):
    certified = {row["ward_code"]: row for row in code_rows}
    candidates = {row["ward_code"]: row for row in leader_rows}
    lsg = {row["ward_code"]: row for row in lsg_rows}
    body_map = {row["lb_code"]: row["listing_group_id"] for row in crosswalk_rows}
    groups = listing_groups(listing_rows)
    reports_by_district = {}
    urban_report = None
    for row in code_rows:
        code = row["ward_code"]
        if code[0] in {"M", "C"}:
            urban_report = row["source_pdf"]
        else:
            district = code[1:3]
            old = reports_by_district.setdefault(district, row["source_pdf"])
            if old != row["source_pdf"]:
                raise ValueError(f"District {district} appears in two gazettes")
    covered_candidate_codes = {
        code
        for code in candidates
        if (code[0] in {"M", "C"} and urban_report)
        or (code[0] in {"D", "B", "G"} and code[1:3] in reports_by_district)
    }
    comparison = []
    for code in sorted(set(certified) | covered_candidate_codes):
        cert = certified.get(code)
        candidate = candidates.get(code)
        source_report = (
            cert["source_pdf"]
            if cert
            else urban_report
            if code[0] in {"M", "C"}
            else reports_by_district[code[1:3]]
        )
        group_id = body_map.get(code[:6])
        listing = groups[group_id]["wards"].get(int(code[-3:])) if group_id else None
        lsg_status = (
            lsg[code]["listing_match_status"]
            if code in lsg
            else "joined_from_body_crosswalk"
            if listing
            else "unresolved_body"
            if not group_id
            else "missing_listing_ward"
        )
        comparison.append(
            {
                "ward_code": code,
                "source_report": source_report,
                "certified_code_present": bool(cert),
                "candidate_vote_present": bool(candidate),
                "candidate_vote_leader_status": candidate["winner_status"]
                if candidate
                else None,
                "lsg_listing_match_status": lsg_status,
                "lsg_listing_present": bool(listing),
                "lsg_listing_source_row": listing["source_row"] if listing else None,
                "code_comparison_status": (
                    "both"
                    if cert and candidate
                    else "certified_only"
                    if cert
                    else "candidate_only"
                ),
            }
        )
    return comparison


def main():
    manifest = read_csv(MANIFEST)
    source_coverage = RECORDS / SOURCE.name / "coverage.json"
    coverage = json.loads(source_coverage.read_text(encoding="utf-8"))
    if len(manifest) != len(coverage["documents"]):
        raise ValueError("Winner manifest and acquisition coverage differ")
    code_rows, source_audits = [], []
    for source in manifest:
        path = SOURCE / f"{source['document_id']}.pdf"
        receipt_path = receipt_for(path)
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt["status"] != "ok":
            source_audits.append(
                {
                    "document_id": source["document_id"],
                    "status": receipt["status"],
                    "rows": None,
                }
            )
            continue
        rows, pages = parse_pdf(path, source["district"] == "")
        code_rows.extend(rows)
        source_audits.append(
            {
                "document_id": source["document_id"],
                "status": "ok",
                "source_sha256": sha256(path),
                "pages": pages,
                "rows": len(rows),
                "number_position": dict(Counter(r["number_position"] for r in rows)),
            }
        )
    if len({r["ward_code"] for r in code_rows}) != len(code_rows):
        raise ValueError("Ward code repeated across winner gazettes")
    comparison = compare_codes(
        code_rows,
        read_rows(CANDIDATE_LEADERS),
        read_rows(LSG_COMPARISON),
        read_rows(BODY_CROSSWALK),
        read_csv(LSG_LISTING),
    )
    DERIVED.mkdir(parents=True, exist_ok=True)
    code_path = DERIVED / "2020_certified_ward_codes.parquet"
    comparison_path = DERIVED / "2020_candidate_certified_comparison.parquet"
    write_rows(code_path, code_rows, CODE_FIELDS)
    write_rows(comparison_path, comparison, COMPARISON_FIELDS)
    audit = {
        "manifest_sha256": sha256(MANIFEST),
        "acquisition_coverage_sha256": sha256(source_coverage),
        "candidate_leaders_sha256": sha256(CANDIDATE_LEADERS),
        "lsg_comparison_sha256": sha256(LSG_COMPARISON),
        "body_crosswalk_sha256": sha256(BODY_CROSSWALK),
        "lsg_listing_sha256": sha256(LSG_LISTING),
        "certified_codes_sha256": sha256(code_path),
        "comparison_sha256": sha256(comparison_path),
        "source_status": dict(Counter(x["status"] for x in source_audits)),
        "sources": source_audits,
        "certified_ward_codes": len(code_rows),
        "certified_only_lsg_present": sum(
            row["code_comparison_status"] == "certified_only"
            and row["lsg_listing_present"]
            for row in comparison
        ),
        "comparison_status": dict(
            Counter(x["code_comparison_status"] for x in comparison)
        ),
    }
    (DERIVED / "2020_certified_coverage.audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps({k: v for k, v in audit.items() if k != "sources"}, sort_keys=True)
    )


if __name__ == "__main__":
    main()
