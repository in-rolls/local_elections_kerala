"""Extract candidates from the SEC's 2005 district reports and audit each ward."""

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path

import pdfplumber

from local_elections_kerala.paths import receipt_for
from local_elections_kerala.tables import write_rows

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "data/raw/sec/2005_reports"
DERIVED = ROOT / "data/build/sec/2005"
TIERS = {
    "Block Panchayat": "Block",
    "Grama Panchayat": "Grama Panchayat",
    "District Panchayat": "District Panchayat",
    "Muncipality": "Municipality",
    "Corporation": "Corporation",
}
FIELDS = [
    "document_id",
    "district",
    "lgi_type",
    "source_page",
    "body_no",
    "body_name",
    "ward_no",
    "ward_name",
    "candidate_no",
    "candidate_name",
    "party",
    "votes",
    "status",
    "votes_polled",
    "valid_votes",
    "invalid_votes",
    "ward_validated",
]


def column(words, left, right):
    return " ".join(w["text"] for w in words if left <= w["x0"] < right).strip()


def lines(page):
    words = [
        w
        for w in page.extract_words(x_tolerance=1.5, y_tolerance=2)
        if 105 < w["top"] < page.height - 30
    ]
    words.sort(key=lambda w: (w["top"], w["x0"]))
    groups = []
    for word in words:
        if not groups or word["top"] - groups[-1][0] > 3:
            groups.append([word["top"], [word]])
        else:
            groups[-1][1].append(word)
    return [sorted(group, key=lambda w: w["x0"]) for _, group in groups]


def extract(path, document_id, expected_district):
    candidates = []
    problems = []
    body_names = {}
    wards = {}
    current = None
    with pdfplumber.open(path) as pdf:
        page_texts = [page.extract_text() or "" for page in pdf.pages]
        headers = [text.splitlines()[:4] for text in page_texts]
        source_candidate_rows = sum(
            line.rstrip().endswith(("Elected", "Deposit Loss"))
            for text in page_texts
            for line in text.splitlines()
        )
        page_tiers = [
            next((TIERS[h.strip()] for h in header if h.strip() in TIERS), None)
            for header in headers
        ]
        for page_no, page in enumerate(pdf.pages, 1):
            header = headers[page_no - 1]
            tier = page_tiers[page_no - 1]
            if not tier:
                problems.append(
                    {"page": page_no, "problem": "unknown_tier", "header": header}
                )
                continue
            district_line = next(
                (h for h in header if h.startswith("District Name")), ""
            )
            district = district_line.partition(":")[2].strip()
            if district != expected_district:
                problems.append(
                    {"page": page_no, "problem": "district_mismatch", "found": district}
                )
                continue
            page_lines = lines(page)
            starts = [
                i for i, row in enumerate(page_lines) if column(row, 398, 435).isdigit()
            ]
            if not starts:
                problems.append({"page": page_no, "problem": "no_candidates"})
                continue
            next_tier = page_tiers[page_no] if page_no < len(page_tiers) else None
            transition = None
            if next_tier and next_tier != tier:
                transition = next(
                    (
                        i
                        for i in reversed(starts)
                        if column(page_lines[i], 0, 40).isdigit()
                        and column(page_lines[i], 95, 115) == "1"
                        and column(page_lines[i], 398, 435) == "1"
                    ),
                    None,
                )
                if transition is None:
                    problems.append(
                        {"page": page_no, "problem": "unlocated_tier_transition"}
                    )
            for j, start in enumerate(starts):
                row = page_lines[start]
                effective_tier = (
                    next_tier
                    if transition is not None and start >= transition
                    else tier
                )
                if current and current[0] != effective_tier:
                    current = None
                next_start = starts[j + 1] if j + 1 < len(starts) else len(page_lines)
                continuation = page_lines[start + 1 : next_start]
                body_no = column(row, 0, 40)
                ward_no = column(row, 95, 115)
                if body_no.isdigit():
                    current = (effective_tier, int(body_no), None)
                if ward_no.isdigit() and current:
                    current = (current[0], current[1], int(ward_no))
                if current is None or current[2] is None:
                    problems.append(
                        {
                            "page": page_no,
                            "problem": "missing_body_or_ward",
                            "candidate_no": column(row, 398, 435),
                        }
                    )
                    continue
                key = current
                body_english = [column(line, 41, 102) for line in continuation]
                body_english = [x for x in body_english if x]
                if body_no.isdigit() and body_english:
                    body_names[key[:2]] = " ".join(body_english)
                ward_english = [column(line, 115, 245) for line in continuation]
                ward_english = [x for x in ward_english if x]
                if ward_no.isdigit():
                    polled = column(row, 245, 300)
                    valid = column(row, 300, 338)
                    invalid = column(row, 338, 390)
                    if not valid.isdigit() or (invalid and not invalid.isdigit()):
                        problems.append(
                            {"page": page_no, "problem": "bad_ward_totals", "key": key}
                        )
                    wards[key] = {
                        "ward_name": " ".join(ward_english),
                        "votes_polled": polled,
                        "valid_votes": valid,
                        "invalid_votes": invalid,
                    }
                names = [column(line, 435, 620) for line in continuation]
                names = [name for name in names if name]
                candidate_name = " ".join(names)
                if not candidate_name:
                    problems.append(
                        {
                            "page": page_no,
                            "problem": "missing_candidate_name",
                            "key": key,
                        }
                    )
                status = column(row, 750, 841)
                votes = column(row, 705, 750)
                party = column(row, 620, 705)
                if (
                    status not in {"Elected", "Non Elected", "Deposit Loss"}
                    or not votes.isdigit()
                    or not party
                ):
                    problems.append(
                        {
                            "page": page_no,
                            "problem": "bad_candidate_fields",
                            "key": key,
                            "status": status,
                            "votes": votes,
                            "party": party,
                        }
                    )
                candidates.append(
                    {
                        "document_id": document_id,
                        "district": district,
                        "lgi_type": effective_tier,
                        "source_page": page_no,
                        "body_no": key[1],
                        "body_name": "",
                        "ward_no": key[2],
                        "ward_name": "",
                        "candidate_no": int(column(row, 398, 435)),
                        "candidate_name": candidate_name,
                        "party": party,
                        "votes": votes,
                        "status": status,
                        "votes_polled": "",
                        "valid_votes": "",
                        "invalid_votes": "",
                    }
                )
    if len(candidates) != source_candidate_rows:
        problems.append(
            {
                "problem": "candidate_row_count_mismatch",
                "source_rows": source_candidate_rows,
                "parsed_rows": len(candidates),
            }
        )
    for candidate in candidates:
        key = (candidate["lgi_type"], candidate["body_no"], candidate["ward_no"])
        candidate["body_name"] = body_names.get(key[:2], "")
        candidate.update(wards.get(key, {}))
    return candidates, problems


def audit(candidates, parse_problems):
    groups = defaultdict(list)
    for candidate in candidates:
        key = (candidate["lgi_type"], candidate["body_no"], candidate["ward_no"])
        groups[key].append(candidate)
    issues = list(parse_problems)
    warnings = []
    for key, rows in groups.items():
        elected = [row for row in rows if row["status"] == "Elected"]
        if len(elected) != 1:
            issues.append(
                {"problem": "elected_count", "key": key, "count": len(elected)}
            )
        ids = sorted(row["candidate_no"] for row in rows)
        if ids != list(range(1, len(ids) + 1)):
            issues.append({"problem": "candidate_sequence", "key": key, "ids": ids})
        first = rows[0]
        if not first["body_name"] or not first["ward_name"]:
            warnings.append({"problem": "missing_geography", "key": key})
        try:
            valid = int(first["valid_votes"])
            polled = (
                int(first["votes_polled"]) if first["votes_polled"].isdigit() else None
            )
            invalid = (
                int(first["invalid_votes"])
                if first["invalid_votes"].isdigit()
                else None
            )
            if polled is None:
                warnings.append(
                    {"problem": "source_polled_votes_unreadable", "key": key}
                )
            if invalid is None:
                warnings.append({"problem": "source_invalid_votes_blank", "key": key})
            if polled is not None and invalid is not None and polled != valid + invalid:
                issues.append(
                    {
                        "problem": "poll_total",
                        "key": key,
                        "polled": polled,
                        "valid": valid,
                        "invalid": invalid,
                    }
                )
            elif polled is not None and polled < valid:
                issues.append({"problem": "polled_below_valid", "key": key})
            cast = sum(int(row["votes"]) for row in rows)
            if cast != valid:
                issues.append(
                    {
                        "problem": "candidate_vote_sum",
                        "key": key,
                        "cast": cast,
                        "valid": valid,
                    }
                )
        except (ValueError, TypeError):
            issues.append({"problem": "invalid_vote_cell", "key": key})
    bad_keys = {tuple(issue["key"]) for issue in issues if "key" in issue}
    for row in candidates:
        key = (row["lgi_type"], row["body_no"], row["ward_no"])
        row["ward_validated"] = key not in bad_keys
    return groups, issues, warnings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("document_id")
    parser.add_argument("district")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    source = args.source or SOURCE / f"{args.document_id}.pdf"
    if args.source is None:
        receipt_path = receipt_for(source)
        if not receipt_path.exists():
            parser.error(f"missing acquisition receipt: {receipt_path}")
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if (
            receipt.get("status") != "ok"
            or receipt.get("sha256") != hashlib.sha256(source.read_bytes()).hexdigest()
        ):
            parser.error(
                f"source does not match successful acquisition receipt: {source}"
            )
    out = args.out or DERIVED / f"{args.document_id}_candidates.parquet"
    candidates, problems = extract(source, args.document_id, args.district)
    groups, issues, warnings = audit(candidates, problems)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    with pdfplumber.open(source) as pdf:
        source_pages = len(pdf.pages)
    report = {
        "document_id": args.document_id,
        "source_sha256": digest,
        "source_pages": source_pages,
        "candidate_rows": len(candidates),
        "wards": len(groups),
        "elected": sum(row["status"] == "Elected" for row in candidates),
        "validated_wards": len(groups)
        - len({tuple(i["key"]) for i in issues if "key" in i}),
        "issues_by_type": dict(Counter(issue["problem"] for issue in issues)),
        "warnings_by_type": dict(Counter(issue["problem"] for issue in warnings)),
        "issues": issues,
        "warnings": warnings,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    report_path = out.with_suffix(".audit.json")
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if any(
        i["problem"]
        in {
            "unknown_tier",
            "district_mismatch",
            "no_candidates",
            "missing_body_or_ward",
            "unlocated_tier_transition",
            "candidate_row_count_mismatch",
        }
        for i in issues
    ):
        print(
            json.dumps(
                {k: v for k, v in report.items() if k not in {"issues", "warnings"}},
                sort_keys=True,
            )
        )
        raise SystemExit(1)
    write_rows(out, candidates, FIELDS)
    with out.open("rb") as stream:
        report["output_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {k: v for k, v in report.items() if k not in {"issues", "warnings"}},
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
