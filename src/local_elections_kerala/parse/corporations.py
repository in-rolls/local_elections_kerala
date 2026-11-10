"""Build an offline, source-separated corporation handoff; never infer sex."""

import argparse
import csv
import hashlib
import json
import unicodedata
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BODIES = {
    "C01001": ("Thiruvananthapuram", "Thiruvananthapuram", 100, 101),
    "C02002": ("Kollam", "Kollam", 55, 56),
    "C07003": ("Kochi", "Ernakulam", 74, 76),
    "C08004": ("Thrissur", "Thrissur", 55, 56),
    "C11005": ("Kozhikode", "Kozhikode", 75, 76),
    "C13006": ("Kannur", "Kannur", 55, 56),
}
INTS = {
    "year",
    "ward_number",
    "profile_source_row",
    "sec_votes",
    "secondary_votes",
    "provisional_winner_votes",
}
BOOLS = {
    "profile_present",
    "sec_present",
    "secondary_present",
    "profile_name_equal",
    "profile_party_equal",
    "profile_identity_link",
    "profile_sex_reservation_conflict",
    "sec_secondary_vote_conflict",
    "provisional_winner_woman",
    "provisional_winner_certified",
    "analysis_eligible",
}
COLUMNS = [
    "year",
    "body_code",
    "city",
    "ward_number",
    "ward_code",
    "profile_present",
    "profile_ward_name_raw",
    "profile_member_raw",
    "profile_party_raw",
    "profile_sex_raw",
    "profile_reservation_raw",
    "profile_source_file",
    "profile_source_row",
    "profile_sex_reservation_conflict",
    "sec_present",
    "sec_winner_status",
    "sec_candidate_raw",
    "sec_party_raw",
    "sec_gender_raw",
    "sec_votes",
    "sec_source_file",
    "secondary_present",
    "secondary_candidate_raw",
    "secondary_party_raw",
    "secondary_ward_name_raw",
    "secondary_votes",
    "secondary_source_file",
    "sec_secondary_vote_conflict",
    "provisional_winner_name",
    "provisional_winner_party",
    "provisional_winner_votes",
    "provisional_winner_source",
    "provisional_winner_status",
    "provisional_winner_certified",
    "profile_name_equal",
    "profile_party_equal",
    "profile_identity_link",
    "provisional_winner_woman",
    "provisional_sex_source",
    "analysis_eligible",
]
SCHEMA = pa.schema(
    [
        (c, pa.int64() if c in INTS else pa.bool_() if c in BOOLS else pa.string())
        for c in COLUMNS
    ]
)


def normalized(value):
    """Keep Unicode letters; empty strings never identify people."""
    return "".join(
        c for c in unicodedata.normalize("NFKC", value or "").casefold() if c.isalnum()
    )


def equal(a, b):
    left, right = normalized(a), normalized(b)
    return bool(left and right and left == right)


def unique_index(rows, key):
    result = {}
    for row in rows:
        value = key(row)
        if value in result:
            raise ValueError(f"Duplicate key: {value}")
        result[value] = row
    return result


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(root, output):
    manifest_path = root / "data/raw/secondary/farzzn/manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for source in manifest:
        if sha256(root / source["path"]) != source["sha256"]:
            raise ValueError(f"Source hash mismatch: {source['path']}")
    profile_path = "data/release/elected_members_2010_2020.parquet"
    sec_path = "data/build/sec/modern/2020_vote_leader_comparison.parquet"
    city_codes = {v[1]: k for k, v in BODIES.items()}
    profiles = unique_index(
        (
            r
            for r in pq.read_table(root / profile_path).to_pylist()
            if r["lgi_type"] == "Corporation"
        ),
        lambda r: (r["year"], city_codes[r["corporation_raw"]], int(r["ward_no_raw"])),
    )
    sec = unique_index(
        (
            r
            for r in pq.read_table(root / sec_path).to_pylist()
            if r["ward_code"].startswith("C")
        ),
        lambda r: r["ward_code"],
    )
    secondary = {}
    for year in [2015, 2020, 2025]:
        path = (
            f"data/raw/secondary/farzzn/Kerala_LocalBodies_Election_{year}_MASTER.csv"
        )
        with (root / path).open(encoding="utf-8-sig", newline="") as handle:
            rows = [
                dict(r, source_file=path)
                for r in csv.DictReader(handle)
                if r["Local Body ID"].startswith("C")
            ]
        secondary.update(
            unique_index(
                rows,
                lambda r: (int(r["Year"]), r["Local Body ID"], int(r["Ward Number"])),
            )
        )
    rows = []
    for year in [2010, 2015, 2020, 2025]:
        for code, (city, _, old_count, new_count) in BODIES.items():
            if year == 2010 and code == "C13006":
                continue
            for ward in range(1, (new_count if year == 2025 else old_count) + 1):
                key = year, code, ward
                ward_code = f"{code}{ward:03}"
                p, s = profiles.get(key, {}), secondary.get(key, {})
                q = sec.get(ward_code, {}) if year == 2020 else {}
                conflict = bool(
                    p
                    and "woman" in (p["reserved_raw"] or "").casefold()
                    and p["winner_woman_raw"] == "Male"
                )
                r = dict.fromkeys(COLUMNS)
                r.update(
                    year=year,
                    body_code=code,
                    city=city,
                    ward_number=ward,
                    ward_code=ward_code,
                    profile_present=bool(p),
                    profile_ward_name_raw=p.get("ward_name_raw"),
                    profile_member_raw=p.get("elected_member_raw"),
                    profile_party_raw=p.get("party_raw"),
                    profile_sex_raw=p.get("winner_woman_raw"),
                    profile_reservation_raw=p.get("reserved_raw"),
                    profile_source_file=p.get("source_file"),
                    profile_source_row=p.get("source_row"),
                    profile_sex_reservation_conflict=conflict,
                    sec_present=bool(q),
                    sec_winner_status=q.get("pdf_winner_status"),
                    sec_candidate_raw=q.get("pdf_winner_name_raw"),
                    sec_party_raw=q.get("pdf_winner_party_raw"),
                    sec_gender_raw=q.get("pdf_gender_raw"),
                    sec_votes=q.get("pdf_top_votes"),
                    sec_source_file=sec_path if q else None,
                    secondary_present=bool(s),
                    secondary_candidate_raw=s.get("Candidate"),
                    secondary_party_raw=s.get("Party"),
                    secondary_ward_name_raw=s.get("Ward Name"),
                    secondary_votes=int(s["Votes"]) if s else None,
                    secondary_source_file=s.get("source_file"),
                    provisional_winner_certified=False,
                    analysis_eligible=False,
                )
                r["sec_secondary_vote_conflict"] = bool(
                    q and s and r["sec_votes"] != r["secondary_votes"]
                )
                if q and q["pdf_winner_status"] == "tied_max":
                    r["provisional_winner_status"] = "unresolved_vote_tie"
                elif r["sec_secondary_vote_conflict"]:
                    r["provisional_winner_status"] = "conflicting_vote_totals"
                elif q and q["pdf_winner_status"] == "unique_max":
                    r.update(
                        provisional_winner_name=r["sec_candidate_raw"],
                        provisional_winner_party=r["sec_party_raw"],
                        provisional_winner_votes=r["sec_votes"],
                        provisional_winner_source=sec_path,
                        provisional_winner_status="official_unique_vote_leader",
                    )
                elif s:
                    r.update(
                        provisional_winner_name=r["secondary_candidate_raw"],
                        provisional_winner_party=r["secondary_party_raw"],
                        provisional_winner_votes=r["secondary_votes"],
                        provisional_winner_source=s["source_file"],
                        provisional_winner_status="secondary_reported_vote_leader",
                    )
                else:
                    r["provisional_winner_status"] = (
                        "profile_only_not_election_verified" if p else "missing"
                    )
                if (
                    q
                    and q["pdf_winner_status"] == "unique_max"
                    and not r["provisional_winner_name"]
                    and s
                    and not r["sec_secondary_vote_conflict"]
                ):
                    r.update(
                        provisional_winner_name=r["secondary_candidate_raw"],
                        provisional_winner_party=r["secondary_party_raw"],
                        provisional_winner_source=s["source_file"],
                        provisional_winner_status="secondary_name_official_unique_votes",
                    )
                r["profile_name_equal"] = equal(
                    r["provisional_winner_name"], r["profile_member_raw"]
                )
                r["profile_party_equal"] = equal(
                    r["provisional_winner_party"], r["profile_party_raw"]
                )
                r["profile_identity_link"] = (
                    r["profile_name_equal"] and r["profile_party_equal"]
                )
                if r["profile_identity_link"] and not conflict:
                    sex = r["profile_sex_raw"]
                    if sex in {"Female", "Male"}:
                        r["provisional_winner_woman"] = sex == "Female"
                        r["provisional_sex_source"] = (
                            "LSG recorded sex; same ward, exact name and party"
                        )
                rows.append(r)
    frame_keys = {(r["year"], r["body_code"], r["ward_number"]) for r in rows}
    if set(profiles) - frame_keys or set(secondary) - frame_keys:
        raise ValueError("Source records outside declared corporation ward frame")
    if set(sec) - {r["ward_code"] for r in rows if r["year"] == 2020}:
        raise ValueError("SEC records outside 2020 frame")
    output.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=SCHEMA), output / "source_comparison.parquet"
    )
    coverage = []
    for year in [2010, 2015, 2020, 2025]:
        for code, (city, *_rest) in BODIES.items():
            group = [r for r in rows if r["year"] == year and r["body_code"] == code]
            if group:
                coverage.append(
                    {
                        "year": year,
                        "body_code": code,
                        "city": city,
                        "frame": len(group),
                        "profiles": sum(r["profile_present"] for r in group),
                        "sec": sum(r["sec_present"] for r in group),
                        "secondary": sum(r["secondary_present"] for r in group),
                        "provisional_winners": sum(
                            r["provisional_winner_name"] is not None for r in group
                        ),
                        "linked_recorded_sex": sum(
                            r["provisional_winner_woman"] is not None for r in group
                        ),
                    }
                )
    with (output / "coverage.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(coverage[0]))
        writer.writeheader()
        writer.writerows(coverage)
    indexed = {(r["year"], r["ward_code"]): r for r in rows}
    transitions = []
    for end in rows:
        year, code = end["year"], end["body_code"]
        if year not in {2015, 2020}:
            continue
        if year == 2015 and code in {"C02002", "C13006"}:
            continue
        start = indexed[year - 5, end["ward_code"]]
        transitions.append(
            {
                "start_year": year - 5,
                "end_year": year,
                "body_code": code,
                "city": end["city"],
                "ward_number": end["ward_number"],
                "prior_reservation_raw": start["profile_reservation_raw"],
                "outcome_reservation_raw": end["profile_reservation_raw"],
                "prior_ward_name_raw": start["profile_ward_name_raw"],
                "outcome_ward_name_raw": end["profile_ward_name_raw"],
                "ward_name_equal": equal(
                    start["profile_ward_name_raw"], end["profile_ward_name_raw"]
                ),
                "provisional_winner_woman": end["provisional_winner_woman"],
                "boundary_source": "SEC 552/2015 para 16"
                if year == 2015
                else "CAG 2022 paragraph 2.5.1",
                "link_status": "unchanged_body_boundaries; ward_number_link",
                "analysis_eligible": False,
            }
        )
    transition_schema = pa.schema(
        [
            (
                c,
                pa.int64()
                if c in {"start_year", "end_year", "ward_number"}
                else pa.bool_()
                if c
                in {"ward_name_equal", "provisional_winner_woman", "analysis_eligible"}
                else pa.string(),
            )
            for c in transitions[0]
        ]
    )
    pq.write_table(
        pa.Table.from_pylist(transitions, schema=transition_schema),
        output / "boundary_supported_links.parquet",
    )
    audit = {
        "rows": len(rows),
        "boundary_supported_links": len(transitions),
        "profile_rows": len(profiles),
        "sec_rows": len(sec),
        "secondary_rows": len(secondary),
        "provisional_status_counts": dict(
            Counter(r["provisional_winner_status"] for r in rows)
        ),
        "missing_profiles_2015": [
            r["ward_code"]
            for r in rows
            if r["year"] == 2015 and not r["profile_present"]
        ],
        "missing_sec_2020": [
            r["ward_code"] for r in rows if r["year"] == 2020 and not r["sec_present"]
        ],
        "missing_secondary_2025": [
            r["ward_code"]
            for r in rows
            if r["year"] == 2025 and not r["secondary_present"]
        ],
        "gender_pairs_on_exact_name_match_2020": dict(
            Counter(
                f"{r['sec_gender_raw']} / {r['profile_sex_raw']}"
                for r in rows
                if r["year"] == 2020 and r["profile_name_equal"]
            )
        ),
        "input_sha256": {
            p: sha256(root / p)
            for p in [profile_path, sec_path, str(manifest_path.relative_to(root))]
        },
        "output_sha256": {p.name: sha256(p) for p in sorted(output.glob("*.parquet"))},
    }
    (output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n")
    return rows, audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    build(args.root, args.output or args.root / "data/build/corporations")


if __name__ == "__main__":
    main()
