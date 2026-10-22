"""Reconcile 2020 SEC vote leaders with saved LSG listings using ward evidence."""

import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path

from local_elections_kerala.tables import read_rows, write_rows

ROOT = Path(__file__).resolve().parents[3]
DERIVED = ROOT / "data/interim/sec/modern"
LISTING = ROOT / "data/raw/lsg_scrape/2020.csv"
TIERS = {
    "B": "Block",
    "C": "Corporation",
    "D": "District",
    "G": "Grama Panchayat",
    "M": "Municipality",
}
CROSSWALK_FIELDS = [
    "lb_code",
    "pdf_district_raw",
    "tier",
    "pdf_body_raw",
    "pdf_wards",
    "listing_group_id",
    "listing_body_raw",
    "proposed_listing_group_id",
    "proposed_listing_body_raw",
    "listing_wards",
    "mapping_status",
    "ward_name_matches",
    "ward_name_fraction",
    "body_name_similarity",
    "best_score",
    "runner_up_score",
]
COMPARISON_FIELDS = [
    "ward_code",
    "district_pdf_raw",
    "tier",
    "body_pdf_raw",
    "ward_number",
    "body_mapping_status",
    "pdf_winner_status",
    "pdf_top_votes",
    "pdf_winner_name_raw",
    "pdf_winner_party_raw",
    "pdf_gender_raw",
    "listing_match_status",
    "listing_source_row",
    "listing_member_raw",
    "listing_party_raw",
    "listing_sex_raw",
    "name_normalized_equal",
    "party_normalized_equal",
    "gender_relation_on_name_match",
]


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def normalize(value):
    return re.sub(r"[^A-Z0-9]", "", (value or "").upper())


def district_key(value):
    return normalize(value).replace("KASARGODE", "KASARGOD")


def pdf_bodies(rows):
    bodies = {}
    for row in rows:
        code = row["lb_code"]
        body = bodies.setdefault(
            code,
            {
                "district": row["district_raw"],
                "tier": TIERS[code[0]],
                "labels": Counter(),
                "wards": defaultdict(Counter),
            },
        )
        if body["district"] != row["district_raw"]:
            raise ValueError(f"PDF body {code} changes district")
        if row["lb_name_raw"]:
            body["labels"][row["lb_name_raw"]] += 1
        body["wards"][int(row["ward_code"][-3:])][normalize(row["ward_name_raw"])] += 1
    for body in bodies.values():
        body["name"] = body["labels"].most_common(1)[0][0] if body["labels"] else ""
        body["wards"] = {
            number: labels.most_common(1)[0][0]
            for number, labels in body["wards"].items()
        }
    return bodies


def listing_groups(rows):
    groups = {}
    district_segment = 0
    previous_district_ward = 0
    for index, row in enumerate(rows, 1):
        tier = row["LGI Type"]
        number = int(row["Ward No."])
        if tier == "District":
            if number <= previous_district_ward:
                district_segment += 1
            previous_district_ward = number
            group_id = f"DistrictSegment:{district_segment:02d}"
            body_name = ""
        elif tier == "Corporation":
            body_name = row["Corporation"]
            group_id = f"Corporation:{normalize(body_name)}"
        else:
            body_name = row[tier]
            group_id = f"{tier}:{district_key(row['District'])}:{normalize(body_name)}"
        group = groups.setdefault(
            group_id,
            {
                "tier": tier,
                "district": district_key(row["District"]),
                "body": body_name,
                "wards": {},
            },
        )
        if number in group["wards"]:
            raise ValueError(f"Repeated ward number in listing group {group_id}")
        group["wards"][number] = {**row, "source_row": index}
    return groups


def match_evidence(pdf_body, listing_group):
    pdf_wards = pdf_body["wards"]
    listing_wards = listing_group["wards"]
    matches = sum(
        pdf_wards.get(number) == normalize(row["Ward Name"])
        for number, row in listing_wards.items()
        if number in pdf_wards
    )
    fraction = matches / max(len(pdf_wards), len(listing_wards))
    similarity = (
        SequenceMatcher(
            None, normalize(pdf_body["name"]), normalize(listing_group["body"])
        ).ratio()
        if pdf_body["name"] and listing_group["body"]
        else 0.0
    )
    return matches, fraction, similarity, fraction + 0.25 * similarity


def choose_mapping(pdf_body, groups):
    candidates = []
    for group_id, group in groups.items():
        if group["tier"] != pdf_body["tier"]:
            continue
        if group["tier"] not in {"District", "Corporation"} and (
            group["district"] != district_key(pdf_body["district"])
        ):
            continue
        matches, fraction, similarity, score = match_evidence(pdf_body, group)
        candidates.append((score, matches, fraction, similarity, group_id))
    if not candidates:
        return None, "no_listing_group", 0, 0.0, 0.0, 0.0, 0.0, None
    candidates.sort(reverse=True)
    best = candidates[0]
    runner_up = candidates[1][0] if len(candidates) > 1 else 0.0
    score, matches, fraction, similarity, group_id = best
    group = groups[group_id]
    proposed_group_id = group_id
    exact_name = bool(pdf_body["name"]) and normalize(pdf_body["name"]) == normalize(
        group["body"]
    )
    same_count = len(pdf_body["wards"]) == len(group["wards"])
    if exact_name and score - runner_up >= 0.2:
        status = "exact_body_name"
    elif matches >= 5 and fraction >= 0.6 and score - runner_up >= 0.2:
        status = "ward_name_mapping"
    elif (
        same_count
        and matches >= 5
        and fraction >= 0.25
        and similarity >= 0.8
        and score - runner_up >= 0.2
    ):
        status = "body_and_ward_mapping"
    else:
        status = "ambiguous_or_weak_evidence"
        group_id = None
    return (
        group_id,
        status,
        matches,
        fraction,
        similarity,
        score,
        runner_up,
        proposed_group_id,
    )


def make_crosswalk(pdf_groups, listing_groups_by_id):
    rows = []
    for code, body in sorted(pdf_groups.items()):
        (
            group_id,
            status,
            matches,
            fraction,
            similarity,
            score,
            runner_up,
            proposed_id,
        ) = choose_mapping(body, listing_groups_by_id)
        group = listing_groups_by_id[group_id] if group_id else None
        proposed = listing_groups_by_id[proposed_id] if proposed_id else None
        rows.append(
            {
                "lb_code": code,
                "pdf_district_raw": body["district"],
                "tier": body["tier"],
                "pdf_body_raw": body["name"],
                "pdf_wards": len(body["wards"]),
                "listing_group_id": group_id,
                "listing_body_raw": group["body"] if group else None,
                "proposed_listing_group_id": proposed_id,
                "proposed_listing_body_raw": proposed["body"] if proposed else None,
                "listing_wards": len(group["wards"]) if group else None,
                "mapping_status": status,
                "ward_name_matches": matches,
                "ward_name_fraction": round(fraction, 4),
                "body_name_similarity": round(similarity, 4),
                "best_score": round(score, 4),
                "runner_up_score": round(runner_up, 4),
            }
        )
    assigned = Counter(
        row["listing_group_id"] for row in rows if row["listing_group_id"]
    )
    for row in rows:
        if row["listing_group_id"] and assigned[row["listing_group_id"]] > 1:
            row["mapping_status"] = "listing_group_collision"
            row["listing_group_id"] = None
            row["listing_body_raw"] = None
            row["listing_wards"] = None
    return rows


def compare_winners(winners, candidates, crosswalk, groups):
    candidates_by_line = {(r["page"], r["page_line"]): r for r in candidates}
    body_map = {row["lb_code"]: row for row in crosswalk}
    comparison = []
    for leader in winners:
        code = leader["ward_code"]
        body = body_map[code[:6]]
        number = int(code[-3:])
        candidate = candidates_by_line.get(
            (leader["top_candidate_page"], leader["top_candidate_line"])
        )
        result = {
            "ward_code": code,
            "district_pdf_raw": body["pdf_district_raw"],
            "tier": body["tier"],
            "body_pdf_raw": body["pdf_body_raw"],
            "ward_number": number,
            "body_mapping_status": body["mapping_status"],
            "pdf_winner_status": leader["winner_status"],
            "pdf_top_votes": leader["top_votes"],
            "pdf_winner_name_raw": leader["winner_name_raw"],
            "pdf_winner_party_raw": leader["winner_party_raw"],
            "pdf_gender_raw": candidate["gender_raw"] if candidate else None,
            "listing_match_status": None,
            "listing_source_row": None,
            "listing_member_raw": None,
            "listing_party_raw": None,
            "listing_sex_raw": None,
            "name_normalized_equal": None,
            "party_normalized_equal": None,
            "gender_relation_on_name_match": None,
        }
        if not body["listing_group_id"]:
            result["listing_match_status"] = "unresolved_body"
        else:
            listing = groups[body["listing_group_id"]]["wards"].get(number)
            if not listing:
                result["listing_match_status"] = "missing_listing_ward"
            else:
                result["listing_match_status"] = "joined"
                result["listing_source_row"] = listing["source_row"]
                result["listing_member_raw"] = listing["Elected Members"]
                result["listing_party_raw"] = listing["Party"]
                result["listing_sex_raw"] = listing["Female/Male"]
                if leader["winner_status"] == "unique_max":
                    if leader["winner_name_raw"]:
                        result["name_normalized_equal"] = normalize(
                            leader["winner_name_raw"]
                        ) == normalize(listing["Elected Members"])
                    if leader["winner_party_raw"]:
                        result["party_normalized_equal"] = normalize(
                            leader["winner_party_raw"]
                        ) == normalize(listing["Party"])
                    if result["name_normalized_equal"] is True and candidate:
                        expected = {"Female": "F", "Male": "M"}.get(
                            listing["Female/Male"]
                        )
                        if expected and candidate["gender_raw"] in {"F", "M"}:
                            result["gender_relation_on_name_match"] = (
                                "same"
                                if candidate["gender_raw"] == expected
                                else "opposite"
                            )
        comparison.append(result)
    return comparison


def main():
    candidates_path = DERIVED / "2020_candidates.parquet"
    leaders_path = DERIVED / "2020_vote_leaders.parquet"
    candidates = read_rows(candidates_path)
    leaders = read_rows(leaders_path)
    groups = listing_groups(read_csv(LISTING))
    crosswalk = make_crosswalk(pdf_bodies(candidates), groups)
    comparison = compare_winners(leaders, candidates, crosswalk, groups)
    crosswalk_path = DERIVED / "2020_body_crosswalk.parquet"
    comparison_path = DERIVED / "2020_vote_leader_comparison.parquet"
    write_rows(crosswalk_path, crosswalk, CROSSWALK_FIELDS)
    write_rows(comparison_path, comparison, COMPARISON_FIELDS)
    audit = {
        "pdf_candidates_sha256": sha256(candidates_path),
        "pdf_leaders_sha256": sha256(leaders_path),
        "listing_sha256": sha256(LISTING),
        "crosswalk_sha256": sha256(crosswalk_path),
        "comparison_sha256": sha256(comparison_path),
        "pdf_body_rows": len(crosswalk),
        "listing_body_groups": len(groups),
        "pdf_ward_rows": len(leaders),
        "listing_rows": sum(len(group["wards"]) for group in groups.values()),
        "mapping_status": dict(Counter(r["mapping_status"] for r in crosswalk)),
        "match_status": dict(Counter(r["listing_match_status"] for r in comparison)),
        "joined_unique_max": sum(
            r["listing_match_status"] == "joined"
            and r["pdf_winner_status"] == "unique_max"
            for r in comparison
        ),
        "name_equal": sum(r["name_normalized_equal"] is True for r in comparison),
        "party_equal": sum(r["party_normalized_equal"] is True for r in comparison),
        "gender_relation_on_name_match": dict(
            Counter(
                r["gender_relation_on_name_match"]
                for r in comparison
                if r["gender_relation_on_name_match"]
            )
        ),
    }
    (DERIVED / "2020_comparison.audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(audit, sort_keys=True))


if __name__ == "__main__":
    main()
