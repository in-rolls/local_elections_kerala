"""Checks that SEC/LSG body joins require distinct ward evidence."""

from local_elections_kerala.parse.sec_2020_compare import (
    choose_mapping,
    listing_groups,
    make_crosswalk,
)


def listing_row(tier, body, ward_number, ward_name):
    return {
        "LGI Type": tier,
        "District": "Alappuzha",
        tier: body,
        "Ward No.": str(ward_number),
        "Ward Name": ward_name,
    }


def test_spelling_alias_with_matching_wards_is_accepted():
    rows = [listing_row("Block", "Haripad", n, f"Ward {n}") for n in range(1, 9)]
    groups = listing_groups(rows)
    pdf_body = {
        "district": "ALAPPUZHA",
        "tier": "Block",
        "name": "Harippad",
        "wards": {n: f"WARD{n}" for n in range(1, 9)},
    }
    group_id, status, matches, *_ = choose_mapping(pdf_body, groups)
    assert group_id == "Block:ALAPPUZHA:HARIPAD"
    assert status in {"ward_name_mapping", "body_and_ward_mapping"}
    assert matches == 8


def test_identical_competing_body_evidence_stays_unresolved():
    rows = [
        listing_row("Block", body, n, f"Ward {n}")
        for body in ("North", "South")
        for n in range(1, 9)
    ]
    groups = listing_groups(rows)
    pdf_body = {
        "district": "ALAPPUZHA",
        "tier": "Block",
        "name": "Unknown",
        "wards": {n: f"WARD{n}" for n in range(1, 9)},
    }
    group_id, status, *_ = choose_mapping(pdf_body, groups)
    assert group_id is None
    assert status == "ambiguous_or_weak_evidence"


def test_unresolved_crosswalk_keeps_proposal_without_join():
    rows = [listing_row("Block", "North", n, f"Ward {n}") for n in range(1, 9)]
    groups = listing_groups(rows)
    pdf_body = {
        "district": "ALAPPUZHA",
        "tier": "Block",
        "name": "Unknown",
        "wards": {n: f"OTHER{n}" for n in range(1, 9)},
    }
    row = make_crosswalk({"B04001": pdf_body}, groups)[0]
    assert row["mapping_status"] == "ambiguous_or_weak_evidence"
    assert row["listing_group_id"] is None
    assert row["proposed_listing_group_id"] == "Block:ALAPPUZHA:NORTH"
    assert row["proposed_listing_body_raw"] == "North"
