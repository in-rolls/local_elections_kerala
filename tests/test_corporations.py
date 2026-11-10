from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from local_elections_kerala.parse.corporations import (
    SCHEMA,
    build,
    equal,
    unique_index,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    output = tmp_path_factory.mktemp("corporations")
    rows, audit = build(ROOT, output)
    return rows, audit, output


def test_unicode_comparison_and_duplicate_rejection():
    assert equal("V. K. PRASANTH", "V K PRASANTH")
    assert not equal("ആന്റണി", "ഷൈനി")
    assert not equal(None, "")
    with pytest.raises(ValueError, match="Duplicate key"):
        unique_index([{"ward": 1}, {"ward": 1}], lambda row: row["ward"])


def test_frame_conservation_and_schema(built):
    rows, audit, output = built
    assert len(rows) == 1608
    assert len({(r["year"], r["ward_code"]) for r in rows}) == 1608
    assert (audit["profile_rows"], audit["sec_rows"], audit["secondary_rows"]) == (
        1185,
        412,
        1246,
    )
    table = pq.read_table(output / "source_comparison.parquet")
    assert table.schema == SCHEMA
    assert table.schema.field("provisional_winner_woman").type == pa.bool_()
    assert not any(r["analysis_eligible"] for r in rows)
    assert not any(r["provisional_winner_certified"] for r in rows)


def test_missing_profiles_preserve_provisional_election_results(built):
    rows, audit, _ = built
    assert set(audit["missing_profiles_2015"]) == {"C01001001", "C07003039"}
    by_key = {(r["year"], r["ward_code"]): r for r in rows}
    for key, name, votes in [
        ((2015, "C01001001"), "V K PRASANTH", 4364),
        ((2015, "C07003039"), "T J VINOD", 1406),
    ]:
        row = by_key[key]
        assert row["provisional_winner_name"] == name
        assert row["provisional_winner_votes"] == votes
        assert row["provisional_winner_woman"] is None
        assert row["profile_reservation_raw"] is None
    assert audit["missing_secondary_2025"] == ["C01001066"]


def test_tie_and_sex_disagreement_are_not_silently_repaired(built):
    rows, audit, _ = built
    by_key = {(r["year"], r["ward_code"]): r for r in rows}
    tie = by_key[2020, "C07003065"]
    assert tie["secondary_candidate_raw"]
    assert tie["provisional_winner_name"] is None
    assert tie["provisional_winner_status"] == "unresolved_vote_tie"
    assert audit["gender_pairs_on_exact_name_match_2020"] == {
        "M / Female": 100,
        "F / Male": 71,
        "M / Male": 1,
    }
    renya = by_key[2020, "C08004004"]
    assert renya["sec_gender_raw"] == "M"
    assert renya["profile_sex_raw"] == "Male"
    assert renya["profile_sex_reservation_conflict"]
    assert renya["provisional_winner_woman"] is None
    for row in rows:
        if row["provisional_winner_woman"] is not None:
            assert row["profile_identity_link"]
            assert not row["profile_sex_reservation_conflict"]
            assert row["provisional_winner_woman"] == (
                row["profile_sex_raw"] == "Female"
            )


def test_boundary_exceptions_and_unmatched_outcomes(built):
    _, audit, output = built
    links = pq.read_table(output / "boundary_supported_links.parquet").to_pylist()
    assert audit["boundary_supported_links"] == 718
    first = [r for r in links if r["end_year"] == 2015]
    assert len(first) == 304
    assert {r["body_code"] for r in first} == {
        "C01001",
        "C07003",
        "C08004",
        "C11005",
    }
    assert sum(r["end_year"] == 2020 for r in links) == 414
    assert not any(r["analysis_eligible"] for r in links)
    missing = next(
        r for r in first if r["body_code"] == "C07003" and r["ward_number"] == 39
    )
    assert missing["outcome_reservation_raw"] is None
    assert missing["provisional_winner_woman"] is None


def test_secondary_sources_are_in_canonical_fetch_log():
    from local_elections_kerala.build.fetch_log import rows

    records = {row["path"]: row for row in rows()}
    key = "data/raw/secondary/farzzn/Kerala_LocalBodies_Election_2015_MASTER.csv"
    assert records[key]["url"].startswith("https://raw.githubusercontent.com/")
    assert records[key]["fetched_at_utc"]
    assert records[key]["source"] == "fa12d39aaacc842370b5674dc8ef59befaea30a3"
