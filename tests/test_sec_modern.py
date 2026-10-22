"""Regression checks for multiline SEC vote extraction and tie handling."""

from local_elections_kerala.parse.sec_2020 import parse_text, winners


def test_multiline_votes_are_not_confused_with_age_or_address_numbers():
    first = (
        "ALAPPUZHA   B04031   Thykkattussery   B04031001   Arookkutty   "
        "INC   Animole   M   50 Kavusseri, 688535"
    )
    second = (
        "ALAPPUZHA   B04031   Thykkattussery   B04031001   Arookkutty   "
        "BJP   Mini   M   47 House"
    )
    text = first + "\n" + " " * 120 + "3164\n" + second.ljust(120) + "1086\f"
    rows, errors = parse_text(text)
    assert errors == []
    assert [row["votes"] for row in rows] == [3164, 1086]
    assert [row["candidate_name_raw"] for row in rows] == ["Animole", "Mini"]
    assert winners(rows)[0]["winner_name_raw"] == "Animole"


def test_tied_top_votes_have_no_inferred_winner():
    rows = [
        {
            "ward_code": "G04005001",
            "votes": 100,
            "page": 1,
            "page_line": 1,
            "candidate_name_raw": "A",
            "party_raw": "INC",
        },
        {
            "ward_code": "G04005001",
            "votes": 100,
            "page": 1,
            "page_line": 2,
            "candidate_name_raw": "B",
            "party_raw": "BJP",
        },
    ]
    winner = winners(rows)[0]
    assert winner["winner_status"] == "tied_max"
    assert winner["winner_name_raw"] is None
