"""Checks for ward codes that wrap in certified SEC winner gazettes."""

from local_elections_kerala.parse.sec_2020_winners import (
    parse_rural_page,
    parse_urban_page,
)


def test_rural_ward_number_can_precede_body_code_by_two_lines():
    page = (
        " 1 D14001-Kasaragod       001-Vorkady\n"
        "                      002-Puthige\n"
        "                       member address\n"
        " 2 D14001-Kasaragod\n"
    )
    rows, failures = parse_rural_page(page, 1, "gazette.pdf")
    assert failures == []
    assert [row["ward_code"] for row in rows] == ["D14001001", "D14001002"]
    assert rows[1]["number_position"] == "previous_2"


def test_urban_ward_numbers_are_padded_to_three_digits():
    page = " 10 M01001- body                         10-member\n"
    rows = parse_urban_page(page, 2, "urban.pdf")
    assert rows[0]["ward_code"] == "M01001010"


def test_rural_body_code_can_precede_ward_number():
    page = " G14026-Kodom                 member\n 452      001-Vayambu\n"
    rows, failures = parse_rural_page(page, 1, "gazette.pdf")
    assert failures == []
    assert rows[0]["ward_code"] == "G14026001"
    assert rows[0]["number_position"] == "next_1"
