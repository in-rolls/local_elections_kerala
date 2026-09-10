"""Keep duplicated source headers, structural rows, and quality flags explicit."""

import csv

import pyarrow.parquet as pq
import pytest
from to_parquet import (
    DETAIL_HEADERS,
    GEO_HEADERS,
    modern_rows,
    old_rows,
    read_csv,
    schema,
    write_table,
)


def save(path, header, rows):
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def test_duplicate_headers_preserve_both_ward_values(tmp_path):
    header = GEO_HEADERS + DETAIL_HEADERS
    row = [""] * len(header)
    row[:3] = ["2020", "District", "Source district"]
    row[7:9] = ["001", "Listing ward"]
    row[13:15] = ["002", "Profile ward"]
    row[18] = "0012345678"
    path = save(tmp_path / "annual.csv", header, [row, row])
    result = modern_rows(path, 2020)
    assert len(result) == 2
    assert result[0]["ward_no_raw"] == "001"
    assert result[0]["profile_ward_no_raw"] == "002"
    assert result[0]["mobile_raw"] == "0012345678"
    assert result[0]["age_raw"] is None
    assert result[0]["district_raw"] == "Source district"
    assert result[0]["quality_flags"] == [
        "district_label_carried_from_first_body",
        "ward_profile_mismatch",
    ]
    assert [r["source_row"] for r in result] == [1, 2]
    import pyarrow as pa

    table = pa.Table.from_pylist(result, schema=schema())
    target = tmp_path / "release.parquet"
    write_table(target, table)
    assert pq.read_table(target).equals(table, check_metadata=True)


def test_2005_headers_are_not_seats_and_body_context_is_retained(tmp_path):
    geo = ["2005", "Grama Panchayat", "Example district", "", "", "", "Example GP"]
    path = save(
        tmp_path / "old.csv",
        [*GEO_HEADERS, "0", "1", "2", "3"],
        [
            geo + ["Example body"] * 4,
            [*geo, "Ward No", "Elected Members", "Front", "Votes"],
            [*geo, "01", "Example member", "Example front", "Unopposed"],
        ],
    )
    rows, excluded = old_rows(path)
    assert excluded == {"body_heading": 1, "column_header": 1}
    assert len(rows) == 1
    assert rows[0]["ward_no_raw"] == "01"
    assert rows[0]["votes_raw"] == "Unopposed"
    assert rows[0]["body_heading_raw"] == "Example body"
    assert rows[0]["source_row"] == 3
    assert rows[0]["quality_flags"] == ["reservation_not_collected"]


def test_unknown_2005_row_is_not_silently_dropped(tmp_path):
    row = ["2005", "District", "", "", "", "", "", "unknown", "x", "y", "z"]
    path = save(tmp_path / "old.csv", [*GEO_HEADERS, "0", "1", "2", "3"], [row])
    with pytest.raises(ValueError, match="unrecognized 2005"):
        old_rows(path)


@pytest.mark.parametrize("content", ["", "x,y\na\n"])
def test_empty_or_ragged_csv_fails(tmp_path, content):
    path = tmp_path / "broken.csv"
    path.write_text(content)
    with pytest.raises(ValueError, match="empty or ragged"):
        read_csv(path)


def test_failed_write_preserves_existing_output(tmp_path, monkeypatch):
    target = tmp_path / "release.parquet"
    target.write_bytes(b"previous")

    def fail(_table, path, **_kwargs):
        path.write_bytes(b"unfinished")
        raise OSError("interrupted")

    monkeypatch.setattr(pq, "write_table", fail)
    with pytest.raises(OSError):
        write_table(target, None)
    assert target.read_bytes() == b"previous"
    assert not target.with_suffix(".parquet.part").exists()
