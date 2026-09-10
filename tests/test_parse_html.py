"""Exercise table selection and incomplete-input reporting."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
from parse_html import parse_table

FIXTURE = Path(__file__).parent / "fixtures/ward_table.html"


def test_selects_ward_table_and_keeps_order_and_script():
    records = parse_table(FIXTURE.read_text())
    assert len(records) == 2
    assert records[0]["ward_no_raw"] == "01"
    assert (
        records[0]["member_url"]
        == "https://lsgkerala.gov.in/en/lbelection/electdmemberpersondet/2010/example"
    )
    assert records[1]["ward_name_raw"] == "രണ്ട്"
    assert records[1]["party_raw"] is None


@pytest.mark.parametrize(
    "html",
    [
        "<html>Error</html>",
        '<div id="block-zircon-content"><table><tr><td>Login</td></tr></table></div>',
    ],
)
def test_missing_election_table_fails(html):
    with pytest.raises(ValueError):
        parse_table(html)


def test_failed_input_keeps_previous_output_and_records_failure(tmp_path):
    invalid = tmp_path / "invalid.html"
    invalid.write_text("<html>error</html>")
    output = tmp_path / "parsed.parquet"
    output.write_bytes(b"previous")
    result = subprocess.run(
        [
            sys.executable,
            str(FIXTURE.parents[2] / "scripts/parse_html.py"),
            str(FIXTURE),
            str(invalid),
            "--out",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert output.read_bytes() == b"previous"
    report = json.loads(output.with_suffix(".manifest.json").read_text())
    assert len(report["sources"]) == len(report["failures"]) == 1
