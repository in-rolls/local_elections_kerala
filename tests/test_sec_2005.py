"""Regression checks for the retained 2005 SEC reports."""

import io
import json
from urllib.error import URLError

from local_elections_kerala.acquire import sec_reports as acquire
from local_elections_kerala.parse import sec_2005 as parse
from local_elections_kerala.parse import sec_2005_compare as compare
from local_elections_kerala.parse.to_parquet import DATA, old_rows


class Response(io.BytesIO):
    status = 200

    def __init__(self, payload):
        super().__init__(payload)
        self.headers = {"Content-Type": "application/pdf"}


def test_acquisition_logs_retry_and_verified_receipt(tmp_path, monkeypatch):
    pdf = (acquire.DEFAULT_OUT / "thiruvananthapuram.pdf").read_bytes()
    calls = 0

    def urlopen(request, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise URLError("temporary outage")
        return Response(pdf)

    monkeypatch.setattr(acquire, "urlopen", urlopen)
    row = {
        "document_id": "pilot",
        "source_url": "https://example.org/report.pdf",
        "archive_url": "",
    }
    receipt = acquire.fetch_one(row, tmp_path, tmp_path / "attempts.jsonl", 2, 1)
    events = [
        json.loads(line)
        for line in (tmp_path / "attempts.jsonl").read_text().splitlines()
    ]
    assert [event["status"] for event in events] == ["failed", "ok"]
    assert receipt["status"] == "ok"
    assert receipt["sha256"] == acquire.sha256(tmp_path / "pilot.pdf")
    assert receipt["page_count"] == 438


def test_acquisition_rejects_html_and_records_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(
        acquire, "urlopen", lambda request, timeout: Response(b"<html>error</html>")
    )
    row = {
        "document_id": "bad",
        "source_url": "https://example.org/bad.pdf",
        "archive_url": "",
    }
    receipt = acquire.fetch_one(row, tmp_path, tmp_path / "attempts.jsonl", 2, 1)
    assert receipt["status"] == "failed"
    assert not (tmp_path / "bad.pdf").exists()
    assert (
        json.loads((tmp_path / "bad.receipt.json").read_text())["errors"][0][
            "error_type"
        ]
        == "ValueError"
    )


def test_coverage_distinguishes_failed_unattempted_and_invalid_receipts(tmp_path):
    manifest = tmp_path / "manifest.csv"
    manifest.write_text("document_id\nready\nfailed\nuntouched\n")
    (tmp_path / "ready.pdf").write_bytes(b"%PDF-verified")
    (tmp_path / "ready.receipt.json").write_text(
        json.dumps(
            {
                "status": "ok",
                "sha256": acquire.sha256(tmp_path / "ready.pdf"),
                "page_count": 1,
            }
        )
    )
    (tmp_path / "failed.receipt.json").write_text(json.dumps({"status": "failed"}))
    rows = [
        {"document_id": doc, "district": "X", "kind": "district_report"}
        for doc in ("ready", "failed", "untouched")
    ]
    result = acquire.coverage(rows, tmp_path, manifest)
    assert result["counts"] == {"ok": 1, "failed": 1, "not_attempted": 1}
    (tmp_path / "ready.pdf").write_bytes(b"tampered")
    result = acquire.coverage(rows, tmp_path, manifest)
    assert result["counts"] == {"invalid_receipt": 1, "failed": 1, "not_attempted": 1}


def test_pdf_parser_handles_tier_change_before_page_header():
    source = parse.SOURCE / "thiruvananthapuram.pdf"
    rows, problems = parse.extract(source, "thiruvananthapuram", "Thiruvananthapuram")
    groups, issues, _ = parse.audit(rows, problems)
    assert len(rows) == 6273
    assert len(groups) == 1740
    assert not any(
        issue["problem"] == "candidate_row_count_mismatch" for issue in issues
    )
    elected = {
        (r["lgi_type"], r["body_no"], r["ward_no"]): r
        for r in rows
        if r["status"] == "Elected"
    }
    assert elected[("Block", 1, 1)]["candidate_name"] == "Biju R S"
    assert elected[("Block", 1, 1)]["votes"] == "4708"
    assert elected[("Corporation", 1, 1)]["candidate_name"] == "L Ajitha"
    assert elected[("Corporation", 1, 1)]["source_page"] == 36


def test_cross_source_comparison_catches_missing_old_values():
    paths = [
        parse.DERIVED / f"{district}_candidates.csv"
        for district in ("thiruvananthapuram", "kozhikode")
    ]
    old = compare.old_winners(DATA / "lsgi-election-kerala-2005.csv")
    pdf = compare.pdf_winners(paths)
    mappings, ambiguous = compare.find_matches(pdf, old)
    rows = compare.compare(pdf, old, mappings)
    assert len(mappings) == 190
    assert not ambiguous
    statuses = {
        status: sum(row["comparison_status"] == status for row in rows)
        for status in {row["comparison_status"] for row in rows}
    }
    assert statuses == {
        "match": 3388,
        "old_votes_missing": 8,
        "old_winner_missing": 2,
        "name_disagreement": 1,
        "ward_missing_from_pdf": 1,
    }


def test_2005_heading_district_exposed_without_changing_raw_label():
    rows, _ = old_rows(DATA / "lsgi-election-kerala-2005.csv")
    koz = next(
        row
        for row in rows
        if row["lgi_type"] == "District" and row["district_heading_raw"] == "Kozhikode"
    )
    assert koz["district_raw"] == "Thiruvananthapuram"
    assert "district_label_mismatch" in koz["quality_flags"]
