"""Download SEC report PDFs with a receipt for every attempted source."""

import argparse
import csv
import hashlib
import json
import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pdfplumber
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "data/search_log/sec/sec_2005_documents.csv"
DEFAULT_OUT = ROOT / "data/raw/sec/2005_reports"
DEFAULT_RECORDS = ROOT / "data/search_log/sec/2005_reports"
LOGGER = logging.getLogger(__name__)


def utc_now():
    return datetime.now(UTC).isoformat()


def append_jsonl(path, item):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(item, sort_keys=True) + "\n")


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def pdf_pages(path):
    with pdfplumber.open(path) as document:
        pages = len(document.pages)
        if not pages:
            raise ValueError("PDF has no pages")
        document.pages[0].extract_text()
        document.pages[-1].extract_text()
    return pages


def fetch_one(row, out, attempts_log, max_attempts, timeout, records=None):
    """Fetch one document into `out`; its receipt goes to `records` (default `out`)."""
    target = out / f"{row['document_id']}.pdf"
    receipt_path = (records or out) / f"{row['document_id']}.receipt.json"
    if target.exists() and receipt_path.exists():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("status") == "ok" and receipt.get("sha256") == sha256(target):
            receipt["page_count"] = pdf_pages(target)
            LOGGER.info("verified existing %s", row["document_id"])
            return receipt
    urls = [u for u in (row["archive_url"], row["source_url"]) if u]
    errors = []
    for url in urls:
        retryer = Retrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception_type((HTTPError, URLError, TimeoutError, OSError)),
            reraise=True,
        )
        try:
            for attempt in retryer:
                with attempt:
                    started = utc_now()
                    part = target.with_suffix(".pdf.part")
                    try:
                        request = Request(
                            url,
                            headers={"User-Agent": "local-elections-kerala/1.0"},
                        )
                        with (
                            urlopen(request, timeout=timeout) as response,
                            part.open("wb") as stream,
                        ):
                            status = response.status
                            content_type = response.headers.get("Content-Type", "")
                            while chunk := response.read(1024 * 1024):
                                stream.write(chunk)
                        if (
                            part.stat().st_size < 100
                            or part.open("rb").read(5) != b"%PDF-"
                        ):
                            raise ValueError("Response is not a PDF")
                        pages = pdf_pages(part)
                        digest = sha256(part)
                        part.replace(target)
                        attempt_row = {
                            "document_id": row["document_id"],
                            "url": url,
                            "started_at": started,
                            "finished_at": utc_now(),
                            "attempt": attempt.retry_state.attempt_number,
                            "status": "ok",
                            "http_status": status,
                            "content_type": content_type,
                            "bytes": target.stat().st_size,
                            "sha256": digest,
                            "page_count": pages,
                        }
                        append_jsonl(attempts_log, attempt_row)
                        receipt = {
                            **attempt_row,
                            "source_url": row["source_url"],
                            "archive_url": row["archive_url"],
                        }
                        receipt_path.write_text(
                            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
                            encoding="utf-8",
                        )
                        LOGGER.info("downloaded %s from %s", row["document_id"], url)
                        return receipt
                    except Exception as exc:
                        part.unlink(missing_ok=True)
                        attempt_row = {
                            "document_id": row["document_id"],
                            "url": url,
                            "started_at": started,
                            "finished_at": utc_now(),
                            "attempt": attempt.retry_state.attempt_number,
                            "status": "failed",
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        }
                        append_jsonl(attempts_log, attempt_row)
                        LOGGER.warning(
                            "%s attempt %s failed: %s",
                            row["document_id"],
                            attempt.retry_state.attempt_number,
                            exc,
                        )
                        raise
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            errors.append(
                {"url": url, "error_type": type(exc).__name__, "error": str(exc)}
            )
    receipt = {
        "document_id": row["document_id"],
        "status": "failed",
        "finished_at": utc_now(),
        "errors": errors,
    }
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return receipt


def coverage(rows, out, manifest, records=None):
    records = records or out
    documents = []
    for row in rows:
        document_id = row["document_id"]
        receipt_path = records / f"{document_id}.receipt.json"
        source_path = out / f"{document_id}.pdf"
        if not receipt_path.exists():
            status = "not_attempted"
            receipt = {}
        else:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            status = receipt.get("status", "invalid_receipt")
            if status == "ok" and (
                not source_path.exists() or receipt.get("sha256") != sha256(source_path)
            ):
                status = "invalid_receipt"
        documents.append(
            {
                "document_id": document_id,
                "district": row["district"],
                "kind": row["kind"],
                "status": status,
                "sha256": receipt.get("sha256") if status == "ok" else None,
                "page_count": receipt.get("page_count") if status == "ok" else None,
            }
        )
    result = {
        "generated_at": utc_now(),
        "manifest_sha256": sha256(manifest),
        "counts": dict(Counter(doc["status"] for doc in documents)),
        "documents": documents,
    }
    (records / "coverage.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--records", type=Path, default=DEFAULT_RECORDS)
    parser.add_argument(
        "--only", nargs="*", help="Document IDs to attempt; default is all"
    )
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=20)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.max_attempts < 1 or args.timeout <= 0:
        parser.error("max-attempts and timeout must be positive")
    with args.manifest.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    selected = set(args.only) if args.only else {row["document_id"] for row in rows}
    unknown = selected - {row["document_id"] for row in rows}
    if unknown:
        parser.error(f"unknown document IDs: {', '.join(sorted(unknown))}")
    args.out.mkdir(parents=True, exist_ok=True)
    args.records.mkdir(parents=True, exist_ok=True)
    attempts_log = args.records / "attempts.jsonl"
    results = [
        fetch_one(
            row,
            args.out,
            attempts_log,
            args.max_attempts,
            args.timeout,
            args.records,
        )
        for row in rows
        if row["document_id"] in selected
    ]
    summary = {
        "selected": len(results),
        "selected_ok": sum(r["status"] == "ok" for r in results),
        "selected_failed": sum(r["status"] == "failed" for r in results),
        "not_selected": len(rows) - len(results),
    }
    inventory = coverage(rows, args.out, args.manifest, args.records)
    summary["inventory"] = inventory["counts"]
    print(json.dumps(summary, sort_keys=True))
    if summary["selected_failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
