# 2005 State Election Commission reports

The 2005 LSG member-profile PHP pages often return literal `***` values. Their Malayalam labels use the legacy ML-TTRevathi font; decoding the labels does not recover missing profile values. A small page pilot found some populated profiles, but the linked photograph for the example Amboori profile was missing. This collection therefore uses the [State Election Commission's 2005 report catalog](https://www.sec.kerala.gov.in/public/elercd/index/Election_Results/5) for candidate and vote data. These reports do not supply a replacement for the missing profile demographics, reservation, or photographs.

## Why the PHP profiles are excluded

The [Amboori example](https://lsgkerala.gov.in/election2005/personalInfo.php?t=5&d=1&lb=290&cid=5526) returns `***` in its profile value cells in the HTTP response itself. Its labels render as mojibake without the legacy Malayalam font, but changing character decoding cannot turn those value placeholders into data. The listing page still gives elected names and votes. In a convenience sample of 31 live profile pages across 10 bodies, 24 had only placeholders and seven had at least one populated field. This sample is not a coverage estimate. Sampled Wayback captures also contained placeholders, and the page source exposed no separate JSON or XHR endpoint. The example photograph URL returned 404. PHP profile fields are therefore not merged into the candidate data. Any later profile collection should log page-level status, preserve raw responses, measure populated-field rates by body, and cross-check values against an independent source before release.

## Sources and coverage

[`../reference/sec_2005_documents.csv`](../../reference/sec_2005_documents.csv) lists 14 district reports and the head/deputy-head report, with canonical SEC URLs. Two district PDFs were recovered from archived copies of the SEC's former `electionker.org` site and are retained in [`2005_reports/`](2005_reports/). Each PDF has a JSON acquisition receipt with its download URL, time, byte count, page count, and SHA-256. [`2005_reports/attempts.jsonl`](2005_reports/attempts.jsonl) records every HTTP attempt, including errors and retries. [`2005_reports/coverage.json`](2005_reports/coverage.json) is the manifest-wide inventory: `ok`, `failed`, `not_attempted`, or `invalid_receipt`. A file is `ok` only when its bytes match its receipt. The inventory is regenerated on each acquisition run.

The retained PDFs cover Thiruvananthapuram (438 pages) and Kozhikode (328 pages). The other 12 district reports and the head/deputy-head report were attempted from this environment and timed out; their failed receipts are retained. No candidate data from them are included here. A failed fetch and an unattempted document are never counted as an empty district.

## Reproduce

```sh
uv sync --frozen --group dev
uv run python -m local_elections_kerala.acquire.sec_reports --only thiruvananthapuram kozhikode
uv run python -m local_elections_kerala.parse.sec_2005 thiruvananthapuram Thiruvananthapuram
uv run python -m local_elections_kerala.parse.sec_2005 kozhikode Kozhikode
uv run python -m local_elections_kerala.parse.sec_2005_compare \
  data/interim/sec/2005/thiruvananthapuram_candidates.parquet \
  data/interim/sec/2005/kozhikode_candidates.parquet
make check
```

Omit `--only` to attempt the whole manifest. The downloader retries connection and HTTP failures with exponential waits, writes a failed receipt after all candidate URLs fail, and exits nonzero when any selected document fails. It skips an existing PDF only after verifying its receipt and hash. The parser refuses a default source file without a matching successful receipt; `--source` permits an explicit offline input for investigation. If a fatal page or candidate-row mismatch occurs, the parser writes an audit and exits without replacing the candidate CSV.

## Output and validation

The candidate tables in [`interim/sec/2005/`](../../interim/sec/2005/) carry source document and page, district, body type/number/name, ward number/name, candidate serial/name/party/votes/status, and poll totals. English names are copied from the printed report; wrapped or colliding text can be imperfect. The parser's `.audit.json` files record source and output hashes, candidate and ward counts, every ward issue, and warnings. `ward_validated=True` means exactly one elected candidate, a complete candidate serial sequence, usable candidate fields and valid-vote total, and candidate votes summing to valid votes. A blank or unreadable printed poll/invalid total is a warning; it is not filled by arithmetic.

The two PDFs contain **11,071 candidates** and **3,399 elected wards**. Of these, **3,354 wards** pass the PDF checks. [`winner_comparison.parquet`](../../interim/sec/2005/winner_comparison.parquet) links all 190 PDF bodies to the original LSG winner table using distinctive ward-number/name/vote triples; the [comparison audit](../../interim/sec/2005/winner_comparison.audit.json) records hashes, unmatched bodies, and disagreements. **3,388 winners** match the old listing on name and vote; the remaining cases include eight blank old vote cells, two blank old names, one name variant, and one old ward absent from the PDF. **3,343 wards** pass the PDF checks and match the old winner listing. Use both `ward_validated=True` and `comparison_status=match` for cross-checked candidate analysis. For turnout calculations, additionally require numeric poll and invalid-vote cells and verify `votes_polled = valid_votes + invalid_votes`.

The old 2005 CSV itself has **541 district-label mismatches**: 311 district-panchayat rows and 230 corporation rows reuse `Thiruvananthapuram` while their source headings name another district. The typed 2005 Parquet export preserves `district_raw`, adds `district_heading_raw`, and flags mismatches. Do not group 2005 district or corporation records by `district_raw` alone.

## Next collection steps

1. Reattempt failed canonical SEC URLs from a network that can reach the SEC host, or locate independently archived copies. Keep the manifest and receipts rather than interpreting network failure as no election record.
2. Run the same parser and audit per recovered district. Stop publication of a district if its page header, candidate-row count, or ward geography cannot be parsed. Review ward-level issues against page images; do not infer missing values merely to satisfy totals.
3. Compare winners against the original LSG listing and inspect every disagreement. Keep district files separate until all report identities, tier boundaries, ward counts, and source hashes pass.
4. Seek separate official reservation and demographic sources. Candidate votes and elected status in these reports cannot establish those variables.
5. For 2010–2020, audit the already flagged district labels and the conflicts between winner sex and woman-reserved wards against official year-specific records before changing the retained exports. Keep source sex labels separate from inferred or profile sex.
