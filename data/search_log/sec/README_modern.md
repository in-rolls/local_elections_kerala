# SEC reports and retained LSG sources, 2010–2020

The [candidate-report manifest](sec_modern_documents.csv) identifies the State Election Commission's candidate-vote PDFs for [2010](https://sec.kerala.gov.in/public/elercd/index/Election_Results/6), [2015](https://sec.kerala.gov.in/public/elercd/index/Election_Results/7), and [2020](https://www.sec.kerala.gov.in/public/elercd/index/Election_Results/1). Each manifest row carries the canonical SEC URL and, where found, an archived copy. `source/attempts.jsonl` records every fetch attempt; each `*.receipt.json` and `source/coverage.json` distinguish a downloaded, failed, or unattempted report. A failed receipt is evidence of an inaccessible URL from this environment, not evidence that the report does not exist.

At collection on 2026-10-21/22 UTC, the archived 2020 report downloaded successfully (1,803 pages, SHA-256 `664bd7501ff9c203fee58395f531e0f3839c5e9ad9ab6ba523c55913f310667d`). Two attempts per canonical 2010 and 2015 URL timed out. [Exact-URL Internet Archive checks](modern_candidates/archive_lookup.json) across HTTP/HTTPS and both SEC host names found no captures. Their status is `failed` in the coverage file; neither PDF has been parsed or represented as acquired data. The archive source for 2020 is a 2025-12-15 Wayback capture of the SEC download URL. Source receipts provide exact attempt times and URLs.

## Derived data

- `build/sec/modern/2020_candidates.parquet`: 74,693 candidate rows from the retained 2020 PDF, including original SEC body and ward codes, page and line provenance, and numeric votes. Names, party, age, and gender are extracted only when the text fields align; 869 rows have `field_parse_status=partial`. The original PDF is the authority for these rows.
- `build/sec/modern/2020_vote_leaders.parquet`: 21,821 PDF wards. A unique highest vote count exists in 21,782; 39 have tied maxima and no inferred winner. A unique maximum in this report is a vote leader, not a separately certified election result. Twenty-five wards have a top vote count of zero and need source review.
- `build/sec/modern/2020_vote_leader_comparison.parquet`: a conservative join to the 21,886-row LSG 2020 listing. A [body crosswalk](../../build/sec/modern/2020_body_crosswalk.parquet) uses exact names or distinct ward-name evidence, including the CSV’s carried district labels. It maps 1,183 of 1,199 SEC body codes; 16 remain unresolved. Unresolved rows retain a proposed listing group and its evidence for review, without assigning a join. The ward-level comparison joins 21,553 PDF wards, leaves 254 in unresolved bodies, and finds 14 without a listing ward. Among 21,514 joined wards with a unique maximum, 10,052 PDF names and 19,249 party labels agree after case and punctuation normalization. Differences include real source conflicts and spelling/formatting variants; no value is replaced.
- `build/sec/modern/2010_saved_ward_listings.parquet`: 15,841 rows from 930 distinct saved 2010 LSG ward pages. The audit inventories all 1,042 HTML files with checksums: 62 identical duplicate body pages and 50 failures (45 empty election tables, four without a body heading/ID, one malformed row). The retained archive is partial relative to the 21,607-row 2010 CSV. All 15,841 parsed rows join the 2010 CSV with identical listed member and party. Since the saved pages and CSV derive from the same LSG collection, this checks extraction consistency, not independent accuracy.
- `build/sec/modern/modern_sources.audit.json`: source hashes, row counts, tier distribution, field completeness, key checks, and woman-reserved/Male-profile conflicts for all three later years. The conflicts are 33 in 2010, 51 in 2015, and 197 in 2020. They require record-level review; neither sex nor reservation is recoded.
- [2020 certified winner reports](2020_winners/): the [manifest](sec_2020_winners.csv) records 14 rural district gazettes and one urban gazette. Archived copies of 10 were acquired; five SEC URLs timed out with no known archive capture. The retained reports yield 16,283 distinct ward codes. [The comparison](../../build/sec/2020_winners/2020_candidate_certified_comparison.parquet) finds 16,249 codes in both the certified gazettes and candidate-vote PDF, 34 gazette-only codes, and three candidate-only codes within the acquired reports’ scope. The LSG listing has 32 of the 34 gazette-only wards. This validates ward-code coverage, not the identity of the vote leader: names in the rural gazettes are Malayalam and the urban PDF uses a legacy font encoding. Source hashes, page counts, and per-report extraction counts are in the [audit](../../build/sec/2020_winners/2020_certified_coverage.audit.json).

For 10,052 exact normalized name matches, the 2020 SEC PDF gender letter is opposite the LSG profile sex in 9,966 cases and agrees in only 86. A rendered page confirms these are the PDF's printed values. This comparison strongly suggests a source coding problem, but does not establish a correction rule. The SEC gender column is retained as `gender_raw` only; it must not be used as a corrected member-sex variable. The PDF and the LSG listing also disagree on some party labels. Body spellings and the known district-label defect prevent a complete mechanical join. Do not interpret unmatched rows as absent elections, or compare 2010/2015/2020 wards as a stable panel without boundary review.

## Reproduce

Use `uv sync --frozen --group dev` and install Poppler (`pdftotext`) before parsing the 2020 PDF. Run acquisition explicitly; a nonzero exit means at least one selected report failed, while receipts and coverage are still written.

```sh
uv run python -m local_elections_kerala.acquire.sec_reports \
  --manifest data/search_log/sec/sec_modern_documents.csv \
  --out data/raw/sec/modern_candidates --only 2020_candidates
uv run python -m local_elections_kerala.parse.sec_2020
uv run python -m local_elections_kerala.parse.sec_2020_compare
uv run python -m local_elections_kerala.acquire.sec_reports \
  --manifest data/search_log/sec/sec_2020_winners.csv \
  --out data/raw/sec/2020_winners
uv run python -m local_elections_kerala.parse.sec_2020_winners
uv run python -m local_elections_kerala.parse.sec_2010_html
uv run python -m local_elections_kerala.parse.sec_modern_audit
make check
```

Acquisition verifies an existing PDF against its receipt and does not redownload it. The parser audit files contain output and source hashes. The 2020 parser stops without replacing derived outputs if a candidate row lacks exactly one aligned vote. `2010_saved_html.audit.json` records failures and duplicate pages; its CSV includes only valid distinct bodies.

## Validation before using the supplement

1. Resolve the 2010 and 2015 candidate-PDF downloads and the five missing 2020 winner gazettes or obtain SEC-certified copies, preserving their URLs, hashes, and receipts. Inspect the report schema for each year and adapt the parser only after a page-level pilot.
2. For 2020, inspect a stratified sample of PDF pages across districts, tiers, wrapped rows, and the 869 partial field parses; manually check vote alignment and names against page images. Review all 39 ties and 25 zero-vote leaders.
3. Review the 16 unresolved body mappings, 14 missing listing wards, 34 gazette-only codes, and three candidate-only codes. Check whether certified winner names agree with inferred vote leaders using a separately validated Malayalam extraction/transliteration method; the current gazette comparison checks codes only. Review the CSV district defect before assigning geographic labels.
4. Reconcile the 50 failed saved 2010 HTML files and determine which are empty because a body did not contest in 2010. Verify coverage against a year-specific official body roster before claiming completeness.
5. Release a joined analysis table only after candidate/ward totals, unique keys, missingness, source hashes, winner agreement, and a manual error sample meet stated thresholds. Preserve the source-specific columns alongside any adjudicated field.
6. Treat the [SEC 2025 result catalog](https://sec.kerala.gov.in/public/elercd/index/Election_Results) as a new election-year collection with its own manifest and boundary review before extending this 2005–2020 dataset.
