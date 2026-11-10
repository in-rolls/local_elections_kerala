# Corporation source review

This handoff preserves the best available election records without treating later member profiles as general-election winners. It covers Thiruvananthapuram, Kollam, Kochi, Thrissur, Kozhikode, and Kannur (a corporation from 2015). No effects were estimated and the published release was not changed.

## Coverage and usable fields

| Election | Ward frame | LSG profiles | Provisional election winners | Linked recorded sex |
|---|---:|---:|---:|---:|
| 2010 | 359 | 359 | — | — |
| 2015 | 414 | 412 | 414 | 197 |
| 2020 | 414 | 414 | 411 | 161 |
| 2025 | 421 | — | 420 | — |

The [typed comparison](../../../build/corporations/source_comparison.parquet) contains all 1,608 ward-years. [Coverage](../../../build/corporations/coverage.csv) separates cities. Names, parties, votes, reservation labels, and recorded sex remain in source-specific columns. The `provisional_winner_*` fields supply the best available election name, party, votes, and, when linked, recorded sex. They are not certified results.

The newly retained [secondary files and manifest](../../../raw/secondary/farzzn/manifest.json) come from Farzzn/ElectionDataScapping at commit `fa12d39aaacc842370b5674dc8ef59befaea30a3`. The 2015 notebook describes the official SEC trend endpoint and chooses the first maximum-vote candidate; it does not establish how ties were decided. The notebook was inspected, not executed. Its 2015 names are English; 2020 and 2025 names are Malayalam. The retained CSVs cover the state; this parser extracts only corporations. Source URLs, UTC acquisition times, byte counts, and SHA-256 hashes are in the manifest.

For 2020 the retained official candidate PDF takes priority over the secondary scrape. Its 412 corporation wards have 411 unique vote maxima and one tie (Kochi 65). One unique leader's name failed the existing PDF parser; the secondary name is retained with a distinct status, supported by the identical official maximum vote total. No matched ward has different maximum votes between these sources. The two absent wards are Kochi 63 and Kannur 10; this review does not establish why. The tie remains unresolved even though the secondary scraper selects a name.

The 421-ward 2025 frame follows the [SEC election report](https://fliphtml5.com/rwsis/Election-Report-2025/). The missing row is Thiruvananthapuram 66, Vizhinjam. [Contemporary reporting](https://www.newindianexpress.com/states/kerala/2026/Jan/13/vizhinjam-bypoll-turns-tide-as-udf-reclaims-ward-after-two-terms) identifies its postponed election after a candidate's death; the reported poll/count dates are January 12–13, 2026. No value was filled from news coverage. The official [2025 urban winner gazette](https://www.sec.kerala.gov.in/public/elercd/download/90941035-a6b5-46e0-b20d-667aa9894a46), EOG 4270, December 19, 2025, was located but its download timed out. Thus 2025 remains a provisional portal-derived handoff, not a certified panel. Ward totals change in 2025; the parser does not link 2020 ward numbers to 2025.

## Historical profiles and recorded sex

The two missing 2015 profiles are Kochi 39 (Karukappilly) and Thiruvananthapuram 1 (Kazhakuttom). The secondary election records identify T. J. Vinod (1,406 votes) and V. K. Prasanth (4,364 votes). The retained official [Kochi budget](../../../raw/sec/urban_review/kochi_budget_2019.pdf), physical PDF page 95 / printed page 11, lists Vinod in ward 39 during 2019. His [Assembly biography](https://niyamasabha.nic.in/index.php/profile/index/2344) records his council service through 2019. Contemporary coverage reports the council departures of [Vinod](https://www.newindianexpress.com/amp/story/cities/kochi/2019/Nov/06/udf-at-mercy-of-bjp-to-retain-deputy-mayor-post-2057592.html) and [Prasanth](https://timesofindia.indiatimes.com/city/thiruvananthapuram/on-final-day-a-smiling-mayor-brought-everyone-to-tears/articleshow/71780658.cms) following their October 2019 Assembly elections. These are consistent with a later-incumbent profile snapshot, not two missing 2015 elections. The absent reservation and sex fields remain null.

The 2020 profiles provide a direct later-incumbent example: Vettucaud (Thiruvananthapuram 90) lists `CLINUSROZARIO L`, whereas the official 2020 candidate report identifies Sabu Jose as the unique vote leader (2,524 votes). [December 2021 reporting](https://www.newindianexpress.com/cities/thiruvananthapuram/2021/Dec/09/sweeping-win-for-ldf-in-lsg-bypolls-2393374.html) records Clenus Rosario winning the by-election after Sabu Jose died. Thus the profile export cannot be treated wholesale as the original 2020 winner roster.

Exact normalized name, party, and ward identify the provisional links to recorded profile sex: 197 in 2015 and 162 in 2020, of which one 2020 sex record is withheld because it conflicts with the reservation label. This is deterministic record linkage, not manual verification of every person. Punctuation, spacing, and case are ignored; Malayalam letters are preserved; missing names never match. Unmatched names may reflect spelling differences or changed incumbents, and are not classified as replacements. No sex is inferred from a name or reservation.

The official 2020 PDF's sex labels are unreliable. Its printed header says `Gender`; inspection of the original PDF confirms the letters, so this is not an OCR-only discrepancy. Among 172 exact name matches, 100 rows pair PDF `M` with profile `Female`, 71 pair `F` with `Male`, and one pairs `M` with `Male`. For example, Kavitha L. S. (Thiruvananthapuram 1; PDF page 1424) matches the profile's name, ward, and party; the profile records Female and the PDF prints M. The sole agreement is Renya Byju (Thrissur 4; PDF page 1620), whose profile records Male in a Woman-reserved ward. That outcome is withheld. The source problem is documented; **no global flip is applied**. Provisional sex uses the separate recorded profile field only for exact identity links without a reservation conflict.

## Boundaries and transition handoff

[SEC Order 552/2015/SEC](https://www.sec.kerala.gov.in/public/ic/download/f3a13514-7939-44d0-a219-75fc3bf8d4b9), September 7, 2015, paragraph 16, physical PDF page 242, retains the 2010 constituencies except 28 new municipalities, new Kannur Corporation, reconstituted Kollam Corporation, six grama panchayats, 30 block panchayats, and 13 district panchayats. For corporations this supports 304 wards in Thiruvananthapuram, Kochi, Thrissur, and Kozhikode in 2010–2015. Kollam and Kannur are excluded from that transition. The same order appears in the SEC 2015 election report, Annexure 3, page 72. Both documents were inspected through the web index; direct downloads failed and are not claimed as retained PDFs.

The retained [CAG 2022 report](../../../raw/sec/urban_review/cag_delimitation_2022.pdf), paragraph 2.5.1, physical PDF page 7 / printed page 11, reports that the contemplated delimitation was not conducted for the 2020 urban election. Together these sources support stable body boundaries for 414 wards in 2015–2020. They do not themselves supply a ward-number crosswalk.

[Boundary-supported links](../../../build/corporations/boundary_supported_links.parquet) contain 304 + 414 = 718 proposed ward-number links. Normalized profile ward names agree in 258/304 and 411/414. Recorded sex is linked to provisional outcome winners in 138 and 161, respectively. Raw prior/outcome reservation labels and ward names remain explicit. These are candidate links for review, not 718 validated observations: no transition is marked analysis-eligible. Selection into exact name matches, profile timing, reservation-source timing, remaining ward-name differences, and the 2020 sex discrepancy preclude treating this subset as a representative complete panel.

## Reproduction and receipts

From the repository root:

```sh
PYTHONPATH=src python -m local_elections_kerala.parse.corporations
PYTHONPATH=src python -m pytest -q tests/test_corporations.py
python -m ruff check src/local_elections_kerala/parse/corporations.py tests/test_corporations.py
```

The parser verifies retained secondary hashes before joining, rejects duplicate/out-of-frame keys, preserves ties and null outcomes, and writes typed Parquet plus coverage and an audit with input/output hashes. It reads the existing release and SEC parse; it does not rebuild rural data. `source_comparison` uses int64 identifiers/counts, nullable booleans for provisional sex, booleans for source/link flags, and strings for raw labels. False link flags mean the exact-match criterion was not met, not proof of distinct people.

The adjacent receipts retain successful and failed requests. `documents.csv` works with the existing `acquire.sec_reports` fetcher using this directory for `--records` and `data/raw/sec/urban_review` for `--out`; no new crawler was added. `archive_lookup.json` records two exact-URL Wayback queries with no returned captures; it does not establish that all archives lack these documents. The 2015 candidate POST also timed out. These failures are limits of this acquisition pass, not evidence that the underlying records do not exist.
