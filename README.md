# Kerala Local Election Data

[![DOI](https://img.shields.io/badge/DOI-10.7910%2FDVN%2F0IUQO1-blue)](https://doi.org/10.7910/DVN/0IUQO1)

Historical Kerala local-election records for 2005, 2010, 2015, and 2020, including elected members, party or front, and seat reservations where collected. This repository preserves the original CSVs and saved HTML, provides typed Parquet exports, and supports offline reproduction. Candidate photographs and the deposited dataset are available on [Harvard Dataverse](https://doi.org/10.7910/DVN/0IUQO1).

## Data

| File | Elections | Records | Contents |
|---|---|---:|---|
| [kerala_2005.parquet](data/fin/kerala_2005.parquet) | 2005 | 20,551 | Ward records, elected members, front, and source vote text |
| [kerala_2010_2020.parquet](data/fin/kerala_2010_2020.parquet) | 2010, 2015, 2020 | 65,296 | Ward listings and member-profile fields |
| [Dataverse release](https://doi.org/10.7910/DVN/0IUQO1) | Historical collection | See deposit | Original data and candidate photographs |

The later export contains 21,607 records for 2010, 21,803 for 2015, and 21,886 for 2020. These are collected records, not independently verified coverage totals. They span district panchayats, block panchayats, grama panchayats, municipalities, and corporations. They do not form a validated panel of stable seats across years.

[MANIFEST.json](data/fin/MANIFEST.json) records schemas, source checksums, export checksums, and row counts. `make verify-data` checks the exports against the declared CSV inputs. Existing source files remain under [data/](data/).

## Column dictionary

The `_raw` suffix marks a value carried from the historical source, without substantive recoding. Empty cells become null. Year is an integer; names, ward identifiers, phone fields, age, and vote text remain strings so conversion does not discard leading zeroes or coerce source labels. Each record carries its source filename and logical CSV row number, starting at 1 after the header.

| Columns | Meaning |
|---|---|
| `year` | Election year; int16 |
| `lgi_type` | Type of local government institution |
| `district_raw`, `block_raw`, `municipality_raw`, `corporation_raw`, `grama_panchayat_raw` | Location/body labels stored by the original scraper; see the district-label defect below |
| `ward_no_raw`, `ward_name_raw` | Ward number and name from the listing; 2005 lacks ward names |
| `elected_member_raw` | Name in the elected-members listing |
| `role_raw`, `party_raw`, `reservation_raw` | Listing fields for 2010–2020 |
| `profile_ward_no_raw`, `profile_ward_name_raw`, `member_name_raw` | Separate ward and member fields from the member profile, 2010–2020 |
| `address_raw`, `phone_raw`, `mobile_raw` | Historical member-profile contact fields, 2010–2020 |
| `age_raw`, `sex_raw`, `marital_status_raw`, `education_raw`, `occupation_raw` | Historical member-profile attributes, 2010–2020 |
| `image_url` | Historical photograph URL; the export does not download images |
| `front_raw`, `votes_raw` | 2005 front and vote cells; vote cells can contain text |
| `body_heading_raw` | 2005 body heading retained from the table above the record |
| `source_file`, `source_row` | Source CSV and int32 row number for auditing |
| `quality_flags` | List of known limitations affecting a record; list of strings |

The two exports have different schemas because the 2005 collection did not include the later profile fields. Seat reservation was not collected for 2005; its absence must not be treated as an unreserved seat. Member sex and seat reservation are different variables. The 2005 `Front` field is retained separately from the later `Party` field.

## Coverage and known gaps

**District labels need review for district-panchayat and corporation records.** The original `scrape_common` function set a missing district from the first body and reused it for subsequent bodies. All district-panchayat records in each of the 2010, 2015, and 2020 combined slices therefore carry `Thiruvananthapuram`. The export preserves the supplied label and flags these two institution types with `district_label_carried_from_first_body`. It does not invent replacement districts. Use the source body context to resolve them before geographic analysis.

**Several CSVs repeat `Ward No.` and `Ward Name` as column headers.** One pair belongs to the listing and the other to the member profile. Reading those files into a dictionary keyed only by header text can discard one pair. The converter reads columns by position and gives the profile fields distinct names. A disagreement receives `ward_profile_mismatch`; no record is deduplicated or selected by that flag.

**The 2005 CSV contains table headings among its rows.** Its 22,997 rows include 1,223 repeated body headings and 1,223 column-header rows. The export retains the 20,551 numeric-ward rows and their body headings. The manifest counts the structural exclusions; the original CSV preserves every row. Unknown row shapes stop conversion for review.

The 2015 collection is split between a 16,558-row file and a 5,245-row supplementary GP file. The historical supplement resumed from Malappuram. The export includes both as distinct source observations and does not assume that their body identifiers constitute a verified unique key.

The combined historical CSV reformatted age values and many 2020 mobile values numerically. The export uses the original annual/supplementary files for 2015 and 2020 to preserve their text representation. Its 2010 slice comes from the combined CSV because the separate 2010 profile CSV is absent. It cannot recover any formatting already lost in that source.

`data/2010/combined_tables.csv` is a separate exploratory table extraction with 16,903 rows and navigation-derived `Login`/`Malayalam` columns. It is retained for provenance and is not substituted for the 21,607-record 2010 profile slice. No claim of complete source recovery or current accuracy of historical profile fields is made.

## How collected

| Election/input | Historical source | Method and retained evidence |
|---|---|---|
| 2005 | [LSG election pages](https://lsgkerala.gov.in/election2005/electionDetails.php) | HTML results tables, with four source data columns retained in the CSV |
| 2010, 2015, 2020 | LSG local-body listings and member-profile pages; [2020 entry point](https://lsgkerala.gov.in/en/lbelection/lbelection/2020) | Joined listing/profile tables and photograph URLs; annual files and the combined CSV are retained |
| Saved 2010 ward pages | `data/2010/downloaded_htmls/` | 1,042 saved HTML files; the offline parser selects the election table by its six headers and retains file/row provenance |

The original collectors assumed fixed table positions, used parallel member requests, and wrote partial CSVs when collection stopped. Their source code and notebooks remain in the [historical implementation](https://github.com/in-rolls/local_elections_kerala/tree/35c5414). The current tools are offline converters and parsers; they make no requests to LSG or Dataverse.

The saved HTML's original capture timestamps and complete request manifest were not recorded. New parser receipts identify the saved file and its checksum, without inventing a retrieval date. The original HTML and `localbodies.pdf` remain available for further provenance work.

## Usage

```sh
git clone https://github.com/in-rolls/local_elections_kerala.git
cd local_elections_kerala
uv sync --frozen --group dev
make verify-data
```

Read an export:

```python
import pyarrow.parquet as pq

table = pq.read_table("data/fin/kerala_2010_2020.parquet")
print(table.num_rows)
print(table.schema)
```

Rebuild the published Parquet files from the retained CSVs:

```sh
make to-parquet
make verify-data
```

Parse a saved ward page into a separate output:

```sh
uv run python scripts/parse_html.py data/2010/downloaded_htmls/document_1000.html --out data/derived/ward_sample.parquet
```

The HTML parser keeps row order, uses the election-table headers to avoid navigation tables, and resolves member links against the LSG host. It does not infer geography from the local filename or join separate profile records by row position. A malformed or missing table produces a failure receipt and a nonzero exit; the existing Parquet output is not replaced.

## Development

```sh
make check
```

Checks cover CSV structure, duplicate-header preservation, 2005 structural rows, missing values, table selection, failure reporting, exact Parquet schema/value comparisons, and source checksums. Run `make verify-data` explicitly for Parquet/schema/source-checksum verification when data change. The environment is managed by uv; these data tools do not require an installed library package.

## Citation

Gaurav Sood. *Kerala Local Election Data*. Version 1.0. Harvard Dataverse. [doi:10.7910/DVN/0IUQO1](https://doi.org/10.7910/DVN/0IUQO1).

[CITATION.cff](CITATION.cff) follows the deposited dataset's registered authorship and version. Also record the repository commit when using these derived exports. Contact: [contact@gsood.com](mailto:contact@gsood.com).

## License

Code is [MIT licensed](LICENSE). The registered Dataverse dataset is released under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/). The deposited dataset's terms should be consulted for its files and photographs.

## 🔗 Adjacent Repositories

- [in-rolls/local_elections_uttarakhand](https://github.com/in-rolls/local_elections_uttarakhand) — Data on Local Elections from Uttarakhand
- [in-rolls/local_elections_up](https://github.com/in-rolls/local_elections_up) — UP Local Election Data --- GP and ULB. Seat reservation, winner, and candidates for some elections
- [in-rolls/local_elections_bihar](https://github.com/in-rolls/local_elections_bihar) — Bihar panchayat elections: 2016 candidates and votes for six offices; 2021 mukhiya candidates, results, winners and seat reservations
- [in-rolls/local_elections_rajasthan](https://github.com/in-rolls/local_elections_rajasthan) — Rajasthan GP Election Reservation Status and Results for 2020--2022
- [in-rolls/parse_unsearchable_rolls](https://github.com/in-rolls/parse_unsearchable_rolls) — Parse Unsearchable Electoral Rolls

✨ _Powered by [Adjacent](https://github.com/gojiplus/adjacent)_ 🚀

## Maintenance

This is a point-in-time data collection; see the [shared maintenance policy](https://github.com/soodoku/data-repos#maintenance-policy). Run the affected parser tests when code changes and the relevant data validators when inputs or outputs change. Full-data checks and publication are explicit operations. Routine edits do not require hosted CI, Docker, a Python-version matrix, Preen or pre-commit.
