"""Typed Parquet for the derived SEC tables.

The parsers build rows of text; this writes them with declared types, so a
count is an integer and a yes/no is a boolean in the file. A value that does
not fit its declared type stops the write rather than becoming null.

`read_rows` gives the rows back as text, in the form the parsers wrote them,
so the comparison code that consumes one parser's output in another is
unchanged by the file format.

votes_polled and valid_votes in the 2005 reports stay text: the PDFs' text
layer garbles some of them ("1m0X59n )"), and a number would hide that.
"""

import pyarrow as pa
import pyarrow.parquet as pq

INTEGER = {
    "body_id",
    "body_no",
    "candidate_no",
    "candidate_rows",
    "invalid_votes",
    "listing_source_row",
    "listing_wards",
    "lsg_listing_source_row",
    "old_votes",
    "page",
    "page_line",
    "pdf_body_no",
    "pdf_top_votes",
    "pdf_votes",
    "pdf_wards",
    "source_page",
    "source_row",
    "top_candidate_line",
    "top_candidate_page",
    "top_votes",
    "votes",
    "ward_name_matches",
    "ward_no",
    "ward_number",
}
FLOAT = {"best_score", "body_name_similarity", "runner_up_score", "ward_name_fraction"}
BOOLEAN = {
    "candidate_vote_present",
    "certified_code_present",
    "lsg_listing_present",
    "member_equal",
    "name_normalized_equal",
    "party_equal",
    "party_normalized_equal",
    "pdf_ward_validated",
    "ward_validated",
}
TRUTH = {"True": True, "False": False}


def kind(column):
    if column in INTEGER:
        return pa.int64()
    if column in FLOAT:
        return pa.float64()
    if column in BOOLEAN:
        return pa.bool_()
    return pa.string()


def typed(column, value):
    text = "" if value is None else str(value)
    if text == "":
        return None
    if column in INTEGER:
        return int(text)
    if column in FLOAT:
        return float(text)
    if column in BOOLEAN:
        return TRUTH[text]
    return text


def write_rows(path, rows, columns):
    """Write rows (dicts of text) to Parquet with the declared column types."""
    data = {}
    for column in columns:
        values = []
        for number, row in enumerate(rows, 1):
            try:
                values.append(typed(column, row.get(column)))
            except (KeyError, ValueError) as exc:
                raise ValueError(
                    f"{path}: row {number} {column}={row.get(column)!r}"
                ) from exc
        data[column] = pa.array(values, type=kind(column))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".parquet.part")
    try:
        pq.write_table(pa.table(data), temporary, compression="zstd")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def text(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    return str(value)


def read_rows(path):
    """Rows as dicts of text, the form the parsers wrote them in."""
    table = pq.read_table(path)
    return [
        {key: text(value) for key, value in row.items()} for row in table.to_pylist()
    ]
