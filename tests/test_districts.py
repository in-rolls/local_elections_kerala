"""The district a district panchayat is given by its place in the scrape.

The scrape prints Thiruvananthapuram on every district panchayat, so the
export assigns districts by body order. This checks that order against an
independent source: the SEC's 2020 vote leaders, whose ward codes carry the
district (D01 Thiruvananthapuram ... D14 Kasaragod). Each body's winners must
match its own district's leaders far more often than any other district's.
"""

import re
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

from local_elections_kerala.parse.to_parquet import DISTRICT_ORDER
from local_elections_kerala.tables import read_rows

DATA = Path(__file__).resolve().parents[1] / "data"


def tokens(name):
    words = re.sub(r"[^a-z ]", " ", (name or "").lower()).split()
    return {w for w in words if len(w) >= 4 and w not in {"adv", "advocate"}}


def test_district_panchayat_order_matches_sec_2020_codes():
    rows = [
        r
        for r in pq.read_table(
            DATA / "release/elected_members_2010_2020.parquet"
        ).to_pylist()
        if r["year"] == 2020 and r["lgi_type"] == "District"
    ]
    leaders = {
        r["ward_code"]: r["winner_name_raw"]
        for r in read_rows(DATA / "build/sec/modern/2020_vote_leaders.parquet")
    }
    matches = defaultdict(lambda: [0] * len(DISTRICT_ORDER))
    for row in rows:
        ward = int(float(row["ward_no_raw"]))
        for code in range(len(DISTRICT_ORDER)):
            leader = leaders.get(f"D{code + 1:02d}001{ward:03d}")
            if tokens(row["elected_member_raw"]) & tokens(leader):
                matches[row["district_inferred"]][code] += 1
    assert set(matches) == set(DISTRICT_ORDER)
    for code, district in enumerate(DISTRICT_ORDER):
        own = matches[district][code]
        others = max(n for i, n in enumerate(matches[district]) if i != code)
        assert own >= 10 and own > 5 * others, (district, matches[district])
