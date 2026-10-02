"""Verify published records and refresh their README inventory."""

import argparse
import json
import re

from local_elections_kerala.parse.to_parquet import DATA, ROOT, export


def data_summary():
    manifest = json.loads((DATA / "fin/MANIFEST.json").read_text())
    lines = [
        "<!-- datasets:start -->",
        "",
        "| File | Rows | Each row represents |",
        "| --- | ---: | --- |",
    ]
    for entry in sorted(manifest["files"], key=lambda e: e["file"]):
        name = entry["file"]
        unit = (
            "2005 ward record with elected member, front and vote text"
            if "2005" in name
            else "Ward listing with member-profile fields"
        )
        lines.append(f"| [fin/{name}](data/fin/{name}) | {entry['rows']:,} | {unit} |")
    lines.extend(["", "<!-- datasets:end -->"])
    path = ROOT / "README.md"
    text = path.read_text()
    pattern = r"<!-- datasets:start -->.*?<!-- datasets:end -->"
    if len(re.findall(pattern, text, flags=re.S)) != 1:
        raise ValueError("README must contain one dataset inventory block")
    path.write_text(re.sub(pattern, lambda _: "\n".join(lines), text, flags=re.S))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["verify", "summary"])
    args = parser.parse_args()
    if args.command == "verify":
        export(DATA, DATA / "fin", check=True)
    else:
        data_summary()


if __name__ == "__main__":
    main()
