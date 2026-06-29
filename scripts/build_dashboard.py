#!/usr/bin/env python3
"""Parameterize the Lakebridge dashboard JSON for a target catalog / schema.

Reads  : <repo_root>/Lakebridge Migration Assessment.lvdash.json
Writes : <repo_root>/dist/Lakebridge Migration Assessment.lvdash.json  (gitignored)

The dist/ file is what databricks.yml references for the dashboard resource.
Run this script before every `databricks bundle deploy`.

Usage
-----
  python scripts/build_dashboard.py --catalog my_catalog
  python scripts/build_dashboard.py --catalog my_catalog --schema my_schema
  python scripts/build_dashboard.py --catalog prod_cat --src-catalog classic_stable_pr2ip7
"""

import argparse
import pathlib
import sys

DASHBOARD_FILE  = "Lakebridge Migration Assessment.lvdash.json"
DEFAULT_CATALOG = "classic_stable_pr2ip7"
DEFAULT_SCHEMA  = "lakebridge_assessment"


def build(catalog: str, schema: str, src_catalog: str, src_schema: str) -> pathlib.Path:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    src_path  = repo_root / DASHBOARD_FILE
    dist_dir  = repo_root / "dist"
    dist_dir.mkdir(exist_ok=True)
    dst_path  = dist_dir / DASHBOARD_FILE

    if not src_path.exists():
        sys.exit(f"ERROR: source dashboard not found: {src_path}")

    raw = src_path.read_text(encoding="utf-8")
    replaced = raw.replace(f"{src_catalog}.{src_schema}", f"{catalog}.{schema}")

    if replaced == raw:
        print(f"WARNING: No occurrences of '{src_catalog}.{src_schema}' found.")
        print(f"         Check --src-catalog / --src-schema if unexpected.")
    else:
        count = raw.count(f"{src_catalog}.{src_schema}")
        print(f"OK  Replaced {count} occurrence(s):")
        print(f"    {src_catalog}.{src_schema}  ->  {catalog}.{schema}")

    dst_path.write_text(replaced, encoding="utf-8")
    print(f"    Written: {dst_path}")
    return dst_path


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--catalog",     required=True,           help="Target UC catalog")
    p.add_argument("--schema",      default=DEFAULT_SCHEMA,  help=f"Target schema (default: {DEFAULT_SCHEMA})")
    p.add_argument("--src-catalog", default=DEFAULT_CATALOG, help=f"Source catalog in template (default: {DEFAULT_CATALOG})")
    p.add_argument("--src-schema",  default=DEFAULT_SCHEMA,  help=f"Source schema in template (default: {DEFAULT_SCHEMA})")
    args = p.parse_args()
    build(args.catalog, args.schema, args.src_catalog, args.src_schema)


if __name__ == "__main__":
    main()
