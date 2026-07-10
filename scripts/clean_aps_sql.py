#!/usr/bin/env python3
"""Normalize Microsoft APS / Parallel Data Warehouse (PDW) SQL to T-SQL-compliant SQL.

APS/PDW exports (usually `.dsql`) use MPP-only DDL that the T-SQL parser (and
Lakebridge's SqlglotEngine) cannot handle. This module strips exactly those
constructs so the code can be assessed and transpiled:

  * CREATE TABLE storage clause  WITH ( DISTRIBUTION = HASH(col) | REPLICATE |
    ROUND_ROBIN, CLUSTERED [COLUMNSTORE] INDEX(...), HEAP, PARTITION(... RANGE ...) )
    — the DISTRIBUTION keyword itself parses, but its CLUSTERED/COLUMNSTORE/PARTITION
    siblings do not; none of them mean anything in Databricks/Delta, so the whole
    storage clause is removed (balanced-paren aware, also handles CTAS).
  * CREATE STATISTICS ...            (Databricks auto-collects statistics)
  * CREATE [UNIQUE] [NON]CLUSTERED INDEX ...   (no secondary indexes in Delta)
  * varchar(-1) -> varchar(max)      (APS encodes MAX length as -1)

Everything else (columns, COLLATE, procedures, views, DML) is preserved.

Usable two ways:
  * imported by the conversion notebook  (clean_folder / is_pdw_source)
  * standalone CLI:  python scripts/clean_aps_sql.py <src_dir> <dst_dir>
"""
from __future__ import annotations
import re
import sys
import pathlib

# tokens that mark a WITH(...) as a PDW *table storage* clause (vs a CTE / query hint)
_STORAGE_MARKERS = re.compile(r"\b(DISTRIBUTION|HEAP|CLUSTERED|COLUMNSTORE|PARTITION)\b", re.I)
_WITH_OPEN = re.compile(r"\bWITH\b\s*\(", re.I)
_CREATE_STATISTICS = re.compile(r"\bCREATE\s+STATISTICS\b.*?;", re.I | re.S)
_CREATE_INDEX = re.compile(r"\bCREATE\s+(?:UNIQUE\s+)?(?:(?:NON)?CLUSTERED\s+)?INDEX\b.*?;", re.I | re.S)
_VARCHAR_MAX = re.compile(r"\b(n?varchar|n?char|varbinary)\s*\(\s*-1\s*\)", re.I)


def _strip_with_storage(sql: str) -> tuple[str, int]:
    """Remove every `WITH ( ... )` whose body contains a storage marker (balanced parens)."""
    out, i, n, removed = [], 0, len(sql), 0
    while i < n:
        m = _WITH_OPEN.match(sql, i)
        if not m:
            out.append(sql[i]); i += 1; continue
        depth, j = 0, m.end() - 1          # start at the '('
        while j < n:
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        if _STORAGE_MARKERS.search(sql[m.end():j]):
            removed += 1
            i = j + 1                       # drop the whole WITH(...) span
        else:
            out.append(sql[i]); i += 1      # keep (CTE, query hint, etc.)
    return "".join(out), removed


def clean_text(text: str) -> tuple[str, dict]:
    """Return (cleaned_sql, stats) for one script's contents."""
    text, n_with = _strip_with_storage(text)
    text, n_stats = _CREATE_STATISTICS.subn("", text)
    text, n_index = _CREATE_INDEX.subn("", text)
    text, n_vmax = _VARCHAR_MAX.subn(lambda m: f"{m.group(1)}(max)", text)
    # tidy artefacts left by removals
    text = re.sub(r"\)\s*\n\s*;", ");", text)
    text = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", text)
    return text, {"with": n_with, "stats": n_stats, "index": n_index, "vmax": n_vmax}


def is_pdw_source(folder) -> bool:
    """True if the folder looks like an APS/PDW export (.dsql files, or PDW-only DDL)."""
    folder = pathlib.Path(folder)
    if any(folder.rglob("*.dsql")):
        return True
    for f in list(folder.rglob("*.sql"))[:20]:
        head = f.read_text(encoding="utf-8", errors="replace")[:200_000]
        if re.search(r"\bDISTRIBUTION\s*=|\bCREATE\s+STATISTICS\b", head, re.I):
            return True
    return False


def clean_folder(src, dst) -> dict:
    """Clean every .dsql/.sql under src into T-SQL-compliant .sql files under dst (flat).

    Returns an aggregate summary dict: files, with, stats, index, vmax."""
    src, dst = pathlib.Path(src), pathlib.Path(dst)
    dst.mkdir(parents=True, exist_ok=True)
    total = {"files": 0, "with": 0, "stats": 0, "index": 0, "vmax": 0}
    for f in sorted(list(src.rglob("*.dsql")) + list(src.rglob("*.sql"))):
        if not f.is_file():
            continue
        cleaned, stats = clean_text(f.read_text(encoding="utf-8", errors="replace"))
        (dst / (f.stem + ".sql")).write_text(cleaned, encoding="utf-8")
        total["files"] += 1
        for k in ("with", "stats", "index", "vmax"):
            total[k] += stats[k]
    return total


def main(argv=None) -> None:
    import argparse
    p = argparse.ArgumentParser(description="Normalize APS/PDW .dsql to T-SQL-compliant .sql")
    p.add_argument("src", help="Source folder (contains .dsql / .sql)")
    p.add_argument("dst", help="Destination folder for cleaned .sql")
    args = p.parse_args(argv)
    s = clean_folder(args.src, args.dst)
    print(f"Cleaned {s['files']} file(s) -> {args.dst}")
    print(f"  WITH() storage clauses removed : {s['with']}")
    print(f"  CREATE STATISTICS removed      : {s['stats']}")
    print(f"  CREATE INDEX removed           : {s['index']}")
    print(f"  varchar(-1) -> varchar(max)    : {s['vmax']}")


if __name__ == "__main__":
    main()
