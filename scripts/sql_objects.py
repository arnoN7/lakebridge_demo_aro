#!/usr/bin/env python3
"""Split a T-SQL script into individual database objects and score each.

Turns bulk DDL/DML dumps into one record per object (CREATE TABLE / VIEW /
PROCEDURE / FUNCTION …) so a migration assessment reports real per-object
complexity and transpilability instead of coarse per-file numbers.

Segmentation is GO-aware and keeps routines (procedures/functions/triggers)
whole — their bodies contain internal ';' that must not be split on.

Each object gets:
  * object_id   — stable unique key (schema.name, deduped)
  * object_type — CREATE TABLE / CREATE VIEW / CREATE PROCEDURE / ...
  * nodes       — sqlglot AST node count (size proxy); falls back to line count
  * complexity  — LOW / MEDIUM / HIGH from `nodes`
  * source      — the object's SQL text (for transpilation by the caller)

Transpilation itself is left to the caller (the notebook uses Lakebridge's
SqlglotEngine so the metric matches the rest of the pipeline).
"""
from __future__ import annotations
import re
import logging
import pathlib

logging.getLogger("sqlglot").setLevel(logging.CRITICAL)
import sqlglot
from sqlglot.tokens import Tokenizer, TokenType

_ROUTINE = re.compile(r"(?is)\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROC|PROCEDURE|FUNCTION|TRIGGER)\b")
_GO = re.compile(r"(?im)^[ \t]*GO[ \t]*;?[ \t]*$")
# complexity thresholds on AST node count (calibrated on the APS corpus)
_LOW_MAX, _MED_MAX = 60, 400


def _no_comments(s: str) -> str:
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)
    return re.sub(r"--[^\n]*", "", s)


def _split_semicolons(sql: str):
    try:
        toks = Tokenizer().tokenize(sql)
    except Exception:
        return [s for s in sql.split(";") if s.strip()]
    out, start = [], 0
    for t in toks:
        if t.token_type == TokenType.SEMICOLON:
            if sql[start:t.start].strip():
                out.append(sql[start:t.start])
            start = t.end + 1
    if sql[start:].strip():
        out.append(sql[start:])
    return out


def split_statements(sql: str):
    """GO-aware statement split; routine-defining batches are kept whole."""
    stmts = []
    for batch in _GO.split(sql):
        if not batch.strip():
            continue
        if _ROUTINE.search(_no_comments(batch)):
            stmts.append(batch)
        else:
            stmts.extend(_split_semicolons(batch))
    return stmts


def classify(stmt: str) -> str:
    s = re.sub(r"^\s*(?:--[^\n]*\n|/\*.*?\*/\s*)+", "", stmt, flags=re.S).lstrip()
    h = s[:60].upper()
    if h.startswith("CREATE"):
        for kw, name in (("TABLE", "CREATE TABLE"), ("VIEW", "CREATE VIEW"),
                         ("PROC", "CREATE PROCEDURE"), ("FUNCTION", "CREATE FUNCTION"),
                         ("SCHEMA", "CREATE SCHEMA"), ("TRIGGER", "CREATE TRIGGER")):
            if kw in h:
                return name
        return "CREATE (other)"
    for kw in ("ALTER", "DROP", "TRUNCATE", "INSERT", "UPDATE", "DELETE", "MERGE"):
        if h.startswith(kw):
            return kw
    if h.startswith(("SELECT", "WITH")):
        return "SELECT"
    return (h.split() or ["OTHER"])[0]


_NAME = re.compile(
    r"(?is)\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROC(?:EDURE)?|FUNCTION|VIEW|TABLE|TRIGGER|SCHEMA)\s+"
    r"(\[?[\w#]+\]?(?:\.\[?[\w#]+\]?)?)"
)

def object_name(stmt: str) -> str | None:
    m = _NAME.search(_no_comments(stmt))
    if not m:
        return None
    return re.sub(r"[\[\]]", "", m.group(1))


def node_count(stmt: str) -> int:
    try:
        exprs = sqlglot.parse(stmt, read="tsql")
        return sum(len(list(e.walk())) for e in exprs if e) or stmt.count("\n") + 1
    except Exception:
        return stmt.count("\n") + 1


def complexity(nodes: int) -> str:
    return "LOW" if nodes <= _LOW_MAX else "MEDIUM" if nodes <= _MED_MAX else "HIGH"


def iter_objects(path):
    """Yield one dict per object in a .sql file. object_id is unique within the run."""
    text = pathlib.Path(path).read_text(encoding="utf-8", errors="replace")
    for stmt in split_statements(text):
        if not _no_comments(stmt).strip():
            continue
        otype = classify(stmt)
        if otype in ("USE", "SET", "PRINT", "GO", "OTHER", "DECLARE", "IF", "BEGIN", "END"):
            continue  # not a migratable object
        name = object_name(stmt) or f"{pathlib.Path(path).stem}"
        n = node_count(stmt)
        yield {
            "name": name,
            "object_type": otype,
            "nodes": n,
            "complexity": complexity(n),
            "source": stmt.strip(),
            "source_file": pathlib.Path(path).name,
        }


def collect(folder):
    """All objects under a folder, with object_id made unique (suffix on collisions)."""
    seen, out = {}, []
    for f in sorted(pathlib.Path(folder).rglob("*.sql")):
        for o in iter_objects(f):
            base = f"{o['object_type'].split()[-1].lower()}:{o['name']}"
            seen[base] = seen.get(base, 0) + 1
            o["object_id"] = base if seen[base] == 1 else f"{base}#{seen[base]}"
            out.append(o)
    return out
