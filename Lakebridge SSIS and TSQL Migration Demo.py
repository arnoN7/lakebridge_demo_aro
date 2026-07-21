# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "databricks-labs-lakebridge",
#   "openpyxl",
# ]
# ///
# DBTITLE 1,Install Lakebridge
# MAGIC %pip install databricks-labs-lakebridge openpyxl databricks-bb-plugin "numpy<2"
# MAGIC # numpy<2 pin: Lakebridge deps otherwise pull NumPy 2.x, which is ABI-incompatible
# MAGIC # with DBR 15.4's pre-compiled pandas/pyarrow and crashes the Python kernel on restart.

# COMMAND ----------

# DBTITLE 1,Restart Python
# Restart the Python interpreter so the freshly pip-installed packages are importable.
# Required: the cluster ships databricks-sdk in system site-packages, which otherwise
# shadows the `databricks` namespace and hides databricks.labs.* until a restart.
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Demo Overview
# MAGIC %md
# MAGIC ## Lakebridge Demo: Folder-Driven SSIS & T-SQL Migration
# MAGIC
# MAGIC This notebook reads source assets from `sample_assets/` (or a UC Volume set via `input_path`)
# MAGIC and writes converted outputs to a UC Volume: `/Volumes/{catalog}/{schema}/assessment_output/`.
# MAGIC
# MAGIC **Everything runs inside this cluster** — Lakebridge and the BladeBridge transpiler are
# MAGIC installed via `%pip` (cell 1). Nothing is installed on your laptop, so this works even when
# MAGIC local installation of the Lakebridge CLI / native binaries is blocked by security policy.
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. **Assessment** — `bladespector` scans the input folder → Excel reports → UC tables (`job_details`, `transformations`, `functions`, `sql_statements`)
# MAGIC 2. **T-SQL Conversion** (Phase 2a) — `SqlglotEngine` transpiles `.sql` files deterministically → `conversion_results`
# MAGIC 3. **SSIS Conversion** (Phase 2b) — the **BladeBridge** transpiler (`databricks-bb-plugin`) converts each `.dtsx` package to a Databricks notebook → `conversion_results`. It translates **Execute SQL Tasks** and **Data Flow** pipelines (OLE DB Source → transforms → OLE DB Destination) into chained Spark SQL temp views + `INSERT … SELECT`. Tasks the [Lakebridge docs](https://databrickslabs.github.io/lakebridge/docs/transpile/source_systems/ssis/supported_components/) list as **unsupported** (Send Mail, FTP, Web Service, Message Queue, XML, WMI, Bulk Insert, Data Profiling, Export/Import Column) are detected and the package is flagged `transpiled = false` for manual review. **Note:** conversion requires well-formed SSIS XML — packages must be real SSDT exports, not simplified XML
# MAGIC 4. **Metric Views** (Phase 3) — create two Unity Catalog metric views (`assessment_metrics`, `effort_metrics`) that back the **Lakebridge Migration Assessment** dashboard
# MAGIC
# MAGIC **Data model** (all in `{catalog}.{schema}`, default `classic_stable_pr2ip7.lakebridge_assessment`):
# MAGIC
# MAGIC | Layer | Object | Grain |
# MAGIC |---|---|---|
# MAGIC | Assessment | `job_details` | 1 row per source file |
# MAGIC | Assessment | `transformations` | 1 row per SSIS component type |
# MAGIC | Assessment | `functions` | 1 row per T-SQL function |
# MAGIC | Assessment | `sql_statements` | 1 row per SQL embedded in SSIS |
# MAGIC | Conversion | `conversion_results` | 1 row per file (engine + transpilability) |
# MAGIC | Planning | `effort_hypothesis` | 1 row per rate card entry |
# MAGIC | Planning | `overhead_hypothesis` | 1 row per overhead profile |
# MAGIC | Metric View | `assessment_metrics` | Inventory × transpilability (job_details ⋈ conversion_results) |
# MAGIC | Metric View | `effort_metrics` | Per-object effort estimate incl. overhead |
# MAGIC
# MAGIC > **Compute note:** Phase 2b's BladeBridge binary is a native Linux executable, so run this
# MAGIC > notebook on a **classic job cluster** (the bundled DAB job already uses one). The rest runs on Serverless too.
# MAGIC
# MAGIC To analyse real code instead of the samples, set the `input_path` variable in
# MAGIC **databricks.yml** to the folder/Volume holding your `.sql` / `.dtsx` files.

# COMMAND ----------

# DBTITLE 1,Setup — paths and engine
# -- Job / bundle parameters --------------------------------------------------
# These are driven by the bundle variables in databricks.yml (catalog, schema,
# input_path) and passed in as job parameters. Edit them in databricks.yml — not here.
dbutils.widgets.text("catalog", "classic_stable_pr2ip7", "UC Catalog")
dbutils.widgets.text("schema",  "lakebridge_assessment",  "UC Schema")
dbutils.widgets.text("input_path", "", "Input path (blank = bundled sample_assets)")
dbutils.widgets.text("use_switch", "false", "Use Switch LLM converter (true/false)")
dbutils.widgets.text("switch_model", "databricks-claude-sonnet-5", "Switch foundation model")
UC_CAT    = dbutils.widgets.get("catalog")
UC_SCHEMA = dbutils.widgets.get("schema")

from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────────
# Repo root is derived from THIS notebook's location, so the demo works for any
# user / workspace with no edits — whether opened from a Git folder or run by the
# bundled DAB job. (Falls back to CWD if the notebook context is unavailable.)
try:
    _nb_path = (dbutils.notebook.entry_point.getDbutils()
                .notebook().getContext().notebookPath().get())
    REPO_ROOT = Path("/Workspace") / Path(_nb_path.lstrip("/")).parent
except Exception:
    REPO_ROOT = Path.cwd()

# Input source:
#   • input_path (from databricks.yml) set  → real code, e.g. a UC Volume
#       /Volumes/<catalog>/<schema>/landing  that you drop .sql / .dtsx into
#   • input_path blank                       → the version-controlled sample_assets/
_input_path = dbutils.widgets.get("input_path").strip()
input_root  = Path(_input_path) if _input_path else (REPO_ROOT / "sample_assets")

# Outputs go to a UC Volume (never the repo / workspace files), so results live with
# the data and survive redeploys. A dedicated `assessment_output` volume is created
# under the target schema.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {UC_CAT}.{UC_SCHEMA}.assessment_output")
_out_base   = Path(f"/Volumes/{UC_CAT}/{UC_SCHEMA}/assessment_output")
output_root = _out_base / "converted"    # T-SQL conversions (Phase 2a)
ssis_output = _out_base / "ssis_sdp"      # SSIS conversions  (Phase 2b, BladeBridge)
output_root.mkdir(parents=True, exist_ok=True)
ssis_output.mkdir(parents=True, exist_ok=True)

engine = SqlglotEngine()

print(f"Supported source dialects: {engine.supported_dialects}\n")
print(f"Repo root:         {REPO_ROOT}")
_n_src = len(list(input_root.rglob("*.sql")) + list(input_root.rglob("*.dsql")) + list(input_root.rglob("*.dtsx")))
print(f"Input folder:      {input_root}  ({_n_src} .sql/.dsql/.dtsx files, incl. subfolders)")
print(f"T-SQL output:      {output_root}")
print(f"SSIS output:       {ssis_output}")

# COMMAND ----------

# DBTITLE 1,Phase 0a — Normalize APS/PDW source to T-SQL (in-pipeline)
# Microsoft APS / Parallel Data Warehouse exports (.dsql) use MPP-only DDL the T-SQL
# parser rejects: WITH (DISTRIBUTION=…, CLUSTERED [COLUMNSTORE] INDEX…, HEAP,
# PARTITION…), CREATE STATISTICS, CREATE INDEX, varchar(-1). scripts/clean_aps_sql.py
# rewrites the source into T-SQL-compliant .sql before assessment/transpilation.
# No-op for source without APS/PDW constructs (e.g. the bundled sample_assets).
import sys as _sys
_sys.path.insert(0, str(REPO_ROOT / "scripts"))
import clean_aps_sql

if clean_aps_sql.is_pdw_source(input_root):
    # Use a guaranteed-writable temp dir — /local_disk0 is not writable on serverless
    # or UC shared-access clusters. tempfile picks the right local FS on any compute.
    import tempfile as _tempfile
    cleaned_root = Path(_tempfile.mkdtemp(prefix="aps_cleaned_"))
    s = clean_aps_sql.clean_folder(input_root, cleaned_root)
    print(f"APS/PDW source detected — normalized {s['files']} file(s) to T-SQL:")
    print(f"  WITH() storage clauses removed : {s['with']}")
    print(f"  CREATE STATISTICS removed      : {s['stats']}")
    print(f"  CREATE INDEX removed           : {s['index']}")
    print(f"  varchar(-1) -> varchar(max)    : {s['vmax']}")
    input_root = cleaned_root      # all later phases read the cleaned source
    print(f"  Downstream phases will read cleaned source from: {input_root}")
else:
    print("No APS/PDW constructs detected — using source as-is.")

# COMMAND ----------

# DBTITLE 1,Phase 0 — Lakebridge Assessment
from databricks.labs.bladespector.analyzer import Analyzer

# The Lakebridge analyzer (bladespector) scans the source folder and produces
# an Excel + optional JSON report covering complexity, compatibility and effort.
# It runs independently of transpilation — use it first to scope the migration.

print(f"Supported source technologies: {Analyzer.supported_source_technologies()}\n")

# The assessment .xlsx is an intermediate artefact written by the bladespector native
# binary — keep it on local disk (reliable for the binary), not the repo. Final outputs
# (converted code) go to the Volume.
import tempfile as _tf
assessment_root = Path(_tf.mkdtemp(prefix="aps_assessment_"))
assessment_root.mkdir(parents=True, exist_ok=True)

# Folder-driven: only analyze technologies actually present. bladespector's
# `-t SSIS` pass exits non-zero (status 2) on a folder with no .dtsx packages,
# so we add SSIS only when SSIS files exist (e.g. a T-SQL-only client scope).
platforms = {
    "MS SQL Server": assessment_root / "tsql_assessment.xlsx",
}
if any(input_root.rglob("*.dtsx")):
    platforms["SSIS"] = assessment_root / "ssis_assessment.xlsx"

for platform, report_path in platforms.items():
    json_path = report_path.with_suffix(".json")
    print(f"Running analyzer for: {platform}")
    # json_result is optional — omit it; the JSON schema validation in the current
    # bladespector version rejects the binary output and raises RuntimeError.
    Analyzer.analyze(input_root, report_path, platform)
    print(f"  Excel report : {report_path}")

    # Display each sheet inline as a table
    try:
        import openpyxl
        xl = pd.ExcelFile(report_path, engine="openpyxl")
        print(f"  Sheets: {xl.sheet_names}")
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            if not df.empty:
                print(f"\n  ── {platform} / {sheet} ──")
                display(df)
    except Exception as e:
        print(f"  (Could not display inline: {e})")
    print()

# COMMAND ----------

# DBTITLE 1,_explore_excel_tmp
import re

# Catalog/schema set in Setup cell via dbutils.widgets — read here.
UC_CAT    = dbutils.widgets.get("catalog")
UC_SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CAT}.{UC_SCHEMA}")

def _col(c: str) -> str:
    return re.sub(r'\W+', '_', str(c).strip().lower()).strip('_')

def read_sheet(xlsx_path: Path, sheet: str) -> pd.DataFrame:
    # Tolerate a technology that wasn't analyzed (e.g. SSIS absent in a T-SQL-only run).
    if not xlsx_path or not Path(xlsx_path).exists():
        return pd.DataFrame()
    xl = pd.ExcelFile(xlsx_path, engine='openpyxl')
    if sheet not in xl.sheet_names:
        return pd.DataFrame()
    df = xl.parse(sheet, header=0)
    df.columns = [_col(c) for c in df.columns]
    return df.dropna(how='all').reset_index(drop=True)

def to_uc(df: pd.DataFrame, table: str) -> None:
    if df.empty:
        print(f"  [skip] {table} — empty")
        return
    spark.createDataFrame(df).write.format('delta').mode('overwrite') \
         .option('overwriteSchema', 'true') \
         .saveAsTable(f'{UC_CAT}.{UC_SCHEMA}.{table}')
    print(f"  Wrote {len(df):>4} rows  →  {UC_CAT}.{UC_SCHEMA}.{table}")
    display(df)

# ── job_details: one row per assessed file, both platforms ────────────────────
frames = []
for platform_label, report_path in platforms.items():
    df = read_sheet(report_path, 'Job Details')
    if df.empty:
        continue
    df = df[list(df.columns[:7])].copy()
    df.columns = ['file_name', 'folder', 'source_file', 'included',
                  'job_type', 'categorization', 'number_of_nodes']
    df['platform']        = platform_label
    df['file_name']       = df['file_name'].astype(str).str.split('/').str[-1]
    # Normalize: ensure SSIS file_name includes .dtsx extension for join consistency
    if platform_label == 'SSIS':
        df['file_name'] = df['file_name'].apply(lambda n: n if n.endswith('.dtsx') else f"{n}.dtsx")
    df['number_of_nodes'] = pd.to_numeric(df['number_of_nodes'], errors='coerce').fillna(0).astype(int)
    frames.append(df[['platform', 'file_name', 'job_type',
                       'categorization', 'number_of_nodes', 'included']])

print("job_details")
if frames:
    to_uc(pd.concat(frames, ignore_index=True), 'job_details')
else:
    # No analyzer Job Details rows (e.g. a T-SQL-only scope with no SSIS). job_details
    # is seeded entirely from the .sql files in the append step below; drop any prior
    # table first so re-runs stay idempotent.
    print("  [info] no analyzer Job Details rows — seeding job_details from .sql files below")
    spark.sql(f"DROP TABLE IF EXISTS {UC_CAT}.{UC_SCHEMA}.job_details")

# ── functions: T-SQL function call counts ────────────────────────────────
fn_df = read_sheet(platforms['MS SQL Server'], 'Functions')
if not fn_df.empty:
    fn_df = fn_df[list(fn_df.columns[:2])].copy()
    fn_df.columns = ['function_name', 'call_count']
    fn_df = fn_df.dropna(subset=['function_name'])
    fn_df['call_count'] = pd.to_numeric(fn_df['call_count'], errors='coerce').fillna(0).astype(int)
    fn_df['platform'] = 'MS SQL Server'
# ── standalone SQL → one job_details row PER OBJECT (table / view / proc / …) ─────
# Bulk .sql dumps hold hundreds of objects each; a per-file view would be meaningless
# ("18 files, all LOW, 0% transpilable"). scripts/sql_objects.py splits every file
# into individual objects with real per-object complexity (AST size). The resulting
# `sql_objects_list` is reused by Phase 2a for per-object transpilation.
_sys.path.insert(0, str(REPO_ROOT / "scripts"))
import sql_objects
sql_objects_list = sql_objects.collect(input_root)
if sql_objects_list:
    obj_rows = [{
        "platform": "MS SQL Server",
        "file_name": o["object_id"],          # unique key; joins conversion_results
        "job_type": o["object_type"],
        "categorization": o["complexity"],
        "number_of_nodes": int(o["nodes"]),
        "included": "YES",
    } for o in sql_objects_list]
    spark.createDataFrame(pd.DataFrame(obj_rows)).write.format("delta").mode("append") \
         .saveAsTable(f"{UC_CAT}.{UC_SCHEMA}.job_details")
    print(f"\n  Appended {len(obj_rows)} SQL object rows → {UC_CAT}.{UC_SCHEMA}.job_details")
    display(pd.DataFrame(obj_rows).groupby(['job_type', 'categorization']).size()
            .reset_index(name='objects'))

print("\nfunctions")
to_uc(fn_df if not fn_df.empty else pd.DataFrame(columns=['function_name', 'call_count', 'platform']), 'functions')

# ── transformations: SSIS component types ──────────────────────────────
trans_df = read_sheet(platforms.get('SSIS'), 'Transformations')
if not trans_df.empty:
    trans_df = trans_df[list(trans_df.columns[:5])].copy()
    trans_df.columns = ['transformation_type', 'occurrences', 'jobs_count', 'supported', 'component_level']
    trans_df['occurrences'] = pd.to_numeric(trans_df['occurrences'], errors='coerce').fillna(0).astype(int)
    trans_df['jobs_count']  = pd.to_numeric(trans_df['jobs_count'],  errors='coerce').fillna(0).astype(int)
print("\ntransformations")
to_uc(trans_df, 'transformations')

# ── sql_statements: SQL embedded inside SSIS packages ─────────────────────
sql_df = read_sheet(platforms.get('SSIS'), 'SQL Statements')
if not sql_df.empty:
    sql_df = sql_df[list(sql_df.columns[:6])].copy()
    sql_df.columns = ['package_name', 'node', 'complexity', 'connection_type', 'length', 'sql_text']
    sql_df['length'] = pd.to_numeric(sql_df['length'], errors='coerce').fillna(0).astype(int)
print("\nsql_statements")
to_uc(sql_df, 'sql_statements')

# COMMAND ----------

# DBTITLE 1,Phase 2a — SQL Analysis and Conversion
conversion_rows, converted_code = [], {}

# One transpile attempt PER OBJECT (from sql_objects_list built in Phase 1), so
# transpilability is reported per table/view/procedure — not "whole file failed
# because one statement did". Keyed by object_id to join job_details.
for o in sql_objects_list:
    oid = o["object_id"]
    try:
        result = await engine.transpile(
            source_dialect="tsql", target_dialect="databricks",
            source_code=o["source"], file_path=Path(o["source_file"]),
        )
        errors = result.error_list
        converted_code[oid] = result.transpiled_code
        conversion_rows.append({
            "file_name":      oid,
            "file_type":      o["object_type"],
            "engine":         "sqlglot",
            "model":          None,
            "success_count":  result.success_count,
            "error_count":    len(errors),
            "transpiled":     len(errors) == 0,
            "failure_reason": "; ".join(str(e) for e in errors) if errors else None,
            "output_file":    None,
        })
    except Exception as e:
        msg = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
        conversion_rows.append({
            "file_name": oid, "file_type": o["object_type"], "engine": "sqlglot",
            "model": None, "success_count": 0, "error_count": 1, "transpiled": False,
            "failure_reason": msg, "output_file": None,
        })

_ok = sum(r["transpiled"] for r in conversion_rows)
print(f"  Transpiled {len(conversion_rows)} objects — {_ok} fully transpilable "
      f"({_ok*100//max(len(conversion_rows),1)}%)")

# Persist converted Databricks SQL to the Volume, one file per source file.
_by_file = {}
for o in sql_objects_list:
    code = converted_code.get(o["object_id"])
    if code:
        _by_file.setdefault(o["source_file"], []).append(code)
for src_file, blocks in _by_file.items():
    (output_root / f"converted_{Path(src_file).stem}.sql").write_text(
        "\n\n;\n\n".join(blocks), encoding="utf-8")
print(f"  Wrote {len(_by_file)} converted SQL file(s) → {output_root}")

# Note: SSIS package rows are written to conversion_results by Phase 2b (BladeBridge),
# with their real transpilation outcomes — not pre-registered here.

for row in conversion_rows:
    row.setdefault("transpilation_scope", "Object")
    row.setdefault("failure_reason", None)

conversion_df = pd.DataFrame(conversion_rows)
display(conversion_df)

# ── Persist to UC — Phase 2a fully REPLACES conversion_results with the current
# per-object SQL rows, so rows from a prior run keyed differently (e.g. old
# file-level rows) never linger. Phase 2b (SSIS) MERGEs its package rows in after.
target_table = f"{UC_CAT}.{UC_SCHEMA}.conversion_results"

# ── Define the table schema in DDL FIRST, then write rows into it. ─────────────
# CRITICAL: `model` / `output_file` are frequently all-NULL (a T-SQL-only project
# with no Switch/LLM run). If we instead let `spark.createDataFrame(pandas_df)`
# INFER the schema, an all-None column becomes VOID (NullType), which Delta persists
# as a `void` column that is never materialised to Parquet. Downstream reads then
# fail — the SQL warehouse with [INTERNAL_ERROR] Couldn't find model#… , and the
# notebook Parquet reader with "Cannot find column index for attribute 'model'"
# (metadata says 10 cols, the files hold 8). Declaring the schema in DDL makes the
# table authoritative and inference-free; `CREATE OR REPLACE TABLE` atomically
# rebuilds it every run, so any pre-existing void-schema table is fully replaced —
# no drop/overwriteSchema dance, and the empty and non-empty cases share one schema.
spark.sql(f"""
    CREATE OR REPLACE TABLE {target_table} (
        file_name           STRING,
        file_type           STRING,
        engine              STRING,
        model               STRING,
        success_count       BIGINT,
        error_count         BIGINT,
        transpiled          BOOLEAN,
        failure_reason      STRING,
        transpilation_scope STRING,
        output_file         STRING
    ) USING DELTA
""")

if conversion_rows:
    # Build the DataFrame FROM THE TABLE'S OWN SCHEMA (the DDL above is the single
    # source of truth) so no column type is ever inferred — an all-None `model`
    # stays STRING, not void. Then append the rows into the freshly created table.
    _schema = spark.table(target_table).schema
    _names  = [f.name for f in _schema.fields]
    _rows   = [tuple(r.get(n) for n in _names) for r in conversion_rows]
    (spark.createDataFrame(_rows, _schema)
          .write.format("delta").mode("append").saveAsTable(target_table))
    print(f"✓ {len(conversion_rows)} rows written → {target_table}")

print(f"✓ {len(conversion_rows)} rows upserted → {target_table}")
display(spark.table(target_table).orderBy("file_name"))

# Show source → converted diff for the first object
_src_by_id = {o["object_id"]: o["source"] for o in sql_objects_list}
first = list(converted_code)[0]
print("=" * 80)
print(f"SOURCE object: {first}")
print("=" * 80)
print(_src_by_id.get(first, "(source unavailable)")[:2000])
print("\n" + "-" * 80)
print("CONVERTED TO DATABRICKS SQL")
print("-" * 80)
print(converted_code[first][:2000])

# COMMAND ----------

# DBTITLE 1,Phase 2b — SSIS Conversion (BladeBridge)
# ── Phase 2b: SSIS → Databricks via BladeBridge ────────────────────────────────
#
# BladeBridge converts .dtsx packages to a Databricks notebook (Spark SQL / PySpark)
# using a native `dbxconv` binary that ships INSIDE the databricks-bb-plugin wheel —
# installed in cell 1, runs in THIS cluster, nothing installed locally.
#
# SCOPE: BladeBridge converts Execute SQL Tasks and Data Flow pipelines (OLE DB Source,
# OLE DB Destination, derived expressions, …) into chained Spark SQL temp views + INSERTs.
# Tasks the Lakebridge docs list as unsupported (Send Mail, FTP, Web Service, XML, …) are
# detected via UNSUPPORTED_TYPES below. We flag a package transpiled=false when it contains
# one of those, emits no Spark SQL, or the converter raises a diagnostic — so the dashboard
# reports an honest transpilability rate. (Requires well-formed SSDT XML.)
# No license key is required — the binary runs as-is. (The Transpiler computes a
# converter_key.txt path in __init__ but never passes it to the binary; gating on
# that file's existence is what made an earlier version of this cell refuse to run.)
# Docs: https://databrickslabs.github.io/lakebridge/docs/transpile/source_systems/ssis/
#
# Two further gotchas this cell handles:
#   1. source_tech must be UPPERCASE "SSIS" — the config mapping keys are uppercase,
#      so "ssis" raises ValueError: No mapping for source tech ssis.
#   2. transpile() returns the generated file(s) packed as a MIME multipart blob in
#      edits[0].new_text. Decode it with the email module to recover real files —
#      writing new_text directly would dump raw MIME headers.

import email, os, re, stat
import pandas as pd
from pathlib import Path
from databricks.labs.bladebridge.transpiler import Transpiler

dtsx_files = sorted(input_root.rglob("*.dtsx"))   # rglob → also picks up subfolders
print("Phase 2b — SSIS Conversion (BladeBridge)")
print("=" * 80)
bb_version = __import__("importlib.metadata", fromlist=["metadata"]).metadata("databricks-bb-plugin")["Version"]
print(f"  Package: databricks-bb-plugin v{bb_version}")
print(f"  Input:   {input_root} ({len(dtsx_files)} .dtsx files)")
print(f"  Output:  {ssis_output}\n")

# Bladebridge resolves the right native binary per-platform (Linux on Databricks).
transpiler = Transpiler(source_tech="SSIS", target_tech="SPARKSQL")  # NOTE: uppercase

# Wheels installed to NFS sometimes lose the +x bit — restore it if needed.
binary_path = transpiler._locate_binary()
if not os.access(binary_path, os.X_OK):
    binary_path.chmod(binary_path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    print(f"  Set executable bit on {binary_path.name}\n")


def write_mime_outputs(blob: str, dest_dir: Path, default_stem: str) -> list:
    """Bladebridge packs generated files as a MIME multipart message — decode and write each."""
    msg = email.message_from_string(blob)
    parts = list(msg.walk()) if msg.is_multipart() else [msg]
    written = []
    for part in parts:
        if part.get_content_maintype() == "multipart":
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        # The attachment filename is a temp path — keep only the basename.
        name = Path(part.get_filename() or f"{default_stem}.py").name
        out_file = dest_dir / name
        out_file.write_bytes(payload)
        written.append(out_file)
    return written


# Tasks/components the Lakebridge SSIS converter lists as UNSUPPORTED — "require manual
# conversion". Source: the official supported-components doc
# https://databrickslabs.github.io/lakebridge/docs/transpile/source_systems/ssis/supported_components/
# (Loops, Sequence Container and Script Task ARE supported, so they are NOT listed here.)
# When present, the rest of the package still converts; we flag it for manual review.
UNSUPPORTED_TYPES = {
    # Control flow tasks
    "Microsoft.SendMailTask", "Microsoft.FtpTask", "Microsoft.MessageQueueTask",
    "Microsoft.WebServiceTask", "Microsoft.WmiDataReaderTask", "Microsoft.WmiEventWatcherTask",
    "Microsoft.XMLTask", "Microsoft.BulkInsertTask", "Microsoft.ExecuteDDLTask",
    "Microsoft.AnalysisServicesProcessingTask", "Microsoft.DataProfilingTask",
    # Data flow components
    "Microsoft.ExportColumn", "Microsoft.ImportColumn",
}

def unsupported_components(xml_text: str) -> list:
    found = re.findall(r'(?:componentClassID|ExecutableType)="([^"]+)"', xml_text)
    return sorted({t.replace("Microsoft.", "") for t in found if t in UNSUPPORTED_TYPES})

ssis_results = []
for dtsx_path in dtsx_files:
    source_code = dtsx_path.read_text(encoding="utf-8")
    edits, diagnostics = await transpiler.transpile(dtsx_path.name, source_code)
    # On success transpile() returns (edits, []); a crash/parse failure → ([], [diagnostic]).
    out_files = write_mime_outputs(edits[0].new_text, ssis_output, dtsx_path.stem) if edits else []
    # Honest flag: transpiled only if Spark SQL was emitted, the converter raised no
    # diagnostic, AND the package contains no component type BladeBridge can't handle.
    sql_cells   = sum(f.read_text(encoding="utf-8").count("spark.sql(") for f in out_files)
    unsupported = unsupported_components(source_code)
    transpiled  = bool(out_files) and not diagnostics and sql_cells > 0 and not unsupported
    reasons = [str(d.message).splitlines()[0][:200] for d in diagnostics]
    if unsupported:
        reasons.append("Components requiring manual rewrite (not converted): "
                       + ", ".join(unsupported))
    if not transpiled and not reasons:
        reasons.append("No Spark SQL emitted — review manually")
    scope = (f"Converted — {sql_cells} Spark SQL cell(s)" if transpiled
             else f"Partial — {sql_cells} cell(s) converted; manual review required"
                  if sql_cells else "Not converted — manual review required")
    for f in out_files:
        print(f"  {'OK  ' if transpiled else 'WARN'} {dtsx_path.name} -> {f.name} "
              f"({f.stat().st_size:,} bytes, {sql_cells} SQL cells)"
              + (f"  ⚠ unsupported: {', '.join(unsupported)}" if unsupported else ""))
    for d in diagnostics:
        print(f"  ERR {dtsx_path.name}: {str(d.message).splitlines()[0][:160]}")
    ssis_results.append({
        "file_name": dtsx_path.name,
        "transpiled": transpiled,
        "output_files": ", ".join(f.name for f in out_files) or None,
        "sql_cells_converted": sql_cells,
        "diagnostics": len(diagnostics),
        "transpilation_scope": scope,
        "failure_reason": "; ".join(reasons) or None,
    })

print(f"\n  Output dir: {ssis_output}")
if ssis_results:
    display(pd.DataFrame(ssis_results))
else:
    print("  No SSIS packages to convert (no .dtsx in input) — skipping SSIS results table.")

# ── Write real SSIS outcomes to conversion_results (MERGE — idempotent) ────────
# One row per package, mirroring the SQL rows from Phase 2a. The dashboard's
# assessment_metrics / effort_metrics views read transpilability from here.
target_table = f"{UC_CAT}.{UC_SCHEMA}.conversion_results"
ssis_rows = [{
    "file_name":           r["file_name"],
    "file_type":           "SSIS Package",
    "engine":              "bladebridge",
    "model":               None,
    "success_count":       r["sql_cells_converted"],
    "error_count":         r["diagnostics"],
    "transpiled":          bool(r["transpiled"]),
    "failure_reason":      r["failure_reason"],
    "transpilation_scope": r["transpilation_scope"],
    "output_file":         r["output_files"],
} for r in ssis_results]

if ssis_rows:
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   BooleanType, LongType)
    _cr_schema = StructType([
        StructField("file_name", StringType()),
        StructField("file_type", StringType()),
        StructField("engine", StringType()),
        StructField("model", StringType()),
        StructField("success_count", LongType()),
        StructField("error_count", LongType()),
        StructField("transpiled", BooleanType()),
        StructField("failure_reason", StringType()),
        StructField("transpilation_scope", StringType()),
        StructField("output_file", StringType()),
    ])
    _rows = [(r["file_name"], r["file_type"], r["engine"], r["model"],
             int(r["success_count"]), int(r["error_count"]), bool(r["transpiled"]),
             r["failure_reason"], r["transpilation_scope"], r["output_file"])
            for r in ssis_rows]
    spark.createDataFrame(_rows, _cr_schema).createOrReplaceTempView("ssis_conversion_updates")
    spark.sql(f"""
        MERGE INTO {target_table} AS t
        USING ssis_conversion_updates AS s
        ON t.file_name = s.file_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"\n✓ {len(ssis_rows)} SSIS package rows upserted → {target_table}")
    display(spark.table(target_table).orderBy("file_name"))

# COMMAND ----------

# DBTITLE 1,Using the Switch LLM converter (auto-installed by this notebook)
# MAGIC %md
# MAGIC ### Using the Switch LLM converter (`use_switch=true`)
# MAGIC Switch deploys a Databricks job named **`Lakebridge_Switch`** that this notebook triggers.
# MAGIC **No local install is required** — Switch ships with Lakebridge (pip-installed in cell 1),
# MAGIC so **Phase 2c deploys `Lakebridge_Switch` from this notebook automatically** the first time
# MAGIC you run with `use_switch=true` (via `SwitchDeployment`, serverless). Everything runs in the
# MAGIC workspace; nothing is installed on a laptop.
# MAGIC
# MAGIC **Optional** — you can instead pre-install Switch from the Databricks CLI:
# MAGIC ```bash
# MAGIC databricks labs install lakebridge
# MAGIC databricks labs lakebridge install-transpile   # prompts for dialect / model / catalog-schema
# MAGIC ```
# MAGIC Either way, once `Lakebridge_Switch` exists, `use_switch=true` extracts the failing
# MAGIC procedures to the Volume and triggers Switch on just those.

# COMMAND ----------

# DBTITLE 1,Phase 2c — Switch LLM conversion of failing procedures (optional)
# Targeted hybrid: Phase 2a (deterministic sqlglot) already converts ~99% of tables/views.
# The T-SQL *procedures* it can't parse (control flow, dynamic SQL, MERGE) are sent to
# Lakebridge **Switch** — the agentic LLM transpiler — but ONLY those failing procedures,
# to minimise LLM cost. Entirely gated by the `use_switch` widget/variable.
import re, shutil
from datetime import datetime, timezone

_use_switch   = dbutils.widgets.get("use_switch").strip().lower() in ("true", "1", "yes")
_switch_model = dbutils.widgets.get("switch_model").strip() or "databricks-claude-sonnet-5"

if not _use_switch:
    print("use_switch=false → skipping Switch. Deterministic sqlglot results stand.")
    print("  Set use_switch=true (databricks.yml var or the widget) to LLM-convert the failing procedures.")
else:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    # 1) procedures the deterministic engine could not transpile
    fails = [r.file_name for r in spark.sql(
        f"SELECT file_name FROM {UC_CAT}.{UC_SCHEMA}.conversion_results "
        f"WHERE NOT transpiled AND file_type = 'CREATE PROCEDURE'").collect()]
    src_by_id = {o["object_id"]: o for o in sql_objects_list}

    if not fails:
        print("No failing procedures — nothing for Switch to do.")
    else:
        # 2) extract ONLY those procedures, one file each, into a UC Volume input folder
        switch_in = Path(f"/Volumes/{UC_CAT}/{UC_SCHEMA}/assessment_output/switch_input")
        shutil.rmtree(switch_in, ignore_errors=True)
        switch_in.mkdir(parents=True, exist_ok=True)
        n = 0
        for oid in fails:
            o = src_by_id.get(oid)
            if not o:
                continue
            (switch_in / (re.sub(r"[^0-9A-Za-z_.]", "_", oid) + ".sql")).write_text(o["source"], encoding="utf-8")
            n += 1
        print(f"Extracted {n} failing procedures → {switch_in}")

        # 3) locate the Switch job — or deploy it FROM THIS NOTEBOOK (no local CLI needed).
        # Switch ships with Lakebridge (pip-installed in cell 1), so SwitchDeployment can
        # create the Lakebridge_Switch job in-workspace on first use. This keeps the demo
        # self-contained: everything runs in the workspace, nothing installed on a laptop.
        from databricks.labs.blueprint.installation import Installation
        from databricks.labs.blueprint.installer import InstallState
        from databricks.labs.lakebridge.deployment.switch import SwitchDeployment
        _inst  = Installation(w, "lakebridge")
        _state = InstallState.from_installation(_inst)
        job_id = _state.jobs.get("Switch")
        if not job_id:  # fall back to lookup by job name
            _j = next((j for j in w.jobs.list() if j.settings and j.settings.name == "Lakebridge_Switch"), None)
            job_id = _j.job_id if _j else None
        if not job_id:
            print("Lakebridge_Switch not found — deploying Switch from the notebook (one-time)…")
            SwitchDeployment(w, _inst, _state).install(use_serverless=True)
            job_id = _state.jobs.get("Switch")
            print(f"  Deployed Switch job id={job_id}")

        if not job_id:
            print(f"⚠ Could not deploy/find the Switch job. Procedures staged at {switch_in}.")
        else:
            # 4) write a Switch config file. target_type / source_format etc. come from this
            # YAML (via switch_config_path), NOT job parameters.
            cfg_path = Path(f"/Volumes/{UC_CAT}/{UC_SCHEMA}/assessment_output/switch_config.yml")
            cfg_path.write_text(
                'target_type: "notebook"\n'      # full pipeline: analyze→convert→validate→fix
                'source_format: "sql"\n'
                'comment_lang: "English"\n'
                'log_level: "INFO"\n'
                'token_count_threshold: 20000\n'
                'concurrency: 4\n'
                'max_fix_attempts: 1\n'
                'sdp_language: "python"\n', encoding="utf-8")

            # 5) trigger Switch on just those procedures (source-dialect mssql = SQL Server T-SQL)
            _ts   = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            _user = w.current_user.me().user_name
            out_folder = f"/Workspace/Users/{_user}/switch_output/{UC_SCHEMA}_{_ts}"
            run = w.jobs.run_now(job_id=int(job_id), job_parameters={
                "source_tech":        "mssql",     # SQL Server / APS T-SQL
                "input_dir":          str(switch_in),
                "output_dir":         out_folder,
                "foundation_model":   _switch_model,
                "catalog":            UC_CAT,
                "schema":             UC_SCHEMA,
                "switch_config_path": str(cfg_path),
            })
            url = f"{w.config.host}/jobs/{job_id}/runs/{run.run_id}"
            print(f"✓ Switch triggered on {n} procedures using {_switch_model}")
            print(f"  Run:    {url}")
            print(f"  Output: {out_folder}  (converted notebooks)")
            print("  Switch runs asynchronously (analyze→convert→validate→fix); does not block this job.")

# COMMAND ----------

# DBTITLE 1,How to adapt for your own code
# MAGIC %md
# MAGIC ## How to adapt for your own code
# MAGIC
# MAGIC Point the demo at real code by setting the `input_path` variable in **databricks.yml**
# MAGIC (no notebook edits needed):
# MAGIC
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   input_path:
# MAGIC     default: "/Volumes/my_catalog/my_schema/landing"   # drop .sql / .dtsx here
# MAGIC ```
# MAGIC
# MAGIC Re-deploy the bundle and run the job (or set the **input_path** widget for an interactive
# MAGIC run). The pipeline is idempotent — tables are upserted, not recreated.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Querying the unified metric view:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Overall transpilability rate
# MAGIC SELECT MEASURE(`Transpilability Rate`)
# MAGIC FROM classic_stable_pr2ip7.lakebridge_assessment.assessment_metrics
# MAGIC
# MAGIC -- Breakdown by platform and complexity
# MAGIC SELECT `Platform`, `Complexity`, MEASURE(`Total Files`), MEASURE(`Transpilable Files`)
# MAGIC FROM classic_stable_pr2ip7.lakebridge_assessment.assessment_metrics
# MAGIC GROUP BY ALL
# MAGIC ```
# MAGIC
# MAGIC The MV joins `job_details` (inventory) with `conversion_results` (transpilation outcomes) and exposes dimensions (`Platform`, `Complexity`, `Engine`, `Transpiled`) and measures (`Total Files`, `Transpilable Files`, `Transpilability Rate`, `Total Statements Converted`).

# COMMAND ----------

# DBTITLE 1,Migration Cost Estimate — Effort Hypothesis
# MAGIC %md
# MAGIC ## Effort Hypothesis — Migration Cost Estimation
# MAGIC
# MAGIC Edit `unit_rates` and `overhead_rates` in the cell below, then re-run. The dashboard reads live from Delta.
# MAGIC
# MAGIC **Transpilability status** is derived from `conversion_results.transpiled` (populated by the conversion phases):
# MAGIC * SQL files (Phase 2a): `engine = sqlglot`, `transpiled` = error_count == 0 (deterministic)
# MAGIC * SSIS packages (Phase 2b): `engine = bladebridge`, `transpiled` = the package converted with no diagnostics
# MAGIC
# MAGIC > **Note:** The `transformations.supported` flag from bladespector reflects **analytic transpilability** — whether bladespector's rule-based engine can categorize a component type. The actual SSIS conversion outcome is decided at runtime by BladeBridge (Phase 2b) and recorded per package in `conversion_results`.
# MAGIC
# MAGIC Non-transpilable objects carry a **3× effort uplift** vs transpilable.
# MAGIC
# MAGIC | Object type | Complexity | Transpilable MD | Not-transpilable MD | Profile |
# MAGIC |---|---|---|---|---|
# MAGIC | SQL Statement | LOW | 0.2 | 0.6 | Senior Data Engineer |
# MAGIC | SQL Statement | MEDIUM | 1.5 | 4.5 | Senior Data Engineer |
# MAGIC | SQL Statement | HIGH | 3.0 | 9.0 | Senior Data Engineer |
# MAGIC | T-SQL Function | STANDARD | 0.25 | 0.75 | Senior Data Engineer |
# MAGIC | SSIS Component | Orchestration | 0.25 | 0.75 | ETL Specialist |
# MAGIC | SSIS Component | Transformation | 0.5 | 1.5 | ETL Specialist |
# MAGIC
# MAGIC **Overhead:** Data Architect 10%, QA Engineer 20% of dev total.

# COMMAND ----------

# DBTITLE 1,Effort Hypothesis — seed Delta tables
# ── EFFORT HYPOTHESIS ────────────────────────────────────────────────────────
# Edit these lists to model different migration scenarios.
# After editing, re-run this cell — the dashboard updates automatically.
#
# transpilability_status values:
#   'transpilable'     — Lakebridge auto-converts; effort = review + validate
#   'not_transpilable' — requires manual rewrite (≈ 2–3× the transpilable rate)

UC_CAT    = dbutils.widgets.get("catalog")
UC_SCHEMA = dbutils.widgets.get("schema")

# (object_type, complexity_level, transpilability_status, effort_md_per_unit, profile, notes)
unit_rates = [
    # ── Transpilable — Lakebridge handles auto-conversion ───────────────────
    ("SQL Statement",  "LOW",            "transpilable",     0.2,  "Senior Data Engineer", "Auto-converted — review & validate output"),
    ("SQL Statement",  "MEDIUM",         "transpilable",     1.5,  "Senior Data Engineer", "Auto-converted — review CTEs and JOINs"),
    ("SQL Statement",  "HIGH",           "transpilable",     3.0,  "Senior Data Engineer", "Auto-converted — review SPs and dynamic SQL"),
    ("T-SQL Function", "STANDARD",       "transpilable",     0.25, "Senior Data Engineer", "Auto-converted — validate function behaviour"),
    ("SSIS Component", "Orchestration",  "transpilable",     0.25, "ETL Specialist",       "Supported component — reconfigure in Databricks"),
    ("SSIS Component", "Transformation", "transpilable",     0.5,  "ETL Specialist",       "Supported component — map to Spark equivalent"),
    # ── Not transpilable — manual rewrite required (≈ 2–3× transpilable rate) ──
    ("SQL Statement",  "LOW",            "not_transpilable", 0.6,  "Senior Data Engineer", "Manual rewrite — 3× uplift for unsupported syntax"),
    ("SQL Statement",  "MEDIUM",         "not_transpilable", 4.5,  "Senior Data Engineer", "Manual rewrite — 3× uplift for moderate complexity"),
    ("SQL Statement",  "HIGH",           "not_transpilable", 9.0,  "Senior Data Engineer", "Manual rewrite — 3× uplift for complex SP / dynamic SQL"),
    ("T-SQL Function", "STANDARD",       "not_transpilable", 0.75, "Senior Data Engineer", "Manual reimplementation — 3× uplift"),
    ("SSIS Component", "Orchestration",  "not_transpilable", 0.75, "ETL Specialist",       "Redesign control-flow — 3× uplift for unsupported task"),
    ("SSIS Component", "Transformation", "not_transpilable", 1.5,  "ETL Specialist",       "Redesign data-flow — 3× uplift for unsupported component"),
]

# (profile, rate, notes)  — rate is a fraction of dev total (0.10 = 10 %)
overhead_rates = [
    ("Data Architect", 0.10, "Design, technical governance, code review"),
    ("QA Engineer",    0.20, "Unit tests, integration tests, UAT support"),
]

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

unit_schema = StructType([
    StructField("object_type",            StringType()),
    StructField("complexity_level",       StringType()),
    StructField("transpilability_status", StringType()),
    StructField("effort_md_per_unit",     DoubleType()),
    StructField("profile",                StringType()),
    StructField("notes",                  StringType()),
])
overhead_schema = StructType([
    StructField("profile", StringType()),
    StructField("rate",    DoubleType()),
    StructField("notes",   StringType()),
])

spark.createDataFrame(unit_rates, unit_schema)\
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
     .saveAsTable(f"{UC_CAT}.{UC_SCHEMA}.effort_hypothesis")

spark.createDataFrame(overhead_rates, overhead_schema)\
     .write.mode("overwrite").saveAsTable(f"{UC_CAT}.{UC_SCHEMA}.overhead_hypothesis")

print("✓ Hypothesis tables written to Delta.")
display(spark.table(f"{UC_CAT}.{UC_SCHEMA}.effort_hypothesis"))
display(spark.table(f"{UC_CAT}.{UC_SCHEMA}.overhead_hypothesis"))

# COMMAND ----------

# DBTITLE 1,Phase 3 — UC Metric Views (dashboard datasets)
# Two Unity Catalog metric views back the Lakebridge Migration Assessment dashboard.
# They are the dashboard's ONLY datasets (besides the effort-hypothesis rate card),
# so all KPIs are defined once here and reused via MEASURE() — no query duplication.
#
# Created LAST, after every source table exists (job_details, conversion_results,
# sql_statements, transformations, functions, effort_hypothesis, overhead_hypothesis).
UC_CAT    = dbutils.widgets.get("catalog")
UC_SCHEMA = dbutils.widgets.get("schema")
FQ = f"{UC_CAT}.{UC_SCHEMA}"

def _try_metric_view(ddl: str, name: str) -> None:
    # Metric views need a recent runtime (DBSQL / newer DBR); this job cluster (DBR 15.4)
    # cannot parse `WITH METRICS`. The dashboard uses base-table datasets, so a failure
    # here is non-fatal — log and continue so the assessment still completes.
    try:
        spark.sql(ddl)
        print(f"✓ Created metric view {name}")
    except Exception as e:
        print(f"⚠ Skipped metric view {name}: {type(e).__name__}: {str(e).splitlines()[0][:160]}")
        print("  Non-fatal — dashboard reads base tables, not metric views.")

# 1) assessment_metrics — inventory × transpilability, 1 row per source file.
_try_metric_view(f"""
CREATE OR REPLACE VIEW {FQ}.assessment_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: |
  SELECT j.platform, j.file_name, j.job_type, j.categorization, j.number_of_nodes, j.included,
         c.engine, c.model, COALESCE(c.transpiled, TRUE) AS transpiled, c.failure_reason,
         c.success_count
  FROM {FQ}.job_details j
  LEFT JOIN {FQ}.conversion_results c ON j.file_name = c.file_name
comment: Unified migration assessment — inventory joined with conversion outcomes, per source file.
dimensions:
  - name: Platform
    expr: platform
  - name: File Name
    expr: file_name
  - name: Job Type
    expr: job_type
  - name: Complexity
    expr: categorization
  - name: Included
    expr: included
  - name: Engine
    expr: engine
  - name: Model
    expr: model
  - name: Transpiled
    expr: CAST(transpiled AS STRING)
  - name: Failure Reason
    expr: failure_reason
measures:
  - name: Total Files
    expr: COUNT(DISTINCT `File Name`)
  - name: Transpilable Files
    expr: COUNT(DISTINCT `File Name`) FILTER (WHERE transpiled = true)
  - name: Not Transpilable Files
    expr: COUNT(DISTINCT `File Name`) FILTER (WHERE transpiled = false)
  - name: Transpilability Rate
    expr: COUNT(DISTINCT `File Name`) FILTER (WHERE transpiled = true) * 100.0 / NULLIF(COUNT(DISTINCT `File Name`), 0)
  - name: Low Complexity Files
    expr: COUNT(DISTINCT `File Name`) FILTER (WHERE `Complexity` = 'LOW')
  - name: Medium Complexity Files
    expr: COUNT(DISTINCT `File Name`) FILTER (WHERE `Complexity` = 'MEDIUM')
  - name: High Complexity Files
    expr: COUNT(DISTINCT `File Name`) FILTER (WHERE `Complexity` = 'HIGH')
  - name: Avg Nodes per File
    expr: AVG(number_of_nodes)
$$
""", f"{FQ}.assessment_metrics")

# 2) effort_metrics — per-object effort estimate (+ overhead rows), driven by the
#    rate card. SSIS vs SQL is keyed off platform (job_type is 'Package'/'SQL File').
_try_metric_view(f"""
CREATE OR REPLACE VIEW {FQ}.effort_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: |
  WITH file_effort AS (
    SELECT j.file_name AS object_name,
      CASE WHEN j.platform = 'SSIS' THEN 'SSIS Component' ELSE 'SQL Statement' END AS object_type,
      CASE WHEN j.platform = 'SSIS'
           THEN CASE WHEN j.categorization = 'HIGH' THEN 'Transformation' ELSE 'Orchestration' END
           ELSE j.categorization END AS complexity_level,
      CASE WHEN c.transpiled THEN 'transpilable' ELSE 'not_transpilable' END AS transpilability_status,
      j.platform, c.engine, c.model
    FROM {FQ}.job_details j
    LEFT JOIN {FQ}.conversion_results c ON j.file_name = c.file_name
  ),
  with_effort AS (
    SELECT f.object_name, f.object_type, f.complexity_level, f.transpilability_status,
           f.platform, f.engine, f.model, h.profile, h.effort_md_per_unit AS effort_md
    FROM file_effort f
    JOIN {FQ}.effort_hypothesis h
      ON h.object_type = f.object_type AND h.complexity_level = f.complexity_level
     AND h.transpilability_status = f.transpilability_status
  ),
  dev_total AS (SELECT SUM(effort_md) AS total FROM with_effort),
  overhead AS (
    SELECT '— Overhead' AS object_name, '— Overhead' AS object_type, o.profile AS complexity_level,
           NULL AS transpilability_status, NULL AS platform, NULL AS engine, NULL AS model,
           o.profile, ROUND(d.total * o.rate, 2) AS effort_md
    FROM {FQ}.overhead_hypothesis o CROSS JOIN dev_total d
  )
  SELECT * FROM with_effort UNION ALL SELECT * FROM overhead
comment: Per-object migration effort (man-days) from the rate card, including overhead rows.
dimensions:
  - name: object_name
    expr: object_name
  - name: object_type
    expr: object_type
  - name: complexity_level
    expr: complexity_level
  - name: transpilability_status
    expr: transpilability_status
  - name: platform
    expr: platform
  - name: engine
    expr: engine
  - name: model
    expr: model
  - name: profile
    expr: profile
measures:
  - name: Effort (MD)
    expr: SUM(effort_md)
$$
""", f"{FQ}.effort_metrics")

# Quick sanity check (only meaningful if the metric views were created)
try:
    display(spark.sql(f"""
      SELECT 'Total files'        AS metric, CAST(MEASURE(`Total Files`) AS STRING) AS value FROM {FQ}.assessment_metrics
      UNION ALL SELECT 'Transpilability %', CAST(ROUND(MEASURE(`Transpilability Rate`),1) AS STRING) FROM {FQ}.assessment_metrics
      UNION ALL SELECT 'Total effort (MD)', CAST(MEASURE(`Effort (MD)`) AS STRING) FROM {FQ}.effort_metrics
    """))
except Exception as e:
    print(f"⚠ Metric-view sanity check skipped: {type(e).__name__}: {str(e).splitlines()[0][:160]}")