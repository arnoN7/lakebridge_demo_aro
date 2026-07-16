# Lakebridge SSIS & T-SQL Migration Demo

A folder-driven migration analysis and conversion project using [Databricks Labs Lakebridge](https://github.com/databrickslabs/lakebridge).

> **Runs entirely inside your Databricks workspace — no local install required.**
> Lakebridge and the BladeBridge SSIS transpiler are `pip`-installed by the notebook
> and execute on the job cluster. You never install the Lakebridge CLI or its native
> binaries on your laptop, so this works under security policies that block local installs.
> The only thing you run locally is the standard Databricks CLI, to deploy the bundle.

## How to use it

1. **Clone this repo as a Git folder** in the Databricks workspace (Workspace → Create → Git folder).
2. **Deploy the bundle** with the Databricks CLI: `./deploy.sh` (creates the setup job + the dashboard).
3. **Run the guided notebook** (or the `lakebridge_setup` job) to assess and convert your own
   T-SQL and SSIS — drop your files into a Volume and set `input_path` in `databricks.yml`.

The conversion (assessment, T-SQL via SqlglotEngine, full SSIS packages via BladeBridge) all
happens on the cluster.

## Project structure

```
lakebridge_demo/
├── databricks.yml                    ← DAB bundle (job + dashboard resources)
├── deploy.sh                         ← One-command deploy script
├── README.md
├── requirements.txt                  ← Python dependencies
├── .gitignore
├── scripts/
│   ├── build_dashboard.py            ← Parameterizes .lvdash.json for target catalog
│   └── generate_sample_ssis.py      ← Regenerates the SSIS sample packages
├── sample_assets/                    ← Version-controlled demo assets
│   ├── 01_schema_sales.sql           ← T-SQL DDL
│   ├── 02_incremental_extract.sql    ← T-SQL query
│   ├── 03_sp_load_customer_metrics.sql
│   ├── 04_sp_upsert_inventory.sql    ← T-SQL stored procedures
│   ├── 05_ssis_customer_load.dtsx    ← SSIS packages
│   ├── 06_ssis_finance_sync.dtsx
│   └── 07–10_ssis_*.dtsx             ← richer SSIS packages (loops, SCD2, DQ checks…)
├── Lakebridge Migration Assessment.lvdash.json  ← Dashboard source (template)
├── Lakebridge SSIS and TSQL Migration Demo.py   ← Main Databricks notebook
├── scripts/clean_aps_sql.py          ← APS/PDW (.dsql) → T-SQL normalizer (Phase 0a)
├── scripts/sql_objects.py            ← splits bulk SQL into per-object records
└── dist/                             ← Generated parameterized dashboard — git-ignored
```

Converted outputs are written to a **UC Volume**, not the repo:
`/Volumes/{catalog}/{schema}/assessment_output/` — `converted/` (transpiled T-SQL,
Phase 2a) and `ssis_sdp/` (converted SSIS notebooks, Phase 2b).

The dashboard's datasets query the **base Delta tables** the notebook writes
(`job_details`, `conversion_results`, `effort_hypothesis`, `overhead_hypothesis`),
so it renders on any SQL warehouse version. The notebook also creates two UC metric
views (`assessment_metrics`, `effort_metrics`) on a best-effort basis (they need a
recent DBSQL runtime); the dashboard does not depend on them.

## Quick start (interactive)

1. Open the notebook **Lakebridge SSIS and TSQL Migration Demo** in Databricks
2. Run all cells top to bottom
3. Review the inventory, analysis, and conversion outputs in the
   `/Volumes/{catalog}/{schema}/assessment_output/` Volume

## Deploy (Declarative Automation Bundles)

The repo ships as a fully deployable DAB bundle. Anyone with the Databricks CLI
can clone and deploy the notebook job **and** the dashboard in three commands:

```bash
# 1. Install the CLI (once)
pip install databricks-cli
databricks configure          # set DATABRICKS_HOST + DATABRICKS_TOKEN

# 2. Clone the repo
git clone https://github.com/arnaud-rover_data/lakebridge_demo.git
cd lakebridge_demo

# 3. Deploy to dev (default catalog classic_stable_pr2ip7)
./deploy.sh

# — or — deploy to a different catalog:
./deploy.sh dev my_catalog my_schema

# — or — deploy to production:
./deploy.sh prod my_prod_catalog lakebridge_assessment
```

`deploy.sh` does three things in sequence:

| Step | What happens |
|------|-------------|
| 1 | `scripts/build_dashboard.py` substitutes the target catalog/schema into the dashboard JSON and writes it to `dist/` |
| 2 | `databricks bundle deploy` creates the Lakeview dashboard and the setup job in the workspace |
| 3 | `databricks bundle run lakebridge_setup` runs the notebook on a cluster: assessment, conversion, UC tables, and the two metric views |

> The Databricks CLI used here only deploys the bundle. Lakebridge and BladeBridge are
> **not** installed locally — the notebook `pip`-installs them on the cluster at run time.

### Targeting a different environment

```bash
# Override any variable on the CLI
databricks bundle deploy -t prod \
  --var catalog=my_prod_catalog \
  --var schema=lakebridge_assessment
```

Targets and default variable values are defined in `databricks.yml`.

### Node type & compute

The job cluster is a **classic** cluster configured for Azure (`Standard_DS3_v2`).
Edit the `node_type_id` in `databricks.yml` for AWS (`m5.xlarge`) or GCP (`n1-standard-4`).

A classic cluster is required because the BladeBridge SSIS transpiler (Phase 2b) is a
native Linux binary. Assessment, T-SQL conversion, and the metric views also run on Serverless.

## Demo vs real engagement

In demo mode the notebook reads from `sample_assets/` (version-controlled fake assets).

To analyse real code, set the `input_path` variable in `databricks.yml` to the
Volume/folder holding your `.sql` / `.dtsx` files (e.g. `/Volumes/<catalog>/<schema>/landing`),
re-deploy, and run the job. Leave it blank to use the bundled `sample_assets/`.

## Dependencies

Installed on the cluster by the notebook's first cell (`%pip install …`):

```
databricks-labs-lakebridge   # bladespector analyzer + SqlglotEngine
databricks-bb-plugin         # BladeBridge SSIS transpiler (native dbxconv binary)
openpyxl                     # read bladespector's Excel reports
```

## Supported sources

This demo covers **T-SQL (MS SQL Server)** via `SqlglotEngine` and **SSIS** (`.dtsx`) via
BladeBridge. Lakebridge/BladeBridge support many more source systems (Synapse, Oracle,
Netezza, Redshift, Teradata, Informatica, DataStage, …) — see the
[Lakebridge docs](https://databrickslabs.github.io/lakebridge/).
