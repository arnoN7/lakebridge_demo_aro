#!/usr/bin/env bash
# Deploy the Lakebridge demo to a Databricks workspace.
#
# Usage:
#   ./deploy.sh                                     # dev, default catalog
#   ./deploy.sh dev my_catalog                      # dev, custom catalog
#   ./deploy.sh prod my_prod_catalog my_schema      # prod, custom catalog + schema
#
# Positional args (all optional):
#   $1  TARGET    dev (default) | prod
#   $2  CATALOG   UC catalog name  (default: classic_stable_pr2ip7)
#   $3  SCHEMA    UC schema name   (default: lakebridge_assessment)
#
# Prerequisites (local machine):
#   pip install databricks-cli        # or: brew install databricks
#   databricks configure              # set DATABRICKS_HOST + DATABRICKS_TOKEN
#
# Note: Lakebridge / BladeBridge are NOT installed locally. The notebook pip-installs
# them on the job cluster at run time, so no native binaries touch your machine.

set -euo pipefail

TARGET=${1:-dev}
CATALOG=${2:-classic_stable_pr2ip7}
SCHEMA=${3:-lakebridge_assessment}

echo "======================================================"
echo "  Lakebridge Demo -- Deploy"
echo "  Target  : $TARGET"
echo "  Catalog : $CATALOG"
echo "  Schema  : $SCHEMA"
echo "======================================================"

# 1. Parameterize the dashboard JSON (writes to dist/)
echo ""
echo "Step 1/3 -- Parameterizing dashboard JSON..."
python scripts/build_dashboard.py \
  --catalog "$CATALOG" \
  --schema  "$SCHEMA"

# 2. Deploy bundle resources (job + dashboard)
echo ""
echo "Step 2/3 -- Deploying bundle (target: $TARGET)..."
databricks bundle deploy -t "$TARGET" \
  --var catalog="$CATALOG" \
  --var schema="$SCHEMA"

# 3. Run the setup job to seed all UC tables
echo ""
echo "Step 3/3 -- Running setup job (assessment + table seeding)..."
databricks bundle run lakebridge_setup -t "$TARGET" \
  --param catalog="$CATALOG" \
  --param schema="$SCHEMA"

echo ""
echo "Done! Open your workspace -> Dashboards -> Lakebridge Migration Assessment"
