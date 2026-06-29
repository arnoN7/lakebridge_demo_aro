"""
Lakebridge Migration Demo — Path Configuration
==============================================
Centralised place to switch between demo mode (sample_assets/) and a real
client engagement (actual client drop folder).

This file is imported by the main notebook. To run against a real client
delivery, change input_root and re-run the notebook from cell 3 onwards.
"""
from pathlib import Path

# Git repo root — adjust if this file is moved to a different location
REPO_ROOT = Path(__file__).resolve().parent.parent

# ────────────────────────────────────────────────────────────────────────────────
# DEMO MODE: reads from version-controlled fake assets
input_root = REPO_ROOT / "sample_assets"

# REAL ENGAGEMENT: uncomment the line below and set the actual path
# input_root = Path("/Volumes/my_catalog/my_schema/client_delivery")
# ────────────────────────────────────────────────────────────────────────────────

# Output directories (git-ignored — never committed)
output_root       = REPO_ROOT / "_output" / "converted"
ssis_extract_root = REPO_ROOT / "_output" / "ssis_extracted_sql"
