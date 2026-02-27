from dotenv import load_dotenv
import os
import sys
from pathlib import Path

# --------------------------------------------------
# Ensure project root is on PYTHONPATH
# --------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# --------------------------------------------------
# Load environment
# --------------------------------------------------
load_dotenv()

# --------------------------------------------------
# Force SUPABASE Postgres
# --------------------------------------------------
prod_dsn = os.getenv("SUPABASE_POSTGRES_URL")
if not prod_dsn:
    raise RuntimeError("SUPABASE_POSTGRES_URL not set — cannot run prod pipeline")

os.environ["ACTIVE_POSTGRES_DSN"] = prod_dsn

print("🚨 RUNNING PDB PIPELINE AGAINST PRODUCTION (SUPABASE)")
print(f"➡ DSN: {prod_dsn}")

confirm = input("\nType EXACTLY 'DEPLOY' to continue: ")
if confirm != "DEPLOY":
    print("❌ Aborted. No changes were made.")
    sys.exit(1)

# --------------------------------------------------
# Run pipeline
# --------------------------------------------------
from main_scraper import run_pipeline

run_pipeline()
