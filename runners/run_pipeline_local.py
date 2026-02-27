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
# Force LOCAL Postgres
# --------------------------------------------------
local_dsn = os.getenv("POSTGRES_DSN")
if not local_dsn:
    raise RuntimeError("POSTGRES_DSN not set — cannot run local pipeline")

os.environ["ACTIVE_POSTGRES_DSN"] = local_dsn

print("🧪 Running PDB pipeline against LOCAL Postgres")
print(f"➡ DSN: {local_dsn}")

# --------------------------------------------------
# Run pipeline
# --------------------------------------------------
from main_scraper import run_pipeline

run_pipeline()
