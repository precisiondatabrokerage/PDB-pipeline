# PDB Company Pipeline Runbook — Day 1

**Status:** Authoritative for current sprint  
**Purpose:** Eliminate mystery runs and split-brain execution in the company pipeline  
**Owner:** Samuel Barnes  
**Last Updated:** 2026-03-26

---

## 1. Official Company Pipeline

Use this path and no other for the company workflow:

1. `PDB-pipeline/main_scraper.py`  
   Raw company ingestion only. Produces a `run_id`.

2. `PDB-etl/runners/run_etl_for_run_id.py`  
   Authoritative ETL router for web runs. Produces a `dataset_id`.

3. `PDB-pipeline/runners/run_company_entity_expansion.py`  
   Writes raw company expansion docs. This is **not** the apply step.

4. `PDB-etl/transform/company_entity_expansion_apply_v1.py`  
   Authoritative apply step for company expansion results.

5. `PDB-etl/transform/company_contact_discovery_v1.py`  
   Authoritative company contact discovery step.

---

## 2. Non-Authoritative / Legacy Paths

Do **not** use these for the active company pipeline:

- `PDB-etl/etl_raw_to_clean.py`
- `PDB-etl/etl_rawCompanies_to_cleanCompanies.py`
- legacy pipeline-side ETL files under `PDB-pipeline/etl/*`
- any older wrapper flow that bypasses:
  - `run_etl_for_run_id`
  - `company_entity_expansion_apply_v1`
  - `company_contact_discovery_v1`

---

## 3. Production Runbook

### Step 1 — Raw Ingestion

```powershell
cd C:\Users\samue\Repos\PDB-pipeline
$env:APP_ENV="production"
python -u main_scraper.py