Here is the cleaner v2 runbook in Markdown. It reflects the current one-command orchestrator, the stepwise debug path, the scheduled GitHub workflow, and the current preset-driven `web_scraper_targets.py` model. The daily workflow now includes emergency Mongo raw cleanup before running the orchestrator, and the current target model is still preset-based rather than registry-based.  

# PDB Company Pipeline Runbook — Web / Company v2

**Status:** Authoritative for the current web/company sprint
**Purpose:** Keep operators on one path, reduce mystery runs, and document the current preset-driven scraper + web/company ETL flow
**Owner:** Samuel Barnes
**Last Updated:** 2026-05-20

---

## 0. Operator Quick Reference

### Normal manual full run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:APP_ENV="production"
python -u -m runners.run_web_company_end_to_end --trigger manual --raw-retention-mode archive-then-purge --metrics-json-out web_run_metrics.json
```

### Manual targeted market run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="nashville_real_estate_agents"
$env:APP_ENV="production"
python -u main_scraper.py
```

Then resume from the returned `run_id`:

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="nashville_real_estate_agents"
$env:APP_ENV="production"
python -u -m runners.run_web_company_end_to_end --run-id <RUN_ID> --trigger manual --raw-retention-mode archive-then-purge --metrics-json-out web_run_metrics.json
```

### Daily scheduled run

* GitHub Actions workflow: `Daily Web Company Run`
* Schedule: `15 13 * * *`
* Runs the one-command orchestrator after emergency Mongo raw cleanup.  

### Current target model

Current targeting is still **preset-based** via `ACTIVE_SCRAPER_PRESET`, with presets including `default_tn_services`, `nashville_real_estate_agents`, `memphis_tn_services`, `franklin_tn_services`, `jackson_tn_services`, `hendersonville_tn_services`, and `smyrna_tn_services`. 

---

## 1. Authoritative Operator Paths

### 1.1 Primary operator path

Use this path for normal production and scheduled execution:

1. `pdb-pipeline/runners/run_web_company_end_to_end.py`

This is the current one-command operator path. It is the path used by the scheduled GitHub workflow. 

### 1.2 Stepwise debug / recovery path

Use this path only when debugging or recovering a partially completed run:

1. `pdb-pipeline/main_scraper.py`
   Raw web ingestion only. Produces a `run_id`.

2. `pdb-etl/runners/run_web_etl_with_metrics.py --run-id <RUN_ID>`
   Calls the authoritative ETL router. 

3. `pdb-pipeline/runners/run_company_entity_expansion_with_metrics.py --parent-run-id <RUN_ID>`
   Writes raw company expansion docs.

4. `pdb-etl/runners/run_company_entity_expansion_apply_with_metrics.py --parent-run-id <RUN_ID> --dataset-id <DATASET_ID>`
   Applies expansion docs to canonical companies.

5. `pdb-etl/runners/run_company_contact_discovery_with_metrics.py --run-id <RUN_ID> --restrict-to-run-companies`
   Runs run-scoped company contact discovery. A successful Nashville debug run followed this exact sequence.  

---

## 2. Official Web / Company Flow

### 2.1 Raw ingestion

`main_scraper.py` runs the active YellowPages target list, enriches each result with lightweight website metadata, writes raw rows to `raw_businesses`, and creates an `ingestion_runs` document.

### 2.2 Authoritative web ETL

The web ETL wrapper calls `run_etl_for_run_id(run_id)`, and the router sends `web_ingestion` runs into `_run_web_flow(run_id)`. That flow:

* fetches raw businesses
* normalizes them
* resolves entities
* upserts canonical companies
* finalizes the dataset.  

### 2.3 Expansion and discovery

After ETL, the company flow continues with:

* raw company expansion
* company expansion apply
* company contact discovery
* final validation
* metrics output
* post-run raw retention attempt. 

---

## 3. Non-Authoritative / Legacy Paths

Do **not** use these for the active web/company pipeline:

* `pdb-etl/etl_raw_to_clean.py`
* `pdb-etl/etl_rawCompanies_to_cleanCompanies.py`
* older pipeline-side ETL shortcuts
* any flow that bypasses:

  * `run_etl_for_run_id`
  * `run_company_entity_expansion_apply_with_metrics`
  * `run_company_contact_discovery_with_metrics`

These files may still exist, but they are not the active web/company operator path. 

---

## 4. Current Target Selection

### 4.1 How target selection works

The current target model is **preset-based**.

* `ACTIVE_SCRAPER_PRESET` selects one preset
* `get_active_yellowpages_targets()` expands enabled markets and enabled industries into query pairs
* each target carries its own `headless`, `max_pages`, and `max_scrolls` settings. 

### 4.2 Current presets

Current working presets include:

* `default_tn_services`
* `nashville_real_estate_agents`
* `memphis_tn_services`
* `franklin_tn_services`
* `jackson_tn_services`
* `hendersonville_tn_services`
* `smyrna_tn_services` 

### 4.3 Current Tennessee default service pack behavior

`default_tn_services` currently contains hand-curated Tennessee markets such as Knoxville, Maryville, and Chattanooga, with disabled entries still present for Johnson City and Pigeon Forge. 

### 4.4 Current Nashville real-estate preset

`nashville_real_estate_agents` currently targets:

* Real Estate Agents
* Real Estate Buyer Brokers
* Real Estate Consultants
* Commercial Real Estate
* Real Estate Referral & Information Service. 

### 4.5 Scaling note

A registry-based statewide architecture was discussed, but it is **not yet the active production targeting code**. Current production behavior is still preset-based. 

---

## 5. Scheduled Workflow

### 5.1 Workflow name and trigger

Workflow: `Daily Web Company Run`

Triggers:

* cron: `15 13 * * *`
* `workflow_dispatch` manual trigger. 

### 5.2 Current scheduled sequence

The current workflow:

1. checks out `pdb-pipeline`
2. validates required secrets and variables
3. checks out `pdb-etl`
4. installs dependencies and Playwright Chromium
5. performs **Emergency Mongo raw quota cleanup**
6. runs `run_web_company_end_to_end`
7. builds a run summary
8. enforces completed status
9. uploads artifacts.  

### 5.3 Current required GitHub configuration

**Repository secrets**

* `PDB_REPO_PAT`
* `SUPABASE_POSTGRES_URL`
* `MONGODB_URI`
* `MONGODB_DB`

**Repository variables**

* `PDB_ETL_REPOSITORY`
* `WEB_EXPANSION_LIMIT`
* `WEB_CONTACT_LIMIT` 

---

## 6. Manual Run Procedures

### 6.1 Standard one-command full run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:APP_ENV="production"
python -u -m runners.run_web_company_end_to_end --trigger manual --raw-retention-mode archive-then-purge --metrics-json-out web_run_metrics.json
```

### 6.2 Resume mode from an existing `run_id`

Use this when raw ingestion already succeeded.

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:APP_ENV="production"
python -u -m runners.run_web_company_end_to_end --run-id <RUN_ID> --trigger manual --raw-retention-mode archive-then-purge --metrics-json-out web_run_metrics.json
```

A successful Nashville run used this exact resume pattern after `main_scraper.py` completed. 

### 6.3 Targeted preset ingestion

Example:

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="franklin_tn_services"
$env:APP_ENV="production"
python -u main_scraper.py
```

Expected output includes:

```text
INGESTION COMPLETE — <N> raw records (run_id=<RUN_ID>)
```

### 6.4 Stepwise debug flow

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:APP_ENV="production"
python -u main_scraper.py
```

```powershell
cd C:\Users\samue\Repos\pdb-etl
$env:APP_ENV="production"
python -u -m runners.run_web_etl_with_metrics --run-id <RUN_ID>
```

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:APP_ENV="production"
python -u -m runners.run_company_entity_expansion_with_metrics --parent-run-id <RUN_ID> --limit 100
```

```powershell
cd C:\Users\samue\Repos\pdb-etl
$env:APP_ENV="production"
python -u -m runners.run_company_entity_expansion_apply_with_metrics --parent-run-id <RUN_ID> --dataset-id <DATASET_ID>
```

```powershell
cd C:\Users\samue\Repos\pdb-etl
$env:APP_ENV="production"
python -u -m runners.run_company_contact_discovery_with_metrics --run-id <RUN_ID> --limit-companies 250 --restrict-to-run-companies
```

The Nashville debug run showed this path working end-to-end, including web ETL, expansion, apply, and contact discovery.  

---

## 7. Preset-Specific Quick Commands

### Nashville real-estate run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="nashville_real_estate_agents"
$env:APP_ENV="production"
python -u main_scraper.py
```

### Memphis services run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="memphis_tn_services"
$env:APP_ENV="production"
python -u main_scraper.py
```

### Franklin services run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="franklin_tn_services"
$env:APP_ENV="production"
python -u main_scraper.py
```

### Jackson services run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="jackson_tn_services"
$env:APP_ENV="production"
python -u main_scraper.py
```

### Hendersonville services run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="hendersonville_tn_services"
$env:APP_ENV="production"
python -u main_scraper.py
```

### Smyrna services run

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:ACTIVE_SCRAPER_PRESET="smyrna_tn_services"
$env:APP_ENV="production"
python -u main_scraper.py
```

---

## 8. Validation

### 8.1 Primary validation sources

Use:

* stage metrics from each wrapper
* `web_run_metrics.json`
* final validation block from `run_web_company_end_to_end`

The scheduled workflow also writes these values into the GitHub workflow summary. 

### 8.2 Company inspection SQL

```sql
select
  c.id,
  c.canonical_name,
  c.industry_type,
  c.sub_industry,
  c.mailing_address,
  c.mailing_city,
  c.mailing_state,
  c.mailing_zip,
  c.mailing_county,
  c.website,
  c.domain,
  c.phone_primary,
  c.email_primary,
  c.contact_form_url
from public.companies c
join public.datasets d
  on d.id = c.dataset_id
where d.run_id = '<RUN_ID>'
order by c.canonical_name;
```

### 8.3 Lead inspection SQL

```sql
with run_companies as (
  select c.id, c.canonical_name
  from public.companies c
  join public.datasets d
    on d.id = c.dataset_id
  where d.run_id = '<RUN_ID>'
)
select
  l.id,
  rc.canonical_name as company_name,
  l.full_name,
  l.email,
  l.phone,
  l.role_title,
  l.source,
  l.syntax_valid,
  l.mx_valid,
  l.lead_stage,
  l.captured_at
from public.leads l
join run_companies rc
  on rc.id = l.company_id
order by l.captured_at desc nulls last;
```

---

## 9. Operational Rules

### 9.1 Use the one-command orchestrator by default

The default operator path is now `run_web_company_end_to_end`. The daily workflow already uses it directly. 

### 9.2 Use resume mode after successful ingestion

If `main_scraper.py` already produced a valid `run_id`, do not re-run raw scraping unnecessarily. Resume from that `run_id`. The Nashville run used this pattern successfully. 

### 9.3 Treat raw retention as best-effort until fixed

Current successful runs can still end with:

* `RAW RETENTION FAILED: TypeError: 'MongoCollections' object is not subscriptable`

until that helper is fixed. Treat the run as successful if core stages completed and final metrics status is `completed`, then inspect and clean raw Mongo separately if needed. 

### 9.4 Do not store full website HTML in `raw_businesses`

`main_scraper.py` must not store full page HTML inside Mongo raw docs. Large pages can exceed Mongo’s 16 MB BSON limit. Keep only lightweight website metadata in raw docs.

### 9.5 Current target selection is preset-based

Do not operate as if statewide registry mode is already live. Right now the active code path is still preset-driven through `ACTIVE_SCRAPER_PRESET`. 

---

## 10. Troubleshooting Appendix

### A. Ingestion succeeded but ETL should not rescrape

Use resume mode:

```powershell
cd C:\Users\samue\Repos\pdb-pipeline
$env:APP_ENV="production"
python -u -m runners.run_web_company_end_to_end --run-id <RUN_ID> --trigger manual --raw-retention-mode archive-then-purge --metrics-json-out web_run_metrics.json
```

### B. Mongo quota failure

Symptoms:

* `OperationFailure: you are over your space quota`

Immediate actions:

* manually archive/purge old `raw_businesses` runs
* or clear the raw collections if the canonical Postgres state is already preserved
* note that the daily GitHub workflow now performs emergency deletion of:

  * `raw_businesses`
  * `raw_company_expansions`
  * `raw_company_entity_expansion` before running. 

### C. Raw retention failed after a successful run

Symptoms:

* final metrics status is `completed`
* raw retention block shows failed

Current known example:

* `TypeError: 'MongoCollections' object is not subscriptable` after a completed run. 

Response:

* keep the run
* validate Postgres outputs
* inspect raw Mongo collections manually
* perform manual archive/purge if needed

### D. Expansion apply found docs but applied `0`

This usually means:

* raw expansion docs existed
* but they did not improve any canonical company fields already present

The Nashville run showed:

* expansion found docs
* apply completed
* `applied = 0`
* contact discovery still produced leads downstream. 

### E. Contact discovery scanned fewer companies than companies in run

This is expected when only companies with websites are eligible for website contact discovery. In the Nashville example, the run had 36 companies but contact discovery scanned 31 because only website-bearing companies were eligible.

### F. Wrong ETL branch suspected

Use `run_web_etl_with_metrics` or the orchestrator logs and confirm that the router prints:

* `inferred_source=web_ingestion`
* `routing web flow for run_id=...`

That confirms the web/company branch is active. 

### G. Legacy ETL path confusion

If someone starts using:

* `etl_raw_to_clean.py`
* `etl_rawCompanies_to_cleanCompanies.py`

stop and redirect them to:

* `run_web_company_end_to_end`
* or the stepwise debug path documented above. 

---

## 11. Current Mental Model

1. Pick a preset with `ACTIVE_SCRAPER_PRESET`
2. Run ingestion or run the one-command orchestrator
3. If ingestion already succeeded, resume from `run_id`
4. Web ETL writes canonical companies
5. Expansion writes raw company enrichment docs
6. Expansion apply safely updates canonical companies
7. Contact discovery finds leads from company websites
8. Validate by `run_id`
9. Treat raw cleanup as best-effort until the helper is fixed

If you want this turned into a downloadable `.md` file, I can generate it.
