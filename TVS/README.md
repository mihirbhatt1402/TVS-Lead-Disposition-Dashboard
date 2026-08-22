# TVS Lead Disposition Dashboard — Operations Manual

> **Audience**: Anyone who needs to operate, debug, or maintain this pipeline without prior context.
> **Last updated**: 2026-08-22 · Certified HEAD: `b5986f3`

---

## 1. Project Purpose

The TVS Lead Disposition Dashboard is a real-time analytics dashboard used by TVS Motor Company senior leadership to track how each sales lead was handled — created vs updated, DMS vs Call Out retail channel, per model / state / city / source / dealer. Data is refreshed daily at 11:30 AM IST via a GitHub Actions pipeline.

---

## 2. Architecture

```
GitHub Actions (cron 06:00 UTC = 11:30 IST)
    │
    ├─ [1/5] Load hist_cache.json.gz  (committed; covers Apr'25–Jun'26)
    │
    ├─ [2/5] Fetch live retail master  (Google Sheets via Apps Script)
    │          └─ Three-way merge with hist retail map
    │
    ├─ [3/5] Historical leads  (already loaded in step 1)
    │
    ├─ [4/5] Fetch live lead sheets  (7 GSheets via Apps Script, Jul'26+)
    │          └─ extract_rtype_map → rtype_map override
    │
    └─ [5/5] Merge → Aggregate → Validate → Atomic publish
               ├─ _validate_payload() — DMS+CO == Retails for live months
               ├─ Write staging file
               ├─ Staging readback
               ├─ Firebase POST (tvs_payload.json.gz)
               ├─ Back up previous payload → tvs_payload_prev.json.gz
               └─ Promote staging → tvs_payload.json.gz (git commit + push)
```

**Key files:**

| Path | Purpose |
|---|---|
| `TVS/push_tvs_data.py` | Complete pipeline (~2,470 lines). Single-file architecture. |
| `TVS/hist_cache.json.gz` | Committed frozen cache: Apr'25–Jun'26 leads + retail map. |
| `TVS/source_metrics.json` | Previous-run row counts for change-detection (written after each successful run). |
| `data/tvs_payload.json.gz` | Current production payload (served by GitHub Pages). |
| `data/tvs_payload_prev.json.gz` | Previous known-good payload (rollback target). |
| `TVS/test_pipeline.py` | 109-test regression suite (no network required). |
| `.github/workflows/tvs-data-push.yml` | Daily push workflow. |
| `.github/workflows/deploy.yml` | GitHub Pages deployment workflow. |
| `index.html` | Single-file React dashboard. Loads `tvs_payload.json.gz` at runtime. |

---

## 3. Data Sources

### Historical (frozen — never fetched at runtime)
- **`hist_cache.json.gz`** — committed to git. Contains:
  - `retail_map`: `{sourceLeadId → {rm, rtype, pm}}` for 317,962 retail entries (Jan'25–Jun'26)
  - `leads`: 2,634,996 lead rows (Apr'25–Jun'26)

### Live (fetched via Apps Script proxy every run)
- **Live retail master** — Google Sheet `RETAILS_FILE_ID`, tab `Raw`.
  Contains all retails (historical + current). Source of truth for Jul'26+ retail classification.
- **Live lead sheets** — defined in `LEAD_SHEETS` array:
  - `Jul'26-LeadMaster`: `min_mo=2607, max_mo=2607` (Jul'26 rows only)
  - `Aug26+-LeadMaster`: `min_mo=2607, max_mo=None` (Aug'26 onwards, no upper bound)

### Apps Script proxy
- URL: stored as `APPS_SCRIPT_URL` in `push_tvs_data.py`
- Authentication: `SECRET` parameter (value in code — do not expose publicly)
- Actions used: `getSheetData` (paginated, 10 000 rows/page), `getRetails`
- Retry: 3 attempts with escalating timeouts (30 s, 60 s, 180 s for leads; similar for retails)

---

## 4. Data Flow

### Lead processing

1. **Historical leads** loaded from `hist_cache.json.gz` (fixed, 2.6 M rows).
2. **Live lead sheets** fetched for Jul'26+ months. Each sheet goes through:
   - Column schema validation (`_LEAD_REQUIRED_COLS`)
   - `extract_rtype_map()` — extracts `{opty_id → {rm, rtype}}` from "Retail By" / "DMS_Retail_Month" columns
   - `standardize_leads()` — renames columns, derives LeadMonth from Date, title-cases State
   - Month filter (`min_mo` / `max_mo`)
3. **Merge and dedup**: historical + live combined, then `drop_duplicates(subset=['SorceLeadId'], keep='last')` — live wins on overlap.
4. **Live month presence check**: every month from Jul'26 through end of previous calendar month must be present. Hard-fail if any are missing.

### Retail processing

1. **Historical retail map** from `hist_cache.json.gz` — `{sourceLeadId → {rm, rtype, pm}}` covering Jan'25–Jun'26.
2. **Live retail master** fetched (all retails). Each entry goes through `build_retail_map()` to normalise `performanceMonth` → `rm` and `Call Type` → `rtype` ('DMS' or 'Call Out').
3. **Three-way merge**:
   - **Case A** (`rm >= Jul'26`): live retail sheet is authoritative. Overwrites hist entry.
   - **Case B** (`rm < Jul'26`, lid already in hist): hist `rtype` is preserved (Excel "DMS/Call Out" column is accurate for historical months; live Call Type uses "Purchased From" logic that mislabels historical DMS entries). Only `rm` is updated from the live sheet if it is a valid month >= Apr'25.
   - **Case C** (`rm < Jul'26`, lid NOT in hist): retail appeared after the Excel export. Added fully from the live sheet.
4. **rtype_map override**: after the three-way merge, for Jul'26+ retails, if the live lead sheet's "Retail By" column has a non-empty, valid classification (DMS or Call Out), it overrides the retail map's rtype. Pre-Jul'26 retails are never overwritten here.

### On Create (OC) vs On Update (OU)

- **On Create (`monthly`)**: lead counted in its `LeadMonth`; retail (if any) also counted in `LeadMonth`.
- **On Update (`u_monthly`)**: lead counted in its `LeadMonth`; retail counted in its `performanceMonth` (retail attribution month). These can differ.

---

## 5. City Normalization

`normalize_city(raw)` in `push_tvs_data.py`:

1. Strip whitespace, collapse multiple spaces, apply `str.title()`.
2. Exact lookup in `_CITY_ALIAS` dict.
3. If not found and the string contains `/`, `,`, `|`, or `&` (compound city string):
   - Split on separator, canonicalize each token, rejoin with original separator.
   - If all tokens resolve to the same canonical city, collapse to that city.
4. Fallback: return title-cased string as-is.

**Current aliases:**

| Source data | Canonical |
|---|---|
| Bengaluru, Bengalore | Bangalore |
| New Delhi | Delhi |
| Prayagraj | Allahabad |
| Thiruvananthapuram | Trivandrum |

**Compound examples:**
- `"Bengaluru / Bangalore"` → `"Bangalore"`
- `"Begur, Bengaluru"` → `"Begur, Bangalore"`
- `"New Delhi / Delhi"` → `"Delhi"`

To add a new alias: add a `'Title Cased Key': 'Canonical'` entry to `_CITY_ALIAS` in `push_tvs_data.py` and add a corresponding test in `test_pipeline.py`.

---

## 6. Payload Structure

The payload (`tvs_payload.json.gz`) is a compressed JSON object. Key sections:

```json
{
  "maps": {
    "lm":  ["Apr'25", "May'25", ...],   // month label array
    "src": ["Website", "Walk-in", ...], // source array
    "lt":  ["Hot", "Warm", ...],        // lead-type array
    "mdl": ["TVS Apache RTR 200 4V", ...],
    "st":  ["Maharashtra", ...],
    "city":["Mumbai", ...],
    "dl":  ["Dealer Name", ...]
  },
  "monthly":   [[lm_idx, leads, rets, dms, co], ...],  // On Create
  "u_monthly": [[lm_idx, leads, rets, dms, co], ...],  // On Update
  "sm":     [...],  // source × month
  "mm":     [...],  // model × month
  "stm":    [...],  // state × month
  ...
}
```

All dimension values are stored as integer indices into the `maps` arrays to minimise JSON size. The dashboard resolves indices at render time.

---

## 7. Validation Rules

### Source validation
- **Schema**: required columns checked before processing (`_LEAD_REQUIRED_COLS`, `_RETAIL_REQUIRED_COLS`). Missing column → `_fail_exit`.
- **Retail row count**: must be ≥ max(50 000, 80% of previous run). Uses `source_metrics.json` baseline. Missing baseline → absolute 50 000 floor.
- **Source drop**: each lead sheet row count must be ≥ 80% of previous run. Monitors for silent truncation.
- **Empty sheet**: 0 rows → hard fail.

### Payload validation (`_validate_payload`)
- **DMS + Call Out == Retails** for every live month (Jul'26+) in both OC and OU. Any gap → hard fail.
- **Historical months** (pre-Jul'26): unclassified retails are allowed (Excel data has known gaps).
- **Live month presence**: every month from Jul'26 through end of previous calendar month must have > 0 leads. Missing month → hard fail.
- **Reference cross-check**: prints Jul'26 OC retails vs reference value (14 182). Informational; drift > 500 triggers a `<-- DRIFT` flag but does not fail.

### Failure handler
`_fail_exit(stage, reason, stg_path=None)` — all fatal paths use this. It:
1. Prints a structured failure report to stderr (stage, reason, last known-good payload info).
2. If `stg_path` is provided and the staging file exists, names it for inspection.
3. Calls `sys.exit(1)`.
4. **Production payload is never modified** before this point.

---

## 8. Atomic Publication

```
Payload built → _validate_payload → write staging file
  → read back staging (structure check)
  → DRY RUN? stop and delete staging
  → Firebase POST (tvs_payload.json.gz upload)
  → Firebase success? → backup prod → tvs_payload_prev.json.gz
                      → copy staging → tvs_payload.json.gz  (production update)
                      → delete staging
  → Firebase fail?   → delete staging, _fail_exit (production unchanged)
```

**Invariant**: production (`tvs_payload.json.gz`) is never overwritten unless Firebase confirms success. The dashboard always serves either the new confirmed payload or the previous known-good one.

---

## 9. GitHub Actions

### `tvs-data-push.yml` — Daily pipeline
- **Schedule**: `0 6 * * *` (06:00 UTC = 11:30 AM IST)
- **Manual trigger**: `workflow_dispatch` with `dry_run` boolean input
- **Concurrency**: group `tvs-data-push`, `cancel-in-progress: false` (queues duplicate runs rather than cancelling)
- **Timeout**: 60 minutes (Apps Script fetch + processing comfortably fits within this)
- **Dry run**: passes `--dry-run` flag to Python; pipeline fetches, validates, but does NOT write to Firebase or git

### `deploy.yml` — GitHub Pages deployment
- Triggers on push to `main` or manual dispatch
- Copies `index.html` and `data/tvs_payload.json.gz` to `_site/`
- Does NOT run the data push; only deploys what is already committed

---

## 10. Dry-Run Procedure

```bash
# From the repo root (requires Python 3.11+, pandas, requests)
pip install pandas requests openpyxl pyxlsb

python TVS/push_tvs_data.py --dry-run
```

Or via GitHub Actions:
1. Go to Actions → "TVS Daily Data Push" → "Run workflow"
2. Set `dry_run = true`
3. Click "Run workflow"

A dry run:
- Fetches all live data
- Runs all validations
- Prints full reconciliation table
- Writes and reads back the staging file
- **Does NOT push to Firebase** or update `tvs_payload.json.gz`
- Deletes the staging file on completion
- Reports "DRY RUN COMPLETE — production unchanged"

---

## 11. Manual Production Run

> Only run this when explicitly authorised. The scheduled job runs automatically.

```bash
python TVS/push_tvs_data.py
```

Without `--dry-run`, the pipeline will publish if all validations pass.

To trigger via GitHub Actions (production):
1. Go to Actions → "TVS Daily Data Push" → "Run workflow"
2. Leave `dry_run = false` (default)
3. Click "Run workflow"

---

## 12. Diagnosing Failures

### Pipeline exited non-zero

1. Check GitHub Actions log: the `_fail_exit` block prints `Stage:` and `Reason:` to stderr.
2. Look for `TVS DATA PIPELINE — FAILED SAFELY` in the logs.
3. Production payload is unchanged — dashboard is still serving the previous known-good payload.

### Common failure messages

| Stage | Typical cause |
|---|---|
| `Retail schema validation` | GSheet column renamed or removed |
| `Retail fetch size validation` | Apps Script truncated the response; retry the job |
| `Lead schema — ...` | Lead sheet column renamed or missing |
| `Live month presence check` | A lead sheet returned empty data for a prior month |
| `Payload validation -- DMS+CO mismatch` | Unclassified retail entries — investigate `Retail By` column |
| `Firebase POST` | Firebase project quota or auth issue |
| `Staging readback` | Disk issue on the runner (rare) |

### Investigating unclassified retails

If `_validate_payload` fails with a DMS+CO mismatch:
1. Check the pipeline log for `NOTE: extract_rtype_map — unrecognized 'Retail By' values`.
2. Check for `WARNING: Unexpected Call Type` in the retail master fetch section.
3. The culprit is either a new "Retail By" value in the lead sheet (add to `extract_rtype_map` sentinel list or classification logic) or a new "Call Type" in the retail master (add to `build_retail_map`).

---

## 13. Recovery from Failure

### Pipeline failed mid-run

Production is unchanged. Simply re-run the pipeline after fixing the root cause.

### Production payload is corrupted / incorrect

1. The previous known-good payload is at `data/tvs_payload_prev.json.gz`.
2. Copy it back:
   ```bash
   cp data/tvs_payload_prev.json.gz data/tvs_payload.json.gz
   git add data/tvs_payload.json.gz
   git commit -m "rollback: restore prev known-good payload"
   git push
   ```
3. GitHub Pages will redeploy automatically on the next push to `main`.

### Staging file left behind

If a run crashed after writing the staging file but before promoting it, a `tvs_payload_staging_*.json.gz` file may be present in `data/`. It is safe to delete — it is never served as production.

---

## 14. Verifying a Successful Push

After each pipeline run, check:

1. **GitHub Actions**: green checkmark on the latest run of "TVS Daily Data Push".
2. **Log output** (last section):
   ```
   TVS DATA PIPELINE — SUCCESS
   ── Jul'26  OC Leads=191,541  OC Rets=14,182  ...
   ── Aug'26  OC Leads=...      OC Rets=...      ...
   Reference cross-check:
     Jul'26 On Create retails: 14,182  ref=14,182  diff=+0
   ```
3. **Dashboard**: open the live URL, check that Jul'26 and Aug'26 totals are present.
4. **Payload file**: `data/tvs_payload.json.gz` commit timestamp matches the run date.

---

## 15. Handling a New Month

When Sep'26 data begins appearing in CRM:

1. **No code change required.** The `Aug26+-LeadMaster` sheet has `max_mo=None` — it already covers Sep'26 and every subsequent month.
2. The live-month presence check will automatically add Sep'26 to `_prior_live_months` once Aug'26 is a "closed" prior month (i.e. once the calendar rolls to Sep'26).
3. The `_validate_payload` DMS+CO check will automatically enforce correctness for Sep'26.
4. The REF dict in `_validate_payload` is informational only — no update required for new months.

**When does a code change become necessary?**
- TVS migrates to a new CRM Google Sheet for a future month period → add a new entry to `LEAD_SHEETS` in `push_tvs_data.py`.
- A genuinely new city alias appears in data → add to `_CITY_ALIAS`.
- A new TVS model variant appears in the retail master → add to `PURCHASED_MODEL_MAP`.

---

## 16. Adding a New Source Sheet

Add an entry to `LEAD_SHEETS` in `push_tvs_data.py`:

```python
LEAD_SHEETS = [
    ...,
    {
        'id':     '<Google Sheet ID>',
        'tab':    'TVS',        # tab name within the sheet
        'label':  "Sep'26+-LeadMaster",
        'min_mo': 2609,         # Sep'26 = 2609
        'max_mo': None,         # no upper bound (or set a specific value)
    },
]
```

Ensure `min_mo` does not overlap with an existing sheet's range unless deduplication can resolve the overlap (it can — live data wins via `keep='last'`).

---

## 17. Running Tests

```bash
python TVS/test_pipeline.py -v
```

All 109 tests run in < 1 second. No network access, no credentials, no data files required.

Tests cover: city normalization, retail classification, DMS+CO invariant, three-way merge Cases A/B/C, month ordering, year-boundary rollover, deduplication, atomic publication safety, retail fetch validation (dynamic floor), future-data extensibility, and source validation.

---

## 18. Known Limitations

| Limitation | Classification | Notes |
|---|---|---|
| CDN dependencies in dashboard (unpkg, jsdelivr, cdn.sheetjs.com) | Safe technical debt | Acceptable for internal dashboard. If CDN is down, dashboard is inaccessible but data is unaffected. |
| `BuyingDays` is always `"0"` in the payload | Intentional design limitation | Source data does not provide buying days. Dashboard field exists for future use. |
| Admin emails hard-coded in `index.html` | Intentional design limitation | Used as the access-control list for the admin panel. Dashboard is auth-gated (GirnarSoft email required). |
| Apps Script URL visible in `push_tvs_data.py` | Acceptable | URL alone is insufficient for write access. SECRET is required for all data-modifying calls. Repo should remain private. |
| Unexpected `Call Type` in retail master defaults to DMS | Should monitor | If TVS adds a new Call Type variant, it is silently classified as DMS. The WARNING is visible in pipeline logs. Investigate and update `build_retail_map` if a new value appears. |
| `hist_cache.json.gz` is frozen (Apr'25–Jun'26 only) | By design | Historical data is permanently frozen. Live data (Jul'26+) overwrites via the three-way merge. No mechanism to update the cache without a local re-bootstrap. |

---

## 19. Ownership

- **Pipeline**: `TVS/push_tvs_data.py` — maintained by the Analytics team
- **Dashboard**: `index.html` — maintained by the Analytics team
- **GitHub repo**: `mihirbhatt1402/TVS-Lead-Disposition-Dashboard` (private)
- **Firebase project**: `tvs-analytics` (or equivalent — see APPS_SCRIPT_URL for the connected project)
- **GitHub Pages URL**: `https://mihirbhatt1402.github.io/TVS-Lead-Disposition-Dashboard/`

Do not store credentials, secrets, or tokens in this README or in any committed file.
