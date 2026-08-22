"""
Standalone July 2026 TVS Lead Master source verification.
Independently fetches the Jul'26-LeadMaster GSheet via Apps Script and verifies:
  - Total rows fetched vs Apps Script total
  - LeadMonth distribution
  - Duplicate opty_id count
  - Valid Jul'26 lead count
  - Cross-check against expected payload count (190,631)

Does NOT import or depend on push_tvs_data.py.
Run: python TVS/verify_jul26_source.py
"""
import sys
import io
import json
import time
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ── Config (same as push_tvs_data.py) ─────────────────────────────────────────
APPS_SCRIPT_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbwzgnXPbCbunBblnMUrqdWg3eY9qsIwCrFxuYuvYSpxtH22l4Cs32vdkOkDhUn-qwM64w/exec"
)
SECRET         = "tvs2026push"
JUL26_SHEET_ID = '1gaRoPLebv7jaBgWEET-XSQuhqE_XgQlGru39TA-FoSo'
JUL26_TAB      = 'TVS'
PAGE_SIZE      = 10_000

# Expected values from dry-run 4 payload
EXPECTED_RAW_TOTAL    = 280_440   # Apps Script reported total
EXPECTED_JUL26_LEADS  = 190_631   # valid Jul'26 rows post-filter
EXPECTED_BLANK_ROWS   = 89_791    # blank / unresolvable LeadMonth
EXPECTED_JUN26_ROWS   = 18        # Jun'26 rows (out-of-scope)

# ── Helpers ────────────────────────────────────────────────────────────────────
MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

def parse_ym(raw):
    """Return 'Mon'YY' string or '' if unparseable."""
    if not raw or not str(raw).strip():
        return ''
    s = str(raw).strip()
    # "Jul'2026", "Jul'26", "Jul-26", "Jul 2026" etc.
    import re
    m = re.match(r"([A-Za-z]{3})['\- ]?(\d{2,4})$", s)
    if m:
        mon, yr = m.group(1).capitalize(), m.group(2)
        yr2 = yr[-2:]
        if mon in MONTH_NAMES:
            return f"{mon}'{yr2}"
    return ''

def fetch_all_pages(sheet_id, tab, label, timeout_seq=(60, 90, 180)):
    """Fetch all pages from the Apps Script proxy (action=getSheetData).
    Returns (list_of_row_lists, headers, apps_total)."""
    all_rows   = []
    headers    = None
    apps_total = None
    page       = 0
    COLS       = 'opty_id,Lead_Month,Date,model,City,State,Dealer_Name,lead_type,Medium,Retail By,DMS_Retail_Month'

    while True:
        params = {
            'secret':   SECRET,
            'action':   'getSheetData',
            'fileId':   sheet_id,
            'tabName':  tab,
            'page':     page,
            'pageSize': PAGE_SIZE,
            'cols':     COLS,
        }
        for attempt, timeout in enumerate(timeout_seq):
            try:
                resp = requests.get(APPS_SCRIPT_URL, params=params, timeout=timeout)
                resp.raise_for_status()
                data = resp.json()
                if 'error' in data:
                    raise RuntimeError(f"Apps Script error: {data['error']}")
                break
            except Exception as e:
                if attempt < len(timeout_seq) - 1:
                    print(f"  [{label}] page {page} attempt {attempt+1} failed ({e}), "
                          f"retrying in 30s…", flush=True)
                    time.sleep(30)
                else:
                    print(f"  [{label}] page {page} FAILED after {len(timeout_seq)} attempts: {e}",
                          flush=True)
                    raise

        if headers is None:
            headers = data.get('headers', [])
        rows  = data.get('rows', [])
        done  = data.get('done', True)
        total = data.get('total')
        if total is not None and apps_total is None:
            apps_total = total

        all_rows.extend(rows)
        running = len(all_rows)
        print(f"  [{label}] page {page}: +{len(rows):,} rows  "
              f"(running {running:,}/{apps_total or '?'})", flush=True)

        if done or len(rows) == 0:
            print(f"  [{label}] done received at page {page}.", flush=True)
            break
        page += 1

    return all_rows, headers, apps_total


def main():
    print("=" * 64, flush=True)
    print("TVS Jul'26 Lead Master — Independent Source Verification", flush=True)
    print("=" * 64, flush=True)

    # ── Fetch ──────────────────────────────────────────────────────────────────
    print(f"\nFetching Jul'26-LeadMaster (id={JUL26_SHEET_ID}, tab={JUL26_TAB})…", flush=True)
    rows, headers, apps_total = fetch_all_pages(JUL26_SHEET_ID, JUL26_TAB, "Jul'26")

    # ── Basic counts ───────────────────────────────────────────────────────────
    fetched_total = len(rows)
    print(f"\n── Raw fetch results ────────────────────────────────────────", flush=True)
    print(f"  Apps Script total:       {apps_total or '(not reported)':>10}", flush=True)
    print(f"  Rows fetched:            {fetched_total:>10,}", flush=True)

    # ── Identify key columns ───────────────────────────────────────────────────
    if not rows:
        print("  ERROR: 0 rows returned. Cannot validate.", flush=True)
        sys.exit(1)

    # Rows are lists; headers is the column name array.
    print(f"  Headers from API:        {headers}", flush=True)

    def col_idx(headers_list, candidates):
        lc = {h.lower(): i for i, h in enumerate(headers_list)}
        for c in candidates:
            if c.lower() in lc:
                return lc[c.lower()], headers_list[lc[c.lower()]]
        return None, None

    lm_idx, lead_month_col = col_idx(
        headers, ['lead_month', 'leadmonth', 'lead month', 'lead_mth'])
    oid_idx, opty_id_col   = col_idx(
        headers, ['opty_id', 'optyid', 'opportunity_id', 'sorce_lead_id',
                  'sorcelead_id', 'lead_id'])
    print(f"  Detected LeadMonth col:  {lead_month_col!r} (index {lm_idx})", flush=True)
    print(f"  Detected opty_id col:    {opty_id_col!r}   (index {oid_idx})", flush=True)

    # ── LeadMonth distribution ─────────────────────────────────────────────────
    lm_dist: dict = {}
    parsed_dist: dict = {}
    opty_ids: list = []

    for r in rows:
        if not isinstance(r, list):
            continue
        raw_lm = r[lm_idx] if lm_idx is not None and lm_idx < len(r) else ''
        lm_dist[raw_lm] = lm_dist.get(raw_lm, 0) + 1

        parsed = parse_ym(raw_lm)
        parsed_dist[parsed] = parsed_dist.get(parsed, 0) + 1

        if oid_idx is not None:
            opty_ids.append(r[oid_idx] if oid_idx < len(r) else '')

    print(f"\n── Raw LeadMonth distribution ───────────────────────────────", flush=True)
    for val, cnt in sorted(lm_dist.items(), key=lambda x: -x[1]):
        mark = '✓' if cnt == EXPECTED_JUL26_LEADS else ''
        print(f"  {str(val)!r:30s}: {cnt:>8,}  {mark}", flush=True)

    print(f"\n── Parsed LeadMonth distribution ────────────────────────────", flush=True)
    for val, cnt in sorted(parsed_dist.items(), key=lambda x: -x[1]):
        print(f"  {val!r:15s}: {cnt:>8,}", flush=True)

    # ── Duplicate analysis ─────────────────────────────────────────────────────
    print(f"\n── Duplicate analysis ───────────────────────────────────────", flush=True)
    if opty_ids:
        non_blank_ids = [i for i in opty_ids if i and str(i).strip()]
        unique_ids    = set(non_blank_ids)
        dup_count     = len(non_blank_ids) - len(unique_ids)
        blank_id_cnt  = len(opty_ids) - len(non_blank_ids)
        print(f"  Total opty_id values:    {len(opty_ids):>10,}", flush=True)
        print(f"  Blank / null opty_ids:   {blank_id_cnt:>10,}", flush=True)
        print(f"  Non-blank opty_ids:      {len(non_blank_ids):>10,}", flush=True)
        print(f"  Unique opty_ids:         {len(unique_ids):>10,}", flush=True)
        print(f"  Duplicates:              {dup_count:>10,}  {'✓' if dup_count == 0 else '⚠ WARN'}", flush=True)
    else:
        print("  opty_id column not found — skipping duplicate check.", flush=True)
        dup_count = -1

    # ── Validation summary ─────────────────────────────────────────────────────
    jul26_count  = parsed_dist.get("Jul'26", 0)
    blank_count  = parsed_dist.get('', 0)
    jun26_count  = parsed_dist.get("Jun'26", 0)

    print(f"\n── Verification against expected values ─────────────────────", flush=True)

    def check(label, actual, expected, allow_none=False):
        if allow_none and expected is None:
            print(f"  {label:40s}: {actual:>10,}  (no reference)", flush=True)
            return True
        match = actual == expected
        mark  = '✓ PASS' if match else f'✗ FAIL  (expected {expected:,})'
        print(f"  {label:40s}: {actual:>10,}  {mark}", flush=True)
        return match

    results = [
        check("Apps Script total rows",     apps_total or 0,     EXPECTED_RAW_TOTAL),
        check("Total fetched rows",          fetched_total,        EXPECTED_RAW_TOTAL),
        check("Jul'26 valid leads",          jul26_count,          EXPECTED_JUL26_LEADS),
        check("Blank / unresolvable rows",   blank_count,          EXPECTED_BLANK_ROWS),
        check("Jun'26 out-of-scope rows",    jun26_count,          EXPECTED_JUN26_ROWS),
    ]
    if dup_count >= 0:
        results.append(check("Duplicate opty_ids",           dup_count, 0))

    # ── Final verdict ──────────────────────────────────────────────────────────
    all_pass = all(results)
    print(f"\n── Final verdict ────────────────────────────────────────────", flush=True)
    if all_pass:
        print(f"  SOURCE VALIDATION PASSED", flush=True)
        print(f"  Jul'26 independent row count: {jul26_count:,} (matches expected 190,631)", flush=True)
    else:
        print(f"  SOURCE VALIDATION FAILED — see mismatches above.", flush=True)
    print("=" * 64, flush=True)
    sys.exit(0 if all_pass else 1)


if __name__ == '__main__':
    main()
