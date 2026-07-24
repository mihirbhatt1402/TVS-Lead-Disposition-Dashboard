"""
TVS Dashboard Payload Cross-Validator
Run after a pipeline push to verify all matrix counts are consistent.
Usage: python validate_payload.py
"""
import json, sys
from pathlib import Path
from collections import defaultdict

PAYLOAD_PATH = Path(__file__).parent / 'tvs_last_payload.json'

def load():
    with open(PAYLOAD_PATH, encoding='utf-8') as f:
        return json.load(f)

def agg_by_last_dim(rows, val_idx=1):
    """Sum val at val_idx grouped by last dimension index."""
    d = defaultdict(int)
    for row in rows:
        d[row[-5 + val_idx]] += row[val_idx]   # generic – not used
    return d

def totals(rows):
    """Return (total_leads, total_retails, total_dms, total_call) across all rows."""
    L = R = D = C = 0
    for row in rows:
        L += row[-4]; R += row[-3]; D += row[-2]; C += row[-1]
    return L, R, D, C

def by_month(rows, month_dim=-4):
    """Return {month_idx: [leads, rets, dms, call]} summed per month."""
    d = defaultdict(lambda: [0,0,0,0])
    for row in rows:
        mi = row[month_dim]
        d[mi][0] += row[-4]
        d[mi][1] += row[-3]
        d[mi][2] += row[-2]
        d[mi][3] += row[-1]
    return d

def check(name, a, b, label=''):
    if a != b:
        print(f"  FAIL  {name}{(' '+label) if label else ''}: {a:,} != {b:,}")
        return False
    return True

def run():
    print(f"Loading {PAYLOAD_PATH}…")
    p = load()
    maps = p['maps']
    lm_arr = maps['lm']
    src_arr = maps.get('src', [])
    mdl_arr = maps.get('mdl', [])
    st_arr  = maps.get('st', [])
    lt_arr  = maps.get('lt', [])
    n_months = len(lm_arr)

    print(f"Months ({n_months}): {lm_arr}")
    print(f"Sources: {src_arr}")
    print(f"Models ({len(mdl_arr)}): {sorted(set(mdl_arr))}")
    print()

    errors = 0

    # ── 1. Grand totals consistent across all regular matrices ───────────────
    print("=== 1. Grand total consistency (regular mode) ===")
    ref_L, ref_R, ref_D, ref_C = totals(p['monthly'])
    print(f"  monthly  →  leads={ref_L:,}  rets={ref_R:,}  dms={ref_D:,}  call={ref_C:,}")

    for key in ['sm','ltm','mm','stm','mxst','mlt','stlt','zm','stcm','univ']:
        if key not in p: continue
        L,R,D,C = totals(p[key])
        ok = check(key, L, ref_L, 'leads') and check(key, R, ref_R, 'rets') and \
             check(key, D, ref_D, 'dms')   and check(key, C, ref_C, 'call')
        if ok:
            print(f"  {key:<8} OK  leads={L:,}  rets={R:,}")
        else:
            errors += 1

    # bdm: month is last dim; leads/rets/dms/call at positions -4..-1
    if 'bdm' in p:
        L,R,D,C = totals(p['bdm'])
        ok = check('bdm', L, ref_L, 'leads') and check('bdm', R, ref_R, 'rets')
        if ok: print(f"  bdm      OK  leads={L:,}  rets={R:,}")
        else: errors += 1

    # cm / csm
    for key in ['cm','csm']:
        if key not in p: continue
        L,R,D,C = totals(p[key])
        ok = check(key, L, ref_L, 'leads') and check(key, R, ref_R, 'rets')
        if ok: print(f"  {key:<8} OK  leads={L:,}  rets={R:,}")
        else: errors += 1

    # dealer matrices (optional)
    for key in ['cdm','cdsm','stdm','mxdl','ltdl']:
        if key not in p: continue
        L,R,D,C = totals(p[key])
        ok = check(key, L, ref_L, 'leads') and check(key, R, ref_R, 'rets')
        if ok: print(f"  {key:<8} OK  leads={L:,}  rets={R:,}")
        else: errors += 1

    # ── 2. DMS + Call Out ≤ Retail for every row ────────────────────────────
    print("\n=== 2. DMS + Call Out ≤ Retail (row-level) ===")
    dms_violations = []
    for key in ['monthly','sm','ltm','mm','stm','mxst','mlt','stlt','zm','bdm','cm','csm',
                'cdm','cdsm','stdm','mxdl','ltdl']:
        if key not in p: continue
        for row in p[key]:
            l,r,d,c = row[-4], row[-3], row[-2], row[-1]
            if d + c > r:
                dms_violations.append(f"{key} row={row}: dms={d}+call={c}={d+c} > rets={r}")
    if dms_violations:
        print(f"  FAIL  {len(dms_violations)} violations:")
        for v in dms_violations[:10]:
            print(f"    {v}")
        errors += 1
    else:
        print("  OK  All rows: dms+call <= retail")

    # ── 3. Update-mode grand totals ──────────────────────────────────────────
    print("\n=== 3. Update-mode grand totals ===")
    u_L, u_R, u_D, u_C = totals(p['u_monthly'])
    print(f"  u_monthly → leads={u_L:,}  rets={u_R:,}  dms={u_D:,}  call={u_C:,}")

    # Lead total must equal regular mode (same leads, just retail month may shift)
    ok = check('u_monthly', u_L, ref_L, 'leads (must match regular)')
    if not ok: errors += 1
    else: print("  Lead total matches regular mode ✓")

    # Retail total must equal regular mode
    ok = check('u_monthly', u_R, ref_R, 'rets (must match regular)')
    if not ok: errors += 1
    else: print("  Retail total matches regular mode ✓")

    for key in ['u_sm','u_ltm','u_mm','u_stm','u_mxst','u_mlt','u_stlt','u_zm','u_stcm','u_univ']:
        if key not in p: continue
        L,R,D,C = totals(p[key])
        ok = check(key, L, u_L, 'leads') and check(key, R, u_R, 'rets') and \
             check(key, D, u_D, 'dms')   and check(key, C, u_C, 'call')
        if ok: print(f"  {key:<10} OK  leads={L:,}  rets={R:,}")
        else: errors += 1

    if 'u_bdm' in p:
        L,R,D,C = totals(p['u_bdm'])
        ok = check('u_bdm', L, u_L, 'leads') and check('u_bdm', R, u_R, 'rets')
        if ok: print(f"  u_bdm      OK  leads={L:,}  rets={R:,}")
        else: errors += 1

    for key in ['u_stdm','u_mxdl','u_ltdl']:
        if key not in p: continue
        L,R,D,C = totals(p[key])
        ok = check(key, L, u_L, 'leads') and check(key, R, u_R, 'rets')
        if ok: print(f"  {key:<10} OK")
        else: errors += 1

    # ── 4. Per-month lead counts: regular vs update (must match) ────────────
    print("\n=== 4. Per-month lead count: regular == update ===")
    reg_by_month = by_month(p['monthly'], month_dim=0)  # monthly: [li, l, r, d, c]
    upd_by_month = by_month(p['u_monthly'], month_dim=0)
    month_lead_ok = True
    for li in range(n_months):
        lm = lm_arr[li]
        reg_l = reg_by_month[li][0]
        upd_l = upd_by_month[li][0]
        if reg_l != upd_l:
            print(f"  FAIL  {lm}: regular leads={reg_l:,} != update leads={upd_l:,}")
            month_lead_ok = False
            errors += 1
    if month_lead_ok:
        print("  OK  All months: lead counts match between regular and update mode")

    # ── 5. Per-month retail: update mode redistribution check ───────────────
    print("\n=== 5. Per-month retail distribution (create vs update) ===")
    header = f"  {'Month':<12} {'Reg Leads':>10} {'Reg Rets':>10} {'Upd Leads':>10} {'Upd Rets':>10}"
    print(header)
    for li in range(n_months):
        lm = lm_arr[li]
        rl = reg_by_month[li][0]
        rr = reg_by_month[li][1]
        ul = upd_by_month[li][0]
        ur = upd_by_month[li][1]
        flag = '  ← retail shifted' if rr != ur else ''
        print(f"  {lm:<12} {rl:>10,} {rr:>10,} {ul:>10,} {ur:>10,}{flag}")

    # ── 6. Retail dispersion totals match ────────────────────────────────────
    print("\n=== 6. Retail dispersion totals ===")
    if 'disp' in p:
        disp_total = sum(row[-1] for row in p['disp'])
        ok = check('disp', disp_total, ref_R, 'total retails')
        if ok: print(f"  disp total={disp_total:,} matches retail total ✓")
        else: errors += 1
    if 'u_disp' in p:
        u_disp_total = sum(row[-1] for row in p['u_disp'])
        ok = check('u_disp', u_disp_total, u_R, 'update retail total')
        if ok: print(f"  u_disp total={u_disp_total:,} matches update retail total ✓")
        else: errors += 1

    # ── 7. Model names: only canonical names allowed ──────────────────────────
    print("\n=== 7. Model names in maps ===")
    CANONICAL = {
        'APACHE RTR 165','TVS Apache RR 310','TVS Apache RTR 160','TVS Apache RTR 160 4V',
        'TVS Apache RTR 180','TVS Apache RTR 200 4V','TVS Apache RTR 310','TVS Jupiter',
        'TVS Jupiter 125','TVS NTORQ 125','TVS Radeon','TVS Raider','TVS Ronin',
        'TVS Scooty Pep Plus','TVS Scooty Zest','TVS Sport','TVS Star City Plus',
        'TVS XL100','TVS iQube','Unknown',
    }
    non_canonical = [m for m in mdl_arr if m not in CANONICAL]
    if non_canonical:
        print(f"  WARN  {len(non_canonical)} non-canonical model names:")
        for m in sorted(non_canonical):
            print(f"    {repr(m)}")
    else:
        print(f"  OK  All {len(mdl_arr)} model entries are canonical")

    # ── 8. Source names ───────────────────────────────────────────────────────
    print("\n=== 8. Source names ===")
    EXPECTED_SOURCES = {'Facebook','Organic','Google','Non CPS','Unknown'}
    unexpected_src = [s for s in src_arr if s not in EXPECTED_SOURCES]
    if unexpected_src:
        print(f"  WARN  Unexpected sources: {unexpected_src}")
    else:
        print(f"  OK  Sources: {src_arr}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    if errors == 0:
        print(f"ALL CHECKS PASSED  —  {ref_L:,} leads  {ref_R:,} retails")
    else:
        print(f"FAILED: {errors} check(s) failed. Review output above.")
    print(f"{'='*50}")
    return errors

if __name__ == '__main__':
    sys.exit(run())
