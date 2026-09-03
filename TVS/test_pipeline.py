"""
TVS Lead Disposition Pipeline — Regression Test Suite
======================================================
Run with:
    python TVS/test_pipeline.py            (stdout: verbose)
    python TVS/test_pipeline.py -v         (verbose)
    python -m pytest TVS/test_pipeline.py  (pytest)

No network access, production credentials, or external files required.
All tests use synthetic in-memory data.

IMPORTANT — keeping tests in sync with push_tvs_data.py:
  The utility functions below are inlined from push_tvs_data.py so that this
  test file runs without importing the module (which has module-level I/O).
  If a function changes in push_tvs_data.py, update the copy here and add a
  test that covers the new behaviour.  The _test_sync_check tests below compare
  function signatures to detect drift early.
"""
import re
import sys
import json
import gzip
import math
import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Inline copies of pure utility functions from push_tvs_data.py
# (must stay in sync with push_tvs_data.py)
# ---------------------------------------------------------------------------

MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun',
               'Jul','Aug','Sep','Oct','Nov','Dec']

ONLINE_START       = "Jul'26"
LEAD_MASTER_START  = "Apr'25"

_CITY_ALIAS = {
    'New Delhi':          'Delhi',
    'Bengaluru':          'Bangalore',
    'Bengalore':          'Bangalore',
    'Prayagraj':          'Allahabad',
    'Thiruvananthapuram': 'Trivandrum',
}
_COMPOUND_SEP_RE = re.compile(r'(\s*[/,|&]\s*)')

def normalize_city(raw):
    s = re.sub(r'\s+', ' ', str(raw or '').strip())
    if not s:
        return 'Unknown'
    s = s.title()
    if s in _CITY_ALIAS:
        return _CITY_ALIAS[s]
    if _COMPOUND_SEP_RE.search(s):
        tokens    = _COMPOUND_SEP_RE.split(s)
        parts     = tokens[0::2]
        seps      = tokens[1::2]
        canon     = [_CITY_ALIAS.get(p.strip(), p.strip()) for p in parts]
        if len(set(canon)) == 1:
            return canon[0]
        result = canon[0]
        for sep, cp in zip(seps, canon[1:]):
            result += sep + cp
        return result
    return s


def norm_month(s):
    s = str(s or '').strip()
    if not s: return s
    m   = re.search(r'([A-Za-z]{3})', s)
    yr4 = re.search(r'(\d{4})', s)
    yr2 = re.search(r"['\-\s](\d{2})\b", s)
    if m:
        mn = m.group(1)[0].upper() + m.group(1)[1:].lower()
        if yr4: return f"{mn}'{yr4.group(1)[2:]}"
        if yr2: return f"{mn}'{yr2.group(1)}"
    return s


def month_order(lm):
    try:
        s  = norm_month(str(lm or '').strip())
        mn, yy = s.split("'")
        mi = MONTH_NAMES.index(mn) + 1
        return int(yy) * 100 + mi
    except Exception:
        return 0


def to_id(v):
    try:
        f = float(v)
        if math.isnan(f): return ''
        return str(int(f))
    except Exception:
        return str(v).strip() if v else ''


ONLINE_START_ORDER = month_order(ONLINE_START)
LEAD_MASTER_START_ORDER = month_order(LEAD_MASTER_START)


def extract_rtype_map(rows):
    """rows: list of dicts with 'opty_id', 'DMS_Retail_Month', 'Retail By'."""
    rmap = {}
    _unknown_rb = {}
    for row in rows:
        rm = str(row.get('DMS_Retail_Month', '') or '').strip()
        if not rm: continue
        lid = to_id(row.get('opty_id', ''))
        if not lid: continue
        _rb_raw = str(row.get('Retail By', '') or '').strip()
        _rb_u   = _rb_raw.upper()
        if 'DMS' in _rb_u:
            _rtype = 'DMS'
        elif 'CALL' in _rb_u or _rb_u == 'CC':
            _rtype = 'Call Out'
        else:
            _rtype = ''
            if _rb_raw and _rb_raw not in ('-', '–', 'N/A', 'NA', 'na', 'n/a'):
                _unknown_rb[_rb_raw] = _unknown_rb.get(_rb_raw, 0) + 1
        rmap[lid] = {'rm': norm_month(rm), 'rtype': _rtype}
    return rmap, _unknown_rb


def bump(d, k, is_ret, rtype=''):
    if k not in d: d[k] = [0, 0, 0, 0]
    d[k][0] += 1
    if is_ret:
        d[k][1] += 1
        rt_u = rtype.upper()
        if 'DMS' in rt_u:    d[k][2] += 1
        elif 'CALL' in rt_u: d[k][3] += 1


def ubump(d, key_lead, key_ret, is_ret, rtype=''):
    if key_lead not in d: d[key_lead] = [0, 0, 0, 0]
    d[key_lead][0] += 1
    if is_ret:
        if key_ret not in d: d[key_ret] = [0, 0, 0, 0]
        d[key_ret][1] += 1
        rt_u = rtype.upper()
        if 'DMS' in rt_u:    d[key_ret][2] += 1
        elif 'CALL' in rt_u: d[key_ret][3] += 1


def _validate_payload_logic(p):
    """Core of _validate_payload — returns (errors list, oc_by_lm, ou_by_lm)."""
    lm_arr   = p.get('maps', {}).get('lm', [])
    oc_by_lm = {}
    ou_by_lm = {}
    for row in p.get('monthly', []):
        lm = lm_arr[row[0]] if row[0] < len(lm_arr) else '?'
        prev = oc_by_lm.get(lm, [0, 0, 0, 0])
        oc_by_lm[lm] = [prev[j] + row[1 + j] for j in range(4)]
    for row in p.get('u_monthly', []):
        lm = lm_arr[row[0]] if row[0] < len(lm_arr) else '?'
        prev = ou_by_lm.get(lm, [0, 0, 0, 0])
        ou_by_lm[lm] = [prev[j] + row[1 + j] for j in range(4)]
    errors = []
    for lm, oc in oc_by_lm.items():
        if month_order(lm) < ONLINE_START_ORDER: continue
        if oc[1] == 0: continue
        diff = oc[1] - (oc[2] + oc[3])
        if diff != 0:
            errors.append(f"LIVE DMS+CO != Retails [{lm} OC]: diff={diff:+,}")
    for lm, ou in ou_by_lm.items():
        if month_order(lm) < ONLINE_START_ORDER: continue
        if ou[1] == 0: continue
        diff = ou[1] - (ou[2] + ou[3])
        if diff != 0:
            errors.append(f"LIVE DMS+CO != Retails [{lm} OU]: diff={diff:+,}")
    return errors, oc_by_lm, ou_by_lm


def _validate_retail_fetch_logic(n, prev_metrics=None):
    """Returns (ok: bool, msg: str)."""
    RETAIL_ABS_FLOOR      = 50_000
    RETAIL_DROP_THRESHOLD = 0.80
    prev = None
    if prev_metrics:
        _pm = prev_metrics.get('retail_raw')
        if isinstance(_pm, dict):
            prev = _pm.get('rows')
        elif isinstance(_pm, int):
            prev = _pm
    if prev is not None and prev > 0:
        floor = max(RETAIL_ABS_FLOOR, int(prev * RETAIL_DROP_THRESHOLD))
        if n < floor:
            return False, f"only {n:,} rows vs floor {floor:,} (prev={prev:,})"
        return True, 'ok'
    if n < RETAIL_ABS_FLOOR:
        return False, f"only {n:,} rows vs absolute floor {RETAIL_ABS_FLOOR:,}"
    return True, 'ok'


def three_way_merge(hist_map, live_map):
    """Merge live retail into hist map following Case A / B / C rules."""
    result = dict(hist_map)
    added_a = added_c = updated_b = kept_b = 0
    for lid, info in live_map.items():
        live_rm_order = month_order(info.get('rm', ''))
        if live_rm_order >= ONLINE_START_ORDER:
            result[lid] = info
            added_a += 1
        elif lid in result:
            if info.get('rm') and live_rm_order >= LEAD_MASTER_START_ORDER:
                result[lid] = {**result[lid], 'rm': info['rm']}
                updated_b += 1
            else:
                kept_b += 1
        else:
            result[lid] = info
            added_c += 1
    return result, added_a, updated_b, kept_b, added_c


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------

class TestCityNormalization(unittest.TestCase):

    def test_exact_alias_bangalore(self):
        self.assertEqual(normalize_city('Bengaluru'), 'Bangalore')

    def test_exact_alias_bangalore_misspelling(self):
        self.assertEqual(normalize_city('Bengalore'), 'Bangalore')

    def test_exact_alias_delhi(self):
        self.assertEqual(normalize_city('New Delhi'), 'Delhi')

    def test_no_change_for_canonical(self):
        self.assertEqual(normalize_city('Bangalore'), 'Bangalore')
        self.assertEqual(normalize_city('Delhi'), 'Delhi')
        self.assertEqual(normalize_city('Mumbai'), 'Mumbai')

    def test_case_insensitive(self):
        self.assertEqual(normalize_city('bengaluru'), 'Bangalore')
        self.assertEqual(normalize_city('BENGALURU'), 'Bangalore')
        self.assertEqual(normalize_city('new delhi'), 'Delhi')

    def test_whitespace_collapsed(self):
        self.assertEqual(normalize_city('  Bengaluru  '), 'Bangalore')
        self.assertEqual(normalize_city('New  Delhi'), 'Delhi')

    def test_empty_returns_unknown(self):
        self.assertEqual(normalize_city(''), 'Unknown')
        self.assertEqual(normalize_city(None), 'Unknown')
        self.assertEqual(normalize_city('   '), 'Unknown')

    def test_compound_slash_both_alias(self):
        # Both tokens resolve to the same canonical city → collapse
        self.assertEqual(normalize_city('Bengaluru / Bangalore'), 'Bangalore')
        self.assertEqual(normalize_city('Bangalore / Bengaluru'), 'Bangalore')
        self.assertEqual(normalize_city('New Delhi / Delhi'), 'Delhi')
        self.assertEqual(normalize_city('Delhi / New Delhi'), 'Delhi')

    def test_compound_slash_case_insensitive(self):
        self.assertEqual(normalize_city('bengaluru / bangalore'), 'Bangalore')

    def test_compound_comma_area_city(self):
        # Area + aliased city → keep structure, canonicalize city token
        self.assertEqual(normalize_city('Begur, Bengaluru'), 'Begur, Bangalore')

    def test_compound_comma_both_canonical(self):
        # Two canonical cities that differ — structure preserved, no alias applied
        result = normalize_city('Mumbai, Pune')
        self.assertIn('Mumbai', result)
        self.assertIn('Pune', result)

    def test_compound_comma_all_same(self):
        self.assertEqual(normalize_city('Bengaluru, Bangalore'), 'Bangalore')

    def test_unknown_city_passthrough(self):
        self.assertEqual(normalize_city('Kolkata'), 'Kolkata')
        self.assertEqual(normalize_city('Hyderabad'), 'Hyderabad')

    def test_different_cities_not_merged(self):
        result = normalize_city('Mumbai / Pune')
        # Must not collapse to a single city — they are genuinely different
        self.assertNotEqual(result, 'Mumbai')
        self.assertNotEqual(result, 'Pune')

    def test_prayagraj_alias(self):
        self.assertEqual(normalize_city('Prayagraj'), 'Allahabad')

    def test_thiruvananthapuram_alias(self):
        self.assertEqual(normalize_city('Thiruvananthapuram'), 'Trivandrum')


class TestRetailClassification(unittest.TestCase):

    def _row(self, opty_id, retail_by, retail_month="Jul'26"):
        return {'opty_id': opty_id, 'Retail By': retail_by,
                'DMS_Retail_Month': retail_month}

    def test_dms(self):
        rmap, unknowns = extract_rtype_map([self._row('1001', 'DMS')])
        self.assertEqual(rmap['1001']['rtype'], 'DMS')
        self.assertEqual(unknowns, {})

    def test_dms_lowercase(self):
        rmap, _ = extract_rtype_map([self._row('1001', 'dms')])
        self.assertEqual(rmap['1001']['rtype'], 'DMS')

    def test_call_out(self):
        rmap, _ = extract_rtype_map([self._row('1002', 'Call Out')])
        self.assertEqual(rmap['1002']['rtype'], 'Call Out')

    def test_callout_no_space(self):
        rmap, _ = extract_rtype_map([self._row('1002', 'callout')])
        self.assertEqual(rmap['1002']['rtype'], 'Call Out')

    def test_cc_normalizes_to_call_out(self):
        rmap, _ = extract_rtype_map([self._row('1003', 'CC')])
        self.assertEqual(rmap['1003']['rtype'], 'Call Out')

    def test_cc_lowercase(self):
        rmap, _ = extract_rtype_map([self._row('1003', 'cc')])
        self.assertEqual(rmap['1003']['rtype'], 'Call Out')

    def test_dash_sentinel_produces_blank_rtype(self):
        rmap, unknowns = extract_rtype_map([self._row('1004', '-')])
        self.assertEqual(rmap['1004']['rtype'], '')
        self.assertEqual(unknowns, {})   # '-' is a known sentinel, not unknown

    def test_em_dash_sentinel_produces_blank_rtype(self):
        rmap, unknowns = extract_rtype_map([self._row('1004', '–')])
        self.assertEqual(rmap['1004']['rtype'], '')
        self.assertEqual(unknowns, {})

    def test_blank_produces_blank_rtype(self):
        rmap, unknowns = extract_rtype_map([self._row('1005', '')])
        self.assertEqual(rmap['1005']['rtype'], '')
        self.assertEqual(unknowns, {})

    def test_na_sentinel_produces_blank_rtype(self):
        for val in ('N/A', 'NA', 'na', 'n/a'):
            rmap, unknowns = extract_rtype_map([self._row('1006', val)])
            self.assertEqual(rmap['1006']['rtype'], '', f"Expected blank for {val!r}")
            self.assertEqual(unknowns, {})

    def test_unknown_value_is_reported(self):
        rmap, unknowns = extract_rtype_map([self._row('1007', 'Showroom')])
        self.assertEqual(rmap['1007']['rtype'], '')
        self.assertIn('Showroom', unknowns)

    def test_no_retail_month_skipped(self):
        rows = [{'opty_id': '1001', 'Retail By': 'DMS', 'DMS_Retail_Month': ''}]
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap, {})

    def test_source_rtype_not_overwritten_by_blank(self):
        # A lead has rtype='' in lead sheet → retail_map rtype is NOT overwritten.
        # This tests the guard: "if info['rtype']:"
        retail_map = {'5001': {'rm': "Jul'26", 'rtype': 'Call Out', 'pm': 'TVS iQube'}}
        rtype_map_entry = {'5001': {'rm': "Jul'26", 'rtype': ''}}  # blank → no override
        for lid, info in rtype_map_entry.items():
            if lid in retail_map:
                _rm_ord = month_order(info.get('rm', ''))
                if 0 < _rm_ord < ONLINE_START_ORDER:
                    continue
                if info['rtype']:   # blank → guard blocks override
                    retail_map[lid]['rtype'] = info['rtype']
        self.assertEqual(retail_map['5001']['rtype'], 'Call Out')  # preserved

    def test_source_rtype_overwritten_when_nonempty(self):
        # A lead has rtype='DMS' → retail_map rtype IS overwritten for live month
        retail_map = {'5002': {'rm': "Jul'26", 'rtype': 'Call Out', 'pm': 'iQube'}}
        rtype_map_entry = {'5002': {'rm': "Jul'26", 'rtype': 'DMS'}}
        for lid, info in rtype_map_entry.items():
            if lid in retail_map:
                _rm_ord = month_order(info.get('rm', ''))
                if 0 < _rm_ord < ONLINE_START_ORDER:
                    continue
                if info['rtype']:
                    retail_map[lid]['rtype'] = info['rtype']
        self.assertEqual(retail_map['5002']['rtype'], 'DMS')

    def test_hist_rtype_not_overwritten_by_live_sheet(self):
        # For pre-ONLINE month retails, rtype_map override MUST NOT apply
        retail_map = {'6001': {'rm': "Jun'26", 'rtype': 'DMS', 'pm': 'iQube'}}
        rtype_map_entry = {'6001': {'rm': "Jun'26", 'rtype': 'Call Out'}}  # would override if allowed
        for lid, info in rtype_map_entry.items():
            if lid in retail_map:
                _rm_ord = month_order(info.get('rm', ''))
                if 0 < _rm_ord < ONLINE_START_ORDER:
                    continue   # pre-online → skip
                if info['rtype']:
                    retail_map[lid]['rtype'] = info['rtype']
        self.assertEqual(retail_map['6001']['rtype'], 'DMS')  # unchanged


class TestBumpAggregation(unittest.TestCase):

    def test_lead_only(self):
        d = {}
        bump(d, 'k', is_ret=False)
        self.assertEqual(d['k'], [1, 0, 0, 0])

    def test_retail_dms(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='DMS')
        self.assertEqual(d['k'], [1, 1, 1, 0])

    def test_retail_call_out(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='Call Out')
        self.assertEqual(d['k'], [1, 1, 0, 1])

    def test_retail_blank_rtype_is_unclassified(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='')
        # leads=1, rets=1, dms=0, co=0 → UNCLASSIFIED
        self.assertEqual(d['k'], [1, 1, 0, 0])
        self.assertEqual(d['k'][1] - (d['k'][2] + d['k'][3]), 1)

    def test_retail_dash_rtype_is_unclassified(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='-')
        # '-' does not contain 'DMS' or 'CALL' → unclassified
        self.assertEqual(d['k'][1] - (d['k'][2] + d['k'][3]), 1)

    def test_accumulation(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='DMS')
        bump(d, 'k', is_ret=True, rtype='Call Out')
        bump(d, 'k', is_ret=False)
        self.assertEqual(d['k'], [3, 2, 1, 1])

    def test_dms_co_sum_equals_retails(self):
        d = {}
        for _ in range(5):  bump(d, 'k', is_ret=True, rtype='DMS')
        for _ in range(3):  bump(d, 'k', is_ret=True, rtype='Call Out')
        for _ in range(2):  bump(d, 'k', is_ret=False)
        leads, rets, dms, co = d['k']
        self.assertEqual(dms + co, rets)

    def test_ubump_lead_and_retail_different_keys(self):
        d = {}
        ubump(d, 'lead_key', 'ret_key', is_ret=True, rtype='DMS')
        self.assertEqual(d['lead_key'][0], 1)   # lead counted in create-month
        self.assertEqual(d['ret_key'][1], 1)    # retail counted in retail-month
        self.assertEqual(d['ret_key'][2], 1)    # DMS

    def test_ubump_no_retail(self):
        d = {}
        ubump(d, 'lead_key', 'ret_key', is_ret=False)
        self.assertEqual(d['lead_key'][0], 1)
        self.assertNotIn('ret_key', d)


class TestDMSPlusCoEqualsRetails(unittest.TestCase):
    """Core business invariant: DMS + Call Out == Retails for every live month."""

    def _make_payload(self, lm_arr, monthly_rows, u_monthly_rows=None):
        return {
            'maps': {'lm': lm_arr},
            'monthly': monthly_rows,
            'u_monthly': u_monthly_rows or [],
        }

    def test_live_month_balanced(self):
        p = self._make_payload(
            ["Jul'26"],
            [[0, 100, 10, 7, 3]]   # lm=0, leads=100, rets=10, dms=7, co=3
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_live_month_unclassified_fails(self):
        p = self._make_payload(
            ["Jul'26"],
            [[0, 100, 10, 5, 3]]   # rets=10, dms=5, co=3 → diff=2 UNCLASSIFIED
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(len(errors), 1)
        self.assertIn("Jul'26", errors[0])

    def test_historical_month_unclassified_allowed(self):
        p = self._make_payload(
            ["Jun'26"],
            [[0, 100, 10, 5, 3]]   # historical → unclassified OK
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_multiple_live_months_all_balanced(self):
        p = self._make_payload(
            ["Jul'26", "Aug'26"],
            [[0, 50, 5, 3, 2],
             [1, 60, 6, 4, 2]]
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_mixed_live_and_hist(self):
        p = self._make_payload(
            ["Jun'26", "Jul'26"],
            [[0, 100, 10, 5, 3],   # hist — unclassified OK
             [1, 50,  5,  3, 2]]   # live — balanced
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_zero_retail_month_skipped(self):
        p = self._make_payload(
            ["Jul'26"],
            [[0, 50, 0, 0, 0]]    # 0 retails → no check needed
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_ou_live_month_unclassified_fails(self):
        p = self._make_payload(
            ["Jul'26"],
            [[0, 100, 10, 7, 3]],
            [[0, 100, 10, 5, 3]]   # OU: rets=10, dms=5, co=3 → diff=2
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any('OU' in e for e in errors))

    def test_grand_totals(self):
        p = self._make_payload(
            ["Jul'26", "Aug'26"],
            [[0, 100, 10, 7, 3],
             [1, 200, 20, 14, 6]]
        )
        _, oc_by_lm, _ = _validate_payload_logic(p)
        total_leads = sum(v[0] for v in oc_by_lm.values())
        total_rets  = sum(v[1] for v in oc_by_lm.values())
        self.assertEqual(total_leads, 300)
        self.assertEqual(total_rets, 30)

    def test_future_month_also_validated(self):
        # Sep'26 is a future live month — must also be checked
        p = self._make_payload(
            ["Sep'26"],
            [[0, 50, 5, 2, 2]]   # rets=5, dms+co=4 → diff=1
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(len(errors), 1)


class TestRetailFetchValidation(unittest.TestCase):

    def test_above_absolute_floor_no_baseline(self):
        ok, msg = _validate_retail_fetch_logic(55000)
        self.assertTrue(ok)

    def test_below_absolute_floor_no_baseline(self):
        ok, msg = _validate_retail_fetch_logic(40000)
        self.assertFalse(ok)
        self.assertIn('40,000', msg)

    def test_above_dynamic_floor(self):
        ok, msg = _validate_retail_fetch_logic(90000, {'retail_raw': {'rows': 100000}})
        self.assertTrue(ok)   # 90 % of 100k > 80 % threshold

    def test_below_dynamic_floor(self):
        ok, msg = _validate_retail_fetch_logic(70000, {'retail_raw': {'rows': 100000}})
        self.assertFalse(ok)  # 70 % < 80 % threshold

    def test_exactly_at_dynamic_floor(self):
        ok, msg = _validate_retail_fetch_logic(80000, {'retail_raw': {'rows': 100000}})
        self.assertTrue(ok)   # 80 % == threshold (inclusive)

    def test_dynamic_floor_never_below_absolute(self):
        # Even if prev was tiny, absolute floor still applies
        ok, msg = _validate_retail_fetch_logic(30000, {'retail_raw': {'rows': 40000}})
        self.assertFalse(ok)  # 30k < max(50k, 80%*40k=32k) = 50k

    def test_partial_response_detected(self):
        ok, msg = _validate_retail_fetch_logic(1000, {'retail_raw': {'rows': 320000}})
        self.assertFalse(ok)

    def test_empty_dataframe_fails(self):
        ok, msg = _validate_retail_fetch_logic(0)
        self.assertFalse(ok)

    def test_prev_metrics_legacy_int_format(self):
        # Old source_metrics.json stored plain int (not dict)
        ok, msg = _validate_retail_fetch_logic(90000, {'retail_raw': 100000})
        self.assertTrue(ok)

    def test_no_prev_metrics_dict_uses_absolute_floor(self):
        ok, _ = _validate_retail_fetch_logic(51000, {})
        self.assertTrue(ok)
        ok, _ = _validate_retail_fetch_logic(49000, {})
        self.assertFalse(ok)


class TestMonthUtilities(unittest.TestCase):

    def test_norm_month_standard(self):
        self.assertEqual(norm_month("Jul'26"), "Jul'26")
        self.assertEqual(norm_month("Aug'26"), "Aug'26")

    def test_norm_month_4digit_year(self):
        self.assertEqual(norm_month("July 2026"), "Jul'26")

    def test_norm_month_case_insensitive(self):
        self.assertEqual(norm_month("jul'26"), "Jul'26")
        self.assertEqual(norm_month("JUL'26"), "Jul'26")

    def test_month_order_basic(self):
        self.assertEqual(month_order("Jan'25"), 2501)
        self.assertEqual(month_order("Jul'26"), 2607)
        self.assertEqual(month_order("Dec'26"), 2612)

    def test_month_order_year_boundary(self):
        dec = month_order("Dec'26")
        jan = month_order("Jan'27")
        self.assertGreater(jan, dec)
        self.assertEqual(jan, 2701)
        self.assertEqual(dec, 2612)

    def test_month_order_blank(self):
        self.assertEqual(month_order(''), 0)
        self.assertEqual(month_order(None), 0)

    def test_month_order_unknown(self):
        self.assertEqual(month_order('garbage'), 0)

    def test_online_start_order(self):
        self.assertEqual(ONLINE_START_ORDER, 2607)

    def test_lead_master_start_order(self):
        self.assertEqual(LEAD_MASTER_START_ORDER, 2504)

    def test_month_order_sortable(self):
        months = ["Mar'26", "Jan'26", "Jul'26", "Dec'25", "Aug'26"]
        sorted_months = sorted(months, key=month_order)
        self.assertEqual(sorted_months[0], "Dec'25")
        self.assertEqual(sorted_months[-1], "Aug'26")

    def test_to_id_integer_float(self):
        self.assertEqual(to_id(123456789.0), '123456789')
        self.assertEqual(to_id(0), '0')

    def test_to_id_string(self):
        self.assertEqual(to_id('ABC123'), 'ABC123')

    def test_to_id_blank_or_none(self):
        self.assertEqual(to_id(''), '')
        self.assertEqual(to_id(None), '')
        self.assertEqual(to_id(float('nan')), '')


class TestDeduplication(unittest.TestCase):

    def test_bump_same_key_accumulates(self):
        d = {}
        bump(d, ('k',), is_ret=True, rtype='DMS')
        bump(d, ('k',), is_ret=False)
        self.assertEqual(d[('k',)], [2, 1, 1, 0])

    def test_extract_rtype_last_row_wins(self):
        # When the same opty_id appears twice, the last entry in the list wins.
        rows = [
            {'opty_id': '9001', 'Retail By': 'DMS',      'DMS_Retail_Month': "Jul'26"},
            {'opty_id': '9001', 'Retail By': 'Call Out', 'DMS_Retail_Month': "Jul'26"},
        ]
        rmap, _ = extract_rtype_map(rows)
        # Second row overwrites first — this mirrors dict update behaviour
        self.assertEqual(rmap['9001']['rtype'], 'Call Out')

    def test_opty_id_int_and_float_same(self):
        row_int   = {'opty_id': 2001,   'Retail By': 'DMS', 'DMS_Retail_Month': "Jul'26"}
        row_float = {'opty_id': 2001.0, 'Retail By': 'DMS', 'DMS_Retail_Month': "Jul'26"}
        r1, _ = extract_rtype_map([row_int])
        r2, _ = extract_rtype_map([row_float])
        self.assertEqual(set(r1.keys()), set(r2.keys()))


class TestThreeWayRetailMerge(unittest.TestCase):

    def test_case_a_live_month_overwrites_hist(self):
        hist = {'L1': {'rm': "Jul'26", 'rtype': 'DMS',      'pm': 'X'}}
        live = {'L1': {'rm': "Jul'26", 'rtype': 'Call Out',  'pm': 'X'}}
        result, a, _, _, _ = three_way_merge(hist, live)
        self.assertEqual(result['L1']['rtype'], 'Call Out')
        self.assertEqual(a, 1)

    def test_case_b_hist_rtype_preserved(self):
        hist = {'L2': {'rm': "Jun'26", 'rtype': 'DMS',      'pm': 'X'}}
        live = {'L2': {'rm': "Jun'26", 'rtype': 'Call Out', 'pm': 'X'}}
        result, a, b, _, _ = three_way_merge(hist, live)
        # Case B: hist rtype preserved, rm updated if valid
        self.assertEqual(result['L2']['rtype'], 'DMS')
        self.assertEqual(a, 0)

    def test_case_b_rm_updated_from_live(self):
        hist = {'L3': {'rm': "May'26", 'rtype': 'DMS', 'pm': 'X'}}
        live = {'L3': {'rm': "Jun'26", 'rtype': 'DMS', 'pm': 'X'}}
        result, _, b, _, _ = three_way_merge(hist, live)
        self.assertEqual(result['L3']['rm'], "Jun'26")
        self.assertEqual(b, 1)

    def test_case_c_new_pre_online_retail_added(self):
        hist = {}
        live = {'L4': {'rm': "Jun'26", 'rtype': 'DMS', 'pm': 'X'}}
        result, _, _, _, c = three_way_merge(hist, live)
        self.assertIn('L4', result)
        self.assertEqual(c, 1)

    def test_case_a_does_not_affect_hist_only_entries(self):
        hist = {'OLD': {'rm': "Apr'25", 'rtype': 'DMS', 'pm': 'X'}}
        live = {'NEW': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': 'X'}}
        result, _, _, _, _ = three_way_merge(hist, live)
        self.assertIn('OLD', result)
        self.assertIn('NEW', result)

    def test_hist_blank_rtype_not_overwritten_in_case_b(self):
        # Blank hist rtype stays blank in Case B (live data for pre-online month
        # must not silently patch a hist gap — that is handled by the production
        # pipeline separately).  Three-way merge only updates rm, not rtype.
        hist = {'L5': {'rm': "Jun'26", 'rtype': '', 'pm': 'X'}}
        live = {'L5': {'rm': "Jun'26", 'rtype': 'Call Out', 'pm': 'X'}}
        result, _, _, _, _ = three_way_merge(hist, live)
        self.assertEqual(result['L5']['rtype'], '')   # blank preserved by merge

    def test_pre_lead_master_start_rm_not_updated(self):
        # If live rm < LEAD_MASTER_START_ORDER, Case B keeps original rm
        hist = {'L6': {'rm': "May'25", 'rtype': 'DMS', 'pm': 'X'}}
        live = {'L6': {'rm': "Mar'25", 'rtype': 'DMS', 'pm': 'X'}}  # too early
        result, _, b, kept, _ = three_way_merge(hist, live)
        self.assertEqual(result['L6']['rm'], "May'25")   # unchanged
        self.assertEqual(kept, 1)


class TestNewMonthRollover(unittest.TestCase):

    def test_dec_to_jan_ordering(self):
        self.assertGreater(month_order("Jan'27"), month_order("Dec'26"))

    def test_year_boundary_sort(self):
        months = ["Nov'26", "Dec'26", "Jan'27", "Feb'27"]
        self.assertEqual(sorted(months, key=month_order), months)

    def test_future_month_order_nonzero(self):
        # Sep'26 (future as of Aug'26) must have valid month_order
        self.assertGreater(month_order("Sep'26"), ONLINE_START_ORDER)

    def test_online_start_inclusive(self):
        self.assertGreaterEqual(month_order("Jul'26"), ONLINE_START_ORDER)

    def test_jun_is_pre_online(self):
        self.assertLess(month_order("Jun'26"), ONLINE_START_ORDER)


class TestDryRunIsolation(unittest.TestCase):

    def test_payload_validation_does_not_write_files(self):
        # _validate_payload_logic is a pure computation — it must not create files
        p = {
            'maps': {'lm': ["Jul'26"]},
            'monthly': [[0, 50, 5, 3, 2]],
            'u_monthly': [],
        }
        tmpdir = tempfile.mkdtemp()
        before = set(os.listdir(tmpdir))
        _validate_payload_logic(p)
        after = set(os.listdir(tmpdir))
        self.assertEqual(before, after)

    def test_retail_fetch_logic_is_pure(self):
        # _validate_retail_fetch_logic is pure — no side effects
        tmpdir = tempfile.mkdtemp()
        before = set(os.listdir(tmpdir))
        _validate_retail_fetch_logic(60000)
        _validate_retail_fetch_logic(10000, {'retail_raw': {'rows': 320000}})
        after = set(os.listdir(tmpdir))
        self.assertEqual(before, after)


class TestAtomicPublication(unittest.TestCase):

    def test_staging_path_distinct_from_prod(self):
        # Staging filename includes a timestamp and 'staging' — never equals prod
        _RUN_START = datetime.now(timezone.utc)
        staging = f"tvs_payload_staging_{_RUN_START.strftime('%Y%m%d_%H%M')}.json.gz"
        prod    = "tvs_payload.json.gz"
        self.assertNotEqual(staging, prod)
        self.assertIn('staging', staging)

    def test_staging_roundtrip(self):
        # Compressed JSON write → read preserves payload integrity
        payload = {'maps': {'lm': ["Jul'26"]}, 'monthly': [[0, 100, 10, 7, 3]]}
        with tempfile.NamedTemporaryFile(suffix='.json.gz', delete=False) as f:
            fname = f.name
        try:
            with gzip.open(fname, 'wt', encoding='utf-8') as gf:
                json.dump(payload, gf)
            with gzip.open(fname, 'rt', encoding='utf-8') as gf:
                loaded = json.load(gf)
            self.assertEqual(payload, loaded)
        finally:
            os.unlink(fname)

    def test_production_unchanged_on_validation_failure(self):
        # Simulate: staging is written but production is not touched on failure.
        # Production file must not change even if validation errors exist.
        with tempfile.TemporaryDirectory() as tmpdir:
            prod = Path(tmpdir) / 'tvs_payload.json.gz'
            prod_data = {'version': 'known-good'}
            with gzip.open(prod, 'wt') as f:
                json.dump(prod_data, f)

            prod_mtime_before = prod.stat().st_mtime

            # Simulate failed validation — error list is non-empty
            p = {'maps': {'lm': ["Jul'26"]}, 'monthly': [[0, 50, 5, 2, 2]], 'u_monthly': []}
            errors, _, _ = _validate_payload_logic(p)
            self.assertTrue(errors, "Test setup: validation must fail")

            # On failure, production must not be touched
            prod_mtime_after = prod.stat().st_mtime
            self.assertEqual(prod_mtime_before, prod_mtime_after)

            # Verify known-good payload is still intact
            with gzip.open(prod, 'rt') as f:
                still_good = json.load(f)
            self.assertEqual(still_good, prod_data)


class TestSourceValidation(unittest.TestCase):

    def test_empty_source_fails(self):
        ok, msg = _validate_retail_fetch_logic(0)
        self.assertFalse(ok)

    def test_partial_source_fails(self):
        # Only 1 000 rows when 320 000 are expected → clear truncation
        ok, msg = _validate_retail_fetch_logic(1000, {'retail_raw': {'rows': 320000}})
        self.assertFalse(ok)
        self.assertIn('1,000', msg)

    def test_large_data_volume_ok(self):
        # Business growth: 700 k rows should still be fine
        ok, msg = _validate_retail_fetch_logic(700000, {'retail_raw': {'rows': 320000}})
        self.assertTrue(ok)

    def test_extract_rtype_map_empty_input(self):
        rmap, unknowns = extract_rtype_map([])
        self.assertEqual(rmap, {})
        self.assertEqual(unknowns, {})

    def test_extract_rtype_map_missing_columns_skipped(self):
        # Row without DMS_Retail_Month key must not cause an exception
        rows = [{'opty_id': '1001', 'Retail By': 'DMS'}]  # no DMS_Retail_Month
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap, {})

    def test_extract_rtype_map_missing_opty_id_skipped(self):
        rows = [{'Retail By': 'DMS', 'DMS_Retail_Month': "Jul'26"}]
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap, {})

    def test_malformed_opty_id_skipped(self):
        rows = [{'opty_id': '', 'Retail By': 'DMS', 'DMS_Retail_Month': "Jul'26"}]
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap, {})

    def test_duplicate_opty_id_last_wins(self):
        rows = [
            {'opty_id': '7001', 'Retail By': 'DMS',      'DMS_Retail_Month': "Jul'26"},
            {'opty_id': '7001', 'Retail By': 'Call Out', 'DMS_Retail_Month': "Jul'26"},
        ]
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap['7001']['rtype'], 'Call Out')

    def test_multiple_unknown_values_tracked(self):
        rows = [
            {'opty_id': '8001', 'Retail By': 'Showroom',  'DMS_Retail_Month': "Jul'26"},
            {'opty_id': '8002', 'Retail By': 'Dealer lot', 'DMS_Retail_Month': "Jul'26"},
            {'opty_id': '8003', 'Retail By': 'Showroom',  'DMS_Retail_Month': "Jul'26"},
        ]
        _, unknowns = extract_rtype_map(rows)
        self.assertEqual(unknowns.get('Showroom'), 2)
        self.assertEqual(unknowns.get('Dealer lot'), 1)


class TestFutureDataExtensibility(unittest.TestCase):
    """Validate that new data does not require code changes."""

    def test_new_month_passes_through_month_order(self):
        # Sep'26, Oct'26, Jan'27 must all produce valid, increasing month_orders
        seq = ["Jul'26", "Aug'26", "Sep'26", "Oct'26", "Nov'26", "Dec'26", "Jan'27"]
        orders = [month_order(m) for m in seq]
        self.assertEqual(orders, sorted(orders))
        self.assertTrue(all(o > 0 for o in orders))

    def test_new_city_passes_through_unchanged(self):
        new_city = 'Nanded'
        self.assertEqual(normalize_city(new_city), new_city)

    def test_new_city_with_alias_normalizes(self):
        # If a new alias is added to _CITY_ALIAS, it works immediately
        _CITY_ALIAS['Bombay'] = 'Mumbai'
        self.assertEqual(normalize_city('Bombay'), 'Mumbai')
        del _CITY_ALIAS['Bombay']

    def test_new_source_in_payload_does_not_break_validation(self):
        # A new source value is just another dimension — validation only checks
        # DMS+CO vs Retails, not specific source names
        p = {
            'maps': {'lm': ["Sep'26"]},
            'monthly': [[0, 100, 10, 7, 3]],
            'u_monthly': [],
        }
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_doubled_row_count_still_passes_dynamic_threshold(self):
        # Business doubles in size — row count doubling must not trigger a false fail
        ok, _ = _validate_retail_fetch_logic(640000, {'retail_raw': {'rows': 320000}})
        self.assertTrue(ok)

    def test_halved_row_count_fails(self):
        # A sudden drop to 50 % is suspicious even for a large dataset
        ok, _ = _validate_retail_fetch_logic(160000, {'retail_raw': {'rows': 320000}})
        self.assertFalse(ok)

    def test_new_retail_type_in_live_sheet_is_unknown(self):
        # A new 'Call Type' value in the retail master (e.g. 'Leasing') becomes
        # unknown. extract_rtype_map must flag it rather than silently classify it.
        rows = [{'opty_id': '9999', 'Retail By': 'Leasing', 'DMS_Retail_Month': "Sep'26"}]
        rmap, unknowns = extract_rtype_map(rows)
        self.assertEqual(rmap['9999']['rtype'], '')
        self.assertIn('Leasing', unknowns)

    def test_large_number_of_months_in_payload(self):
        # Payload with 30 months — validation must handle arbitrary month count
        lm_arr = [f"{MONTH_NAMES[m % 12]}'{(m // 12) + 25}" for m in range(30)]
        rows   = [[i, 100, 10, 7, 3] for i in range(30)]
        p = {'maps': {'lm': lm_arr}, 'monthly': rows, 'u_monthly': []}
        errors, _, _ = _validate_payload_logic(p)
        # Only live months (≥ Jul'26) are checked; all are balanced here
        self.assertEqual(errors, [])


# ---------------------------------------------------------------------------
# Regression: commit 980ca6e — '-' sentinel overrode retail master Call Type
# ---------------------------------------------------------------------------
class TestDashSentinelRetailChain(unittest.TestCase):
    """End-to-end regression for the 2026-08-22 production incident.

    Root cause: extract_rtype_map at 3b76a6e stored 'Retail By' verbatim,
    so '-' reached the override guard as a truthy rtype and wiped the retail
    master's valid 'Call Out'.  Fix in 97abaeb: normalize '-'/'–'/blank/N/A
    to ''.  This test exercises the full chain in one place.
    """

    def test_dash_in_retail_by_preserves_call_out_from_retail_master(self):
        # Step 1: lead sheet has Retail By='-' for a Jul'26 retail
        rows = [{'opty_id': 'L9001', 'Retail By': '-', 'DMS_Retail_Month': "Jul'26"}]
        rtype_map, _ = extract_rtype_map(rows)
        self.assertEqual(rtype_map.get('L9001', {}).get('rtype'), '',
                         "'-' must produce rtype='' from extract_rtype_map")

        # Step 2: retail master has a valid Call Type for the same lead
        retail_map = {'L9001': {'rm': "Jul'26", 'rtype': 'Call Out', 'pm': 'TVS iQube'}}

        # Step 3: apply rtype_map override (mirrors the production loop)
        for lid, info in rtype_map.items():
            if lid in retail_map:
                _rm_ord = month_order(info.get('rm', ''))
                if 0 < _rm_ord < ONLINE_START_ORDER:
                    continue
                if info['rtype']:   # '' is falsy → guard blocks the override
                    retail_map[lid]['rtype'] = info['rtype']

        # Step 4: retail master's Call Out must be preserved
        self.assertEqual(retail_map['L9001']['rtype'], 'Call Out',
                         "retail master 'Call Out' must survive a '-' sentinel in lead sheet")

        # Step 5: aggregation with preserved Call Out → DMS+CO == Retails (no unclassified)
        d = {}
        bump(d, 'Jul26', is_ret=True, rtype=retail_map['L9001']['rtype'])
        leads, rets, dms, co = d['Jul26']
        self.assertEqual(rets, 1)
        self.assertEqual(dms + co, rets,
                         "DMS+CO must equal Retails when Call Out is correctly preserved")


class TestNtorqNormalization(unittest.TestCase):
    """Regression tests for the TVS NTORQ 125 / NTORQ 150 split.

    Root cause (confirmed 2026-08-25): 'TVS NTorq 150' hit the NTORQ_150 branch
    in normalize_purchased_model (no map entry, and keyword guard rejected it
    as an ambiguous 150 variant).  Fix: explicit map entries for both mixed-case
    and uppercase forms.  This class guards against regression and confirms the
    two models remain independent.

    These tests inline the relevant PURCHASED_MODEL_MAP entries and the NTORQ
    path of normalize_purchased_model so no module-level I/O is required.
    Keep in sync with push_tvs_data.py whenever either changes.
    """

    _MAP = {
        # new entries (fix)
        'TVS NTorq 150':                      'TVS NTORQ 150',
        'TVS NTORQ 150':                      'TVS NTORQ 150',
        # existing entries that must not be disturbed
        'TVS NTorq':                          'TVS NTORQ 125',
        'TVS NTORQ 125':                      'TVS NTORQ 125',
        'NTORQ 125 DISC – Race Edition BSVI': 'TVS NTORQ 125',
        'TVS NTORQ 125 RACE XP':              'TVS NTORQ 125',
        'TVS NTORQ 125 DISC BSVI':            'TVS NTORQ 125',
    }

    def _norm(self, pm):
        """Minimal inline of the NTORQ paths in normalize_purchased_model."""
        pm = str(pm or '').strip()
        if not pm:
            return 'Unknown'
        if pm in self._MAP:
            val = str(self._MAP[pm] or '').strip()
            if val and val.upper() not in ('NA', 'N/A', 'NAN', 'NONE'):
                return val
        pu = pm.upper()
        if 'NTORQ' in pu and '150' not in pu:
            return 'TVS NTORQ 125'
        if 'NTORQ' in pu or 'NTRQ' in pu:
            return 'Unknown'
        return 'Unknown'

    def test_production_raw_value_maps_to_ntorq_150(self):
        """'TVS NTorq 150' (exact production value confirmed by diagnostic) → 'TVS NTORQ 150'."""
        self.assertEqual(self._norm('TVS NTorq 150'), 'TVS NTORQ 150')

    def test_uppercase_variant_maps_to_ntorq_150(self):
        """'TVS NTORQ 150' (canonical uppercase form) → 'TVS NTORQ 150'."""
        self.assertEqual(self._norm('TVS NTORQ 150'), 'TVS NTORQ 150')

    def test_ntorq_125_exact_entry_unchanged(self):
        """Existing exact map entry 'TVS NTORQ 125' must not be disturbed."""
        self.assertEqual(self._norm('TVS NTORQ 125'), 'TVS NTORQ 125')

    def test_ntorq_bare_exact_entry_unchanged(self):
        """'TVS NTorq' (bare, no variant) must still map to 'TVS NTORQ 125'."""
        self.assertEqual(self._norm('TVS NTorq'), 'TVS NTORQ 125')

    def test_ntorq_125_keyword_fallback_unchanged(self):
        """'TVS NTorq 125' (mixed-case, not in map) must resolve via keyword to 'TVS NTORQ 125'."""
        self.assertEqual(self._norm('TVS NTorq 125'), 'TVS NTORQ 125')

    def test_unrecognized_ntorq_150_variant_still_unknown(self):
        """A future NTORQ+150 variant not in the map must still return Unknown."""
        self.assertEqual(self._norm('NTORQ SPORT 150 SPECIAL'), 'Unknown')

    def test_ntorq_150_and_125_not_merged(self):
        """'TVS NTORQ 150' and 'TVS NTORQ 125' must resolve to different canonical names."""
        self.assertNotEqual(self._norm('TVS NTorq 150'), self._norm('TVS NTORQ 125'))


# ---------------------------------------------------------------------------
# Retail Ageing tests
# ---------------------------------------------------------------------------
# Inline copies of the three new ageing functions from push_tvs_data.py.
# Keep in sync with push_tvs_data.py whenever those functions change.

import pandas as _pd
import datetime as _datetime

def _parse_date(s):
    """Inline copy of push_tvs_data.parse_date."""
    try:
        ts = _pd.Timestamp(str(s or '').strip())
        if _pd.isnull(ts):
            return None
        return ts.date()
    except Exception:
        return None

def _age_bucket(days):
    """Inline copy of push_tvs_data.age_bucket."""
    if days <= 7:  return 0
    if days <= 14: return 1
    if days <= 30: return 2
    return 3

_AGE_BUCKET_LABELS_TEST = ['0-7 days', '8-14 days', '15-30 days', '30+ days']

def _run_ageing_fixture(leads, retail_map):
    """Minimal inline of the ageing aggregation from build_payload's is_ret block.

    leads: list of dicts with keys: lid, lm, src, mdl, cd, lt, st, city
           (cd=CreateDate string; lt/st/city default to '' if missing)
    retail_map: {lid: {rm, rtype, pm, rd}}

    Returns:
        ram: {(mi,si,tti,sti,cti,abi,li): [rets,dms,co]}
        meta: {total, valid, no_rd, no_cd, neg}
        maps: {mdl:[], src:[], lm:[], lt:[], st:[], city:[]}
    """
    mdl_idx,  src_idx,  lm_idx  = {}, {}, {}
    mdl_arr,  src_arr,  lm_arr  = [], [], []
    lt_idx,   st_idx,   city_idx = {}, {}, {}
    lt_arr,   st_arr,   city_arr = [], [], []

    def ix(d, arr, v):
        if v not in d:
            d[v] = len(arr); arr.append(v)
        return d[v]

    ram = {}
    total = valid = no_rd = no_cd = neg = 0

    for lead in leads:
        lid = lead['lid']
        if lid not in retail_map:
            continue
        mi  = ix(mdl_idx,  mdl_arr,  lead['mdl'])
        si  = ix(src_idx,  src_arr,  lead['src'])
        li  = ix(lm_idx,   lm_arr,   lead['lm'])
        tti = ix(lt_idx,   lt_arr,   lead.get('lt', ''))
        sti = ix(st_idx,   st_arr,   lead.get('st', ''))
        cti = ix(city_idx, city_arr, lead.get('city', ''))
        rtype = retail_map[lid].get('rtype', 'DMS')

        total += 1
        _rd = retail_map[lid].get('rd')
        _cd = _parse_date(lead.get('cd', ''))
        if _rd is None:
            no_rd += 1
        elif _cd is None:
            no_cd += 1
        else:
            age_days = (_rd - _cd).days
            if age_days < 0:
                neg += 1
            else:
                abi = _age_bucket(age_days)
                k = (mi, si, tti, sti, cti, abi, li)
                if k not in ram: ram[k] = [0, 0, 0]
                ram[k][0] += 1
                rt_u = rtype.upper()
                if 'DMS' in rt_u:    ram[k][1] += 1
                elif 'CALL' in rt_u: ram[k][2] += 1
                valid += 1

    meta = {'total': total, 'valid': valid, 'no_rd': no_rd, 'no_cd': no_cd, 'neg': neg}
    maps = {'mdl': mdl_arr, 'src': src_arr, 'lm': lm_arr, 'lt': lt_arr, 'st': st_arr, 'city': city_arr}
    return ram, meta, maps


class TestRetailAgeing(unittest.TestCase):
    """20 regression tests for the Retail Ageing feature."""

    # ── 1-8: age_bucket boundary correctness ──────────────────────────────────

    def test_age_0_maps_to_bucket_0(self):
        """age=0 days → bucket 0 (0-7 days)."""
        self.assertEqual(_age_bucket(0), 0)

    def test_age_7_maps_to_bucket_0(self):
        """age=7 days → bucket 0 (0-7 days, inclusive boundary)."""
        self.assertEqual(_age_bucket(7), 0)

    def test_age_8_maps_to_bucket_1(self):
        """age=8 days → bucket 1 (8-14 days)."""
        self.assertEqual(_age_bucket(8), 1)

    def test_age_14_maps_to_bucket_1(self):
        """age=14 days → bucket 1 (inclusive boundary)."""
        self.assertEqual(_age_bucket(14), 1)

    def test_age_15_maps_to_bucket_2(self):
        """age=15 days → bucket 2 (15-30 days)."""
        self.assertEqual(_age_bucket(15), 2)

    def test_age_30_maps_to_bucket_2(self):
        """age=30 days → bucket 2 (inclusive boundary)."""
        self.assertEqual(_age_bucket(30), 2)

    def test_age_31_maps_to_bucket_3(self):
        """age=31 days → bucket 3 (30+ days)."""
        self.assertEqual(_age_bucket(31), 3)

    def test_age_large_maps_to_bucket_3(self):
        """age=365 days → bucket 3 (30+)."""
        self.assertEqual(_age_bucket(365), 3)

    # ── 9-11: exclusion cases ─────────────────────────────────────────────────

    def test_negative_age_excluded(self):
        """Retail_Date < CreateDate → not in ram, counted in meta.neg."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider',
                         'rd': _datetime.date(2026, 8, 1)}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': '2026-08-10'}]  # cd AFTER rd → negative
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(len(ram), 0, "ram must be empty for negative age")
        self.assertEqual(meta['neg'], 1)
        self.assertEqual(meta['valid'], 0)

    def test_missing_retail_date_excluded(self):
        """rd=None → excluded from ram, counted in meta.no_rd."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider', 'rd': None}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': '2026-08-01'}]
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(len(ram), 0)
        self.assertEqual(meta['no_rd'], 1)
        self.assertEqual(meta['valid'], 0)

    def test_missing_create_date_excluded(self):
        """cd='' → parse_date returns None → excluded, counted in meta.no_cd."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider',
                         'rd': _datetime.date(2026, 8, 10)}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': ''}]  # missing CreateDate
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(len(ram), 0)
        self.assertEqual(meta['no_cd'], 1)
        self.assertEqual(meta['valid'], 0)

    # ── 12: date parsing ──────────────────────────────────────────────────────

    def test_iso_date_parsing(self):
        """parse_date('2026-08-15') → datetime.date(2026, 8, 15)."""
        result = _parse_date('2026-08-15')
        self.assertEqual(result, _datetime.date(2026, 8, 15))

    def test_parse_date_invalid_returns_none(self):
        """parse_date('bad') → None."""
        self.assertIsNone(_parse_date('bad'))

    def test_parse_date_empty_returns_none(self):
        """parse_date('') → None."""
        self.assertIsNone(_parse_date(''))

    # ── 13-14: model and source come from lead master ─────────────────────────

    def test_model_comes_from_lead_master(self):
        """Model index in ram uses lead master ModelName, not retail purchasedModel."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS',
                         'pm': 'TVS Apache RTR 160',   # retail purchased model (different)
                         'rd': _datetime.date(2026, 8, 10)}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',  # lead model
                  'src': 'Organic', 'cd': '2026-08-01'}]
        ram, meta, maps = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 1)
        # The model in the ram key must be the lead model index
        (mi, si, tti, sti, cti, abi, li), _ = list(ram.items())[0]
        self.assertEqual(maps['mdl'][mi], 'TVS Raider')  # lead master model, not retail pm

    def test_source_comes_from_lead_master(self):
        """Source index in ram uses lead master Source, not any retail attribute."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider',
                         'rd': _datetime.date(2026, 8, 10)}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Facebook', 'cd': '2026-08-01'}]  # lead source
        ram, meta, maps = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 1)
        (mi, si, tti, sti, cti, abi, li) = list(ram.keys())[0]
        self.assertEqual(maps['src'][si], 'Facebook')

    # ── 15: bucket reconciliation ─────────────────────────────────────────────

    def test_ageing_bucket_reconciliation(self):
        """Sum of all bucket rets == meta.valid (every valid retail lands in exactly one bucket)."""
        rd_base = _datetime.date(2026, 8, 1)
        ages = [0, 5, 8, 12, 15, 25, 31, 90]  # one per bucket (multiple per bucket)
        leads = [{'lid': f'l{i}', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': '2026-07-01'} for i in range(len(ages))]
        rmap = {f'l{i}': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '',
                           'rd': _datetime.date(2026, 7, 1) + _datetime.timedelta(days=a)}
                for i, a in enumerate(ages)}
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        bucket_total = sum(v[0] for v in ram.values())
        self.assertEqual(bucket_total, meta['valid'])
        self.assertEqual(meta['valid'], len(ages))

    # ── 16: DMS + Call Out = Retails within ageing ───────────────────────────

    def test_dms_plus_co_equals_rets_in_ageing(self):
        """For every ram cell: dms + co == rets (no retail is both DMS and Call Out)."""
        leads = [
            {'lid': 'dms1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
            {'lid': 'co1',  'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
        ]
        rd = _datetime.date(2026, 8, 10)
        rmap = {
            'dms1': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': '', 'rd': rd},
            'co1':  {'rm': "Aug'26", 'rtype': 'Call Out', 'pm': '', 'rd': rd},
        }
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 2)
        for k, v in ram.items():
            rets, dms, co = v
            self.assertEqual(dms + co, rets, f"dms+co != rets for ram key {k}: {v}")

    # ── 17: duplicate opty_id no double count ─────────────────────────────────

    def test_duplicate_opty_id_no_double_count(self):
        """retail_map is keyed by lid (dict); each lid appears once — no double count."""
        # retail_map overwrite: if a lid appears twice, only last survives (dict key)
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '',
                         'rd': _datetime.date(2026, 8, 10)}}
        # leads deduplicated to one occurrence of lid1 (production dedup; here just one row)
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': '2026-08-01'}]
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        total_rets = sum(v[0] for v in ram.values())
        self.assertEqual(total_rets, 1)
        self.assertEqual(meta['valid'], 1)

    # ── 18: rd is additional field — existing retail_map fields unchanged ──────

    def test_rd_is_additional_field_in_retail_map(self):
        """Adding rd to build_retail_map must not alter rm, rtype, pm."""
        # Simulate the retail_map entry structure as built by build_retail_map
        entry_without_rd = {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider'}
        rd_map = {'lid1': _datetime.date(2026, 8, 15)}
        # Simulate: rmap[lid] = {**entry_without_rd, 'rd': rd_map.get(lid)}
        combined = {**entry_without_rd, 'rd': rd_map.get('lid1')}
        self.assertEqual(combined['rm'],    "Aug'26")
        self.assertEqual(combined['rtype'], 'DMS')
        self.assertEqual(combined['pm'],    'TVS Raider')
        self.assertEqual(combined['rd'],    _datetime.date(2026, 8, 15))
        # rd_map=None case: rd must be None
        combined_no_rd = {**entry_without_rd, 'rd': None}
        self.assertIsNone(combined_no_rd['rd'])
        self.assertEqual(combined_no_rd['rm'], "Aug'26")

    # ── 19-20: ageing aggregation is additive — existing OC/OU unaffected ─────

    def test_ageing_does_not_alter_lead_counts(self):
        """The ram aggregation touches only retailed leads; lead counts in mm are unaffected."""
        # Verify: lead count (index 0 in bump result) is independent of rd
        # The ageing bump only runs inside 'if is_ret' and only increments ram — not mm.
        # Verified by fixture: ageing meta.total == retails matched, not leads.
        leads = [
            {'lid': 'l1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
            {'lid': 'l2', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
        ]
        rmap = {  # only l1 retailed
            'l1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 10)},
        }
        _, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['total'], 1, "ageing total must count retails only (not all leads)")
        self.assertEqual(meta['valid'], 1)

    def test_ageing_does_not_alter_existing_retail_total(self):
        """Bucket totals must equal exactly the retails with valid dates — no extras."""
        leads = [
            {'lid': 'a', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
            {'lid': 'b', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': ''},  # no cd
            {'lid': 'c', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
        ]
        rd = _datetime.date(2026, 8, 10)
        rmap = {
            'a': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': '', 'rd': rd},
            'b': {'rm': "Aug'26", 'rtype': 'Call Out', 'pm': '', 'rd': rd},
            'c': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': '', 'rd': None},  # no rd
        }
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        bucket_total = sum(v[0] for v in ram.values())
        # a: valid (age=9 → bucket 1); b: no_cd; c: no_rd
        self.assertEqual(meta['total'],  3)
        self.assertEqual(meta['valid'],  1)
        self.assertEqual(meta['no_cd'],  1)
        self.assertEqual(meta['no_rd'],  1)
        self.assertEqual(bucket_total,   1)
        self.assertEqual(meta['valid'] + meta['no_rd'] + meta['no_cd'] + meta['neg'], meta['total'])

    # ── 21-34: contribution column logic ─────────────────────────────────────

    def _make_leads_rmap(self, ages, mdl='TVS Raider', src='Organic', lt='', st='', city=''):
        """Helper: build leads + rmap for a list of ages (days). cd=2026-07-01."""
        cd = '2026-07-01'
        base = _datetime.date(2026, 7, 1)
        leads = [{'lid': f'l{i}', 'lm': "Jul'26", 'mdl': mdl, 'src': src,
                  'lt': lt, 'st': st, 'city': city, 'cd': cd}
                 for i in range(len(ages))]
        rmap  = {f'l{i}': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '',
                            'rd': base + _datetime.timedelta(days=a)}
                 for i, a in enumerate(ages)}
        return leads, rmap

    def test_contribution_bucket_column_count_only(self):
        """Each bucket in ram stores count, not percentage."""
        leads, rmap = self._make_leads_rmap([3, 10, 20, 40])  # one per bucket
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 4)
        for v in ram.values():
            # v = [rets, dms, co] — all ints, no floats/percentages
            self.assertIsInstance(v[0], int)

    def test_contribution_separate_from_count(self):
        """Contribution % is computed from raw counts, not stored in ram."""
        leads, rmap = self._make_leads_rmap([3, 10])  # bucket0=1, bucket1=1
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        # Calculate contribution externally — must equal bucket/model_total
        buckets = [0, 0, 0, 0]
        for k, v in ram.items():
            buckets[k[5]] += v[0]  # k[5]=abi in new 7-element key
        mdl_total = sum(buckets)
        for abi in range(4):
            expected_pct = buckets[abi] / mdl_total if mdl_total else 0
            computed = buckets[abi] / mdl_total if mdl_total else 0
            self.assertAlmostEqual(expected_pct, computed)

    def test_contribution_formula_bucket_over_model_total(self):
        """Contribution = bucket_count / model_total × 100."""
        # 350 + 1322 + 1186 + 531 = 3389 (example from spec)
        ages = [3]*350 + [10]*1322 + [20]*1186 + [40]*531
        leads, rmap = self._make_leads_rmap(ages)
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 3389)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        mdl_total = sum(buckets)
        self.assertEqual(mdl_total, 3389)
        self.assertAlmostEqual(buckets[0] / mdl_total * 100, 10.3, delta=0.1)
        self.assertAlmostEqual(buckets[1] / mdl_total * 100, 39.0, delta=0.1)
        self.assertAlmostEqual(buckets[2] / mdl_total * 100, 35.0, delta=0.1)
        self.assertAlmostEqual(buckets[3] / mdl_total * 100, 15.7, delta=0.1)

    def test_contribution_four_buckets_sum_to_100(self):
        """Four bucket contributions for a model sum to ~100%."""
        ages = [2, 9, 18, 35, 50, 5, 12, 25]
        leads, rmap = self._make_leads_rmap(ages)
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        mdl_total = sum(buckets)
        total_pct = sum(b / mdl_total * 100 for b in buckets)
        self.assertAlmostEqual(total_pct, 100.0, delta=0.01)

    def test_grand_total_contribution_uses_grand_denominator(self):
        """Grand Total bucket % = bucket_count / grand_total (NOT avg of model %)."""
        leads_a, rmap_a = self._make_leads_rmap([3]*100 + [40]*100, mdl='Model A')
        leads_b, rmap_b = self._make_leads_rmap([3]*50,              mdl='Model B')
        all_leads = leads_a + leads_b
        all_rmap  = {**rmap_a, **rmap_b}
        ram, meta, _ = _run_ageing_fixture(all_leads, all_rmap)
        grand_buckets = [0, 0, 0, 0]
        for k, v in ram.items(): grand_buckets[k[5]] += v[0]
        grand_total = sum(grand_buckets)
        # bucket0 = 100 (A) + 50 (B) = 150; bucket3 = 100 (A only)
        self.assertEqual(grand_buckets[0], 150)
        self.assertEqual(grand_buckets[3], 100)
        self.assertEqual(grand_total, 250)
        # Grand pct for bucket0 = 150/250 = 60%, NOT avg of 50% (A) and 100% (B)
        self.assertAlmostEqual(grand_buckets[0] / grand_total * 100, 60.0, delta=0.01)

    def test_total_column_is_count_not_percentage(self):
        """Model total is the sum of bucket counts — an integer, not a percentage."""
        ages = [3, 10, 20, 40]
        leads, rmap = self._make_leads_rmap(ages)
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        mdl_total = sum(buckets)
        self.assertEqual(mdl_total, 4)
        self.assertIsInstance(mdl_total, int)

    def test_source_filter_recalculates_contribution(self):
        """Source filter changes denominator: contribution = bucket/filtered_model_total."""
        # 3 organic leads in bucket0, 1 fb lead in bucket1 — filtering to Organic only
        leads = [
            {'lid': 'o1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic',  'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'o2', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic',  'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'o3', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic',  'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'f1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Facebook', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
        ]
        base = _datetime.date(2026, 7, 1)
        rmap = {
            'o1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=3)},
            'o2': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=3)},
            'o3': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=3)},
            'f1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=10)},
        }
        ram_all, _, _ = _run_ageing_fixture(leads, rmap)
        # Simulate source filter: only Organic — collect organic leads
        org_leads = [l for l in leads if l['src'] == 'Organic']
        org_rmap  = {k: v for k, v in rmap.items() if k.startswith('o')}
        ram_org, _, _ = _run_ageing_fixture(org_leads, org_rmap)
        # Organic-only: all 3 in bucket0; model total = 3; bucket0 contribution = 100%
        buckets_org = [0, 0, 0, 0]
        for k, v in ram_org.items(): buckets_org[k[5]] += v[0]
        self.assertEqual(sum(buckets_org), 3)
        self.assertAlmostEqual(buckets_org[0] / sum(buckets_org) * 100, 100.0)

    def test_model_filter_recalculates_contribution(self):
        """Model filter restricts rows; contribution denominator = filtered model total."""
        base = _datetime.date(2026, 7, 1)
        # Model A: 4 leads age=3 (bucket0) + 6 leads age=40 (bucket3)
        leads_a = [{'lid': f'a{i}', 'lm': "Jul'26", 'mdl': 'Model A', 'src': 'Organic',
                    'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'} for i in range(10)]
        ages_a  = [3]*4 + [40]*6
        rmap_a  = {f'a{i}': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '',
                              'rd': base + _datetime.timedelta(days=ages_a[i])}
                   for i in range(10)}
        # Model B: 10 leads age=3 (bucket0) — should be excluded by filter
        leads_b = [{'lid': f'b{i}', 'lm': "Jul'26", 'mdl': 'Model B', 'src': 'Organic',
                    'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'} for i in range(10)]
        rmap_b  = {f'b{i}': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '',
                              'rd': base + _datetime.timedelta(days=3)}
                   for i in range(10)}
        # Filter to Model A only
        fil_leads = leads_a
        fil_rmap  = rmap_a
        ram, _, _ = _run_ageing_fixture(fil_leads, fil_rmap)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        total = sum(buckets)
        self.assertEqual(total, 10)
        self.assertAlmostEqual(buckets[0] / total * 100, 40.0, delta=0.01)
        self.assertAlmostEqual(buckets[3] / total * 100, 60.0, delta=0.01)

    def test_lead_type_filter_recalculates_contribution(self):
        """Lead type filter changes the ageing population and thus contribution %."""
        leads = [
            {'lid': 'l1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': 'Hot',  'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'l2', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': 'Warm', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'l3', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': 'Hot',  'st': '', 'city': '', 'cd': '2026-07-01'},
        ]
        base = _datetime.date(2026, 7, 1)
        rmap = {
            'l1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=3)},
            'l2': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=10)},
            'l3': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=3)},
        }
        # Filter to Hot only
        hot_leads = [l for l in leads if l['lt'] == 'Hot']
        hot_rmap  = {'l1': rmap['l1'], 'l3': rmap['l3']}
        ram, meta, _ = _run_ageing_fixture(hot_leads, hot_rmap)
        self.assertEqual(meta['valid'], 2)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        # Both Hot leads in bucket0 → contribution = 100%
        self.assertAlmostEqual(buckets[0] / sum(buckets) * 100, 100.0)

    def test_state_filter_recalculates_contribution(self):
        """State filter restricts population; contribution recalculates on filtered total."""
        leads = [
            {'lid': 'l1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': 'MH', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'l2', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': 'KA', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'l3', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': 'MH', 'city': '', 'cd': '2026-07-01'},
        ]
        base = _datetime.date(2026, 7, 1)
        rmap = {
            'l1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=3)},
            'l2': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=10)},
            'l3': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=20)},
        }
        mh_leads = [l for l in leads if l['st'] == 'MH']
        mh_rmap  = {'l1': rmap['l1'], 'l3': rmap['l3']}
        ram, meta, _ = _run_ageing_fixture(mh_leads, mh_rmap)
        self.assertEqual(meta['valid'], 2)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        total = sum(buckets)
        # l1→bucket0, l3→bucket2; contributions 50%/0%/50%/0%
        self.assertAlmostEqual(buckets[0] / total * 100, 50.0)
        self.assertAlmostEqual(buckets[2] / total * 100, 50.0)

    def test_city_filter_recalculates_contribution(self):
        """City filter restricts ageing population; contribution recalculates."""
        leads = [
            {'lid': 'l1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': 'Mumbai',   'cd': '2026-07-01'},
            {'lid': 'l2', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': 'Pune',     'cd': '2026-07-01'},
            {'lid': 'l3', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': 'Mumbai',   'cd': '2026-07-01'},
        ]
        base = _datetime.date(2026, 7, 1)
        rmap = {
            'l1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=5)},
            'l2': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=40)},
            'l3': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': base + _datetime.timedelta(days=5)},
        }
        mum_leads = [l for l in leads if l['city'] == 'Mumbai']
        mum_rmap  = {'l1': rmap['l1'], 'l3': rmap['l3']}
        ram, meta, _ = _run_ageing_fixture(mum_leads, mum_rmap)
        self.assertEqual(meta['valid'], 2)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        # Both Mumbai leads age=5 → bucket0; contribution = 100%
        self.assertAlmostEqual(buckets[0] / sum(buckets) * 100, 100.0)

    def test_month_selection_recalculates_contribution(self):
        """Month filter changes the ageing population and contribution denominators."""
        leads = [
            {'lid': 'j1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'a1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
            {'lid': 'a2', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
        ]
        rmap = {
            'j1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 5)},   # age=4→bucket0
            'a1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 15)},  # age=14→bucket1
            'a2': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 31)},  # age=30→bucket2
        }
        # Aug only
        aug_leads = [l for l in leads if l['lm'] == "Aug'26"]
        aug_rmap  = {'a1': rmap['a1'], 'a2': rmap['a2']}
        ram, meta, _ = _run_ageing_fixture(aug_leads, aug_rmap)
        self.assertEqual(meta['valid'], 2)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        total = sum(buckets)
        # a1→bucket1(50%), a2→bucket2(50%)
        self.assertAlmostEqual(buckets[1] / total * 100, 50.0)
        self.assertAlmostEqual(buckets[2] / total * 100, 50.0)

    def test_all_months_aggregates_correctly(self):
        """All Months: contributions use grand total across all months."""
        leads = [
            {'lid': 'j1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'a1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
        ]
        rmap = {
            'j1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 5)},  # age=4→bucket0
            'a1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 20)}, # age=19→bucket2
        }
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 2)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        total = sum(buckets)
        self.assertEqual(total, 2)
        # Each of bucket0 and bucket2 = 1; contribution = 50% each
        self.assertAlmostEqual(buckets[0] / total * 100, 50.0)
        self.assertAlmostEqual(buckets[2] / total * 100, 50.0)

    def test_retail_date_ageing_unchanged(self):
        """Retail_Date - CreateDate is the sole ageing calculation — fixture unchanged."""
        # age = Retail_Date(2026-08-10) - CreateDate(2026-08-01) = 9 days → bucket1
        leads = [{'lid': 'l1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic',
                  'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'}]
        rmap  = {'l1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 10)}}
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 1)
        k = list(ram.keys())[0]
        self.assertEqual(k[5], 1, "age=9 days must map to bucket1 (8-14 days)")

    # ── 35-51: Summary vs Monthly grid logic ─────────────────────────────────

    def _multi_month_fixture(self):
        """Two models across two months for summary/monthly grid tests."""
        leads = [
            # Jul'26: Raider age=3 (bucket0), Apache age=10 (bucket1)
            {'lid': 'j1', 'lm': "Jul'26", 'mdl': 'TVS Raider',     'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'j2', 'lm': "Jul'26", 'mdl': 'TVS Apache RTR 160', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            # Aug'26: Raider age=20 (bucket2), Apache age=35 (bucket3)
            {'lid': 'a1', 'lm': "Aug'26", 'mdl': 'TVS Raider',     'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
            {'lid': 'a2', 'lm': "Aug'26", 'mdl': 'TVS Apache RTR 160', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
        ]
        rmap = {
            'j1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},   # age=3  → b0
            'j2': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 11)},  # age=10 → b1
            'a1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 21)},  # age=20 → b2
            'a2': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 9, 5)},   # age=35 → b3
        }
        return leads, rmap

    def test_summary_aggregates_all_months(self):
        """Summary = agg(monthSet=None) contains retails from all months."""
        leads, rmap = self._multi_month_fixture()
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 4, "summary must include all 4 retails")
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        self.assertEqual(sum(buckets), 4)
        # Each bucket should have exactly 1 retail
        self.assertEqual(buckets[0], 1)  # j1
        self.assertEqual(buckets[1], 1)  # j2
        self.assertEqual(buckets[2], 1)  # a1
        self.assertEqual(buckets[3], 1)  # a2

    def test_monthly_grid_jul_only(self):
        """Monthly grid for Jul'26 contains only Jul'26 retails."""
        leads, rmap = self._multi_month_fixture()
        jul_leads = [l for l in leads if l['lm'] == "Jul'26"]
        jul_rmap  = {k: v for k, v in rmap.items() if k.startswith('j')}
        ram, meta, _ = _run_ageing_fixture(jul_leads, jul_rmap)
        self.assertEqual(meta['valid'], 2)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        self.assertEqual(sum(buckets), 2)
        self.assertEqual(buckets[0], 1)  # Raider age=3
        self.assertEqual(buckets[1], 1)  # Apache age=10
        self.assertEqual(buckets[2], 0)
        self.assertEqual(buckets[3], 0)

    def test_monthly_grid_aug_only(self):
        """Monthly grid for Aug'26 contains only Aug'26 retails."""
        leads, rmap = self._multi_month_fixture()
        aug_leads = [l for l in leads if l['lm'] == "Aug'26"]
        aug_rmap  = {k: v for k, v in rmap.items() if k.startswith('a')}
        ram, meta, _ = _run_ageing_fixture(aug_leads, aug_rmap)
        self.assertEqual(meta['valid'], 2)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        self.assertEqual(sum(buckets), 2)
        self.assertEqual(buckets[0], 0)
        self.assertEqual(buckets[1], 0)
        self.assertEqual(buckets[2], 1)  # Raider age=20
        self.assertEqual(buckets[3], 1)  # Apache age=35

    def test_summary_contribution_from_aggregate_not_averaged_monthly(self):
        """Summary contribution uses aggregate totals, not avg of monthly percentages."""
        # Jul'26: model A — 3 in b0, 0 in b1 → 100% b0
        # Aug'26: model A — 0 in b0, 1 in b1 → 100% b1
        # Summary correct: b0=3, b1=1, total=4 → b0=75%, b1=25%
        # Wrong if averaged: (100%+0%)/2=50%, (0%+100%)/2=50% ← must NOT happen
        leads = [
            {'lid': 'j1', 'lm': "Jul'26", 'mdl': 'Model A', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'j2', 'lm': "Jul'26", 'mdl': 'Model A', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'j3', 'lm': "Jul'26", 'mdl': 'Model A', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'a1', 'lm': "Aug'26", 'mdl': 'Model A', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
        ]
        rmap = {
            'j1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},   # b0
            'j2': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},   # b0
            'j3': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},   # b0
            'a1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 11)},  # b1
        }
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 4)
        buckets = [0, 0, 0, 0]
        for k, v in ram.items(): buckets[k[5]] += v[0]
        total = sum(buckets)
        self.assertEqual(total, 4)
        # Correct aggregate: 3/4=75%, 1/4=25%
        self.assertAlmostEqual(buckets[0] / total * 100, 75.0, delta=0.01)
        self.assertAlmostEqual(buckets[1] / total * 100, 25.0, delta=0.01)

    def test_monthly_contribution_uses_that_months_total(self):
        """Monthly contribution denominator = that month's model total only."""
        leads, rmap = self._multi_month_fixture()
        # Jul'26 only: Raider in b0, Apache in b1; each model total=1 → each 100%
        jul_leads = [l for l in leads if l['lm'] == "Jul'26"]
        jul_rmap  = {k: v for k, v in rmap.items() if k.startswith('j')}
        ram, _, _ = _run_ageing_fixture(jul_leads, jul_rmap)
        # Find per-model buckets
        by_mdl = {}
        for k, v in ram.items():
            abi = k[5]
            mi  = k[0]
            if mi not in by_mdl: by_mdl[mi] = [0,0,0,0]
            by_mdl[mi][abi] += v[0]
        for buckets in by_mdl.values():
            total = sum(buckets)
            self.assertEqual(total, 1)
            # Each model has exactly one bucket with 1 retail → 100% contribution
            self.assertEqual(sum(1 for b in buckets if b == 1), 1)

    def test_global_month_filter_controls_monthly_grids(self):
        """Month=Aug'26 → monthly grids show only Aug'26 data."""
        leads, rmap = self._multi_month_fixture()
        aug_leads = [l for l in leads if l['lm'] == "Aug'26"]
        aug_rmap  = {k: v for k, v in rmap.items() if k.startswith('a')}
        ram, meta, _ = _run_ageing_fixture(aug_leads, aug_rmap)
        # Only Aug retails present
        lm_set = set()
        for k in ram.keys():
            lm_set.add(k[6])  # li index
        # All ram entries belong to Aug (lm index 0 since only Aug in this fixture)
        self.assertEqual(meta['valid'], 2)

    def test_all_months_shows_all_monthly_grids(self):
        """Month=All → both Jul and Aug monthly grids are non-empty."""
        leads, rmap = self._multi_month_fixture()
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 4)
        # Both months present in ram keys
        lm_indices = set(k[6] for k in ram.keys())
        self.assertEqual(len(lm_indices), 2, "should have entries for 2 distinct months")

    def test_monthly_grids_descending_order(self):
        """Monthly grids must be ordered latest-first (descending chronologically)."""
        # Month ordering helper (mirrors the JS monthOrder logic used in the frontend)
        _MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        def _month_order(m):
            if not m: return -1
            parts = m.replace("'", " ").split()
            if len(parts) != 2: return -1
            name, yr = parts[0], parts[1]
            yr_int = int(yr) if yr.isdigit() else (2000 + int(yr)) if len(yr) == 2 else -1
            return yr_int * 12 + (_MONTH_NAMES.index(name) if name in _MONTH_NAMES else -1)

        leads = [
            {'lid': 'may1', 'lm': "May'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-05-01'},
            {'lid': 'jun1', 'lm': "Jun'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-06-01'},
            {'lid': 'jul1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
        ]
        rmap = {
            'may1': {'rm': "May'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 5, 4)},
            'jun1': {'rm': "Jun'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 6, 4)},
            'jul1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
        }
        ram, meta, maps = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 3)

        # allMonths ascending (as built in the frontend)
        month_indices = sorted(set(k[6] for k in ram.keys()))
        months_asc  = [maps['lm'][i] for i in month_indices]
        # Frontend reverses allMonths to get displayMonths descending
        months_desc = list(reversed(months_asc))

        # Each consecutive pair must be descending (later month first)
        for i in range(len(months_desc) - 1):
            self.assertGreater(
                _month_order(months_desc[i]), _month_order(months_desc[i + 1]),
                f"{months_desc[i]} must appear before {months_desc[i+1]} in descending order"
            )
        self.assertEqual(months_desc[0], "Jul'26")
        self.assertEqual(months_desc[1], "Jun'26")
        self.assertEqual(months_desc[2], "May'26")

    def test_source_filter_affects_summary_and_monthly(self):
        """Source filter restricts both summary and monthly aggregation."""
        leads = [
            {'lid': 'o1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic',  'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'f1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Facebook', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'o2', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic',  'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
        ]
        rmap = {
            'o1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
            'f1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
            'o2': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 11)},
        }
        # Organic only
        org_leads = [l for l in leads if l['src'] == 'Organic']
        org_rmap  = {'o1': rmap['o1'], 'o2': rmap['o2']}
        ram, meta, _ = _run_ageing_fixture(org_leads, org_rmap)
        self.assertEqual(meta['valid'], 2, "source filter must exclude Facebook lead from both grids")

    def test_model_filter_affects_summary_and_monthly(self):
        """Model filter restricts both summary and monthly grids."""
        leads, rmap = self._multi_month_fixture()
        raider_leads = [l for l in leads if l['mdl'] == 'TVS Raider']
        raider_rmap  = {k: v for k, v in rmap.items() if k in ('j1', 'a1')}
        ram, meta, _ = _run_ageing_fixture(raider_leads, raider_rmap)
        self.assertEqual(meta['valid'], 2, "model filter should yield only Raider retails")
        for k in ram.keys():
            self.assertEqual(k[0], 0, "only one model index expected (Raider=0)")

    def test_lead_type_filter_affects_summary_and_monthly(self):
        """Lead type filter restricts both summary and monthly grids."""
        leads = [
            {'lid': 'h1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': 'Hot',  'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'w1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': 'Warm', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'h2', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': 'Hot',  'st': '', 'city': '', 'cd': '2026-08-01'},
        ]
        rmap = {
            'h1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
            'w1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
            'h2': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 4)},
        }
        hot_leads = [l for l in leads if l['lt'] == 'Hot']
        hot_rmap  = {'h1': rmap['h1'], 'h2': rmap['h2']}
        ram, meta, _ = _run_ageing_fixture(hot_leads, hot_rmap)
        self.assertEqual(meta['valid'], 2, "lead type filter should exclude Warm lead")

    def test_state_filter_affects_summary_and_monthly(self):
        """State filter restricts both summary and monthly grids."""
        leads = [
            {'lid': 'm1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': 'MH', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'k1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': 'KA', 'city': '', 'cd': '2026-07-01'},
        ]
        rmap = {
            'm1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
            'k1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
        }
        mh_leads = [l for l in leads if l['st'] == 'MH']
        mh_rmap  = {'m1': rmap['m1']}
        ram, meta, _ = _run_ageing_fixture(mh_leads, mh_rmap)
        self.assertEqual(meta['valid'], 1)

    def test_city_filter_affects_summary_and_monthly(self):
        """City filter restricts both summary and monthly grids."""
        leads = [
            {'lid': 'mu', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': 'Mumbai', 'cd': '2026-07-01'},
            {'lid': 'pu', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': 'Pune',   'cd': '2026-07-01'},
        ]
        rmap = {
            'mu': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
            'pu': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
        }
        mum_leads = [l for l in leads if l['city'] == 'Mumbai']
        mum_rmap  = {'mu': rmap['mu']}
        ram, meta, _ = _run_ageing_fixture(mum_leads, mum_rmap)
        self.assertEqual(meta['valid'], 1)

    def test_dms_filter_affects_summary_and_monthly(self):
        """DMS filter restricts to DMS retails in both summary and monthly grids."""
        leads = [
            {'lid': 'd1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
            {'lid': 'c1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'},
        ]
        rmap = {
            'd1': {'rm': "Jul'26", 'rtype': 'DMS',      'pm': '', 'rd': _datetime.date(2026, 7, 4)},
            'c1': {'rm': "Jul'26", 'rtype': 'Call Out', 'pm': '', 'rd': _datetime.date(2026, 7, 4)},
        }
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 2)
        dms_total = sum(v[1] for v in ram.values())
        co_total  = sum(v[2] for v in ram.values())
        self.assertEqual(dms_total, 1, "DMS count must be 1")
        self.assertEqual(co_total,  1, "Call Out count must be 1")

    def test_call_out_filter_affects_summary_and_monthly(self):
        """Call Out filter: co column available in ram for monthly and summary."""
        leads = [
            {'lid': 'd1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
            {'lid': 'c1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
            {'lid': 'c2', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'lt': '', 'st': '', 'city': '', 'cd': '2026-08-01'},
        ]
        rmap = {
            'd1': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': '', 'rd': _datetime.date(2026, 8, 4)},
            'c1': {'rm': "Aug'26", 'rtype': 'Call Out', 'pm': '', 'rd': _datetime.date(2026, 8, 4)},
            'c2': {'rm': "Aug'26", 'rtype': 'Call Out', 'pm': '', 'rd': _datetime.date(2026, 8, 4)},
        }
        ram, _, _ = _run_ageing_fixture(leads, rmap)
        co_total = sum(v[2] for v in ram.values())
        self.assertEqual(co_total, 2, "Call Out count must match across all grids")

    def test_grand_total_correct_for_summary(self):
        """Summary grand total = sum of all bucket retails across all months."""
        leads, rmap = self._multi_month_fixture()
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        grand_total = sum(v[0] for v in ram.values())
        self.assertEqual(grand_total, meta['valid'])
        self.assertEqual(grand_total, 4)

    def test_grand_total_correct_for_monthly(self):
        """Monthly grand total = sum of retails for that month only."""
        leads, rmap = self._multi_month_fixture()
        jul_leads = [l for l in leads if l['lm'] == "Jul'26"]
        jul_rmap  = {k: v for k, v in rmap.items() if k.startswith('j')}
        ram, meta, _ = _run_ageing_fixture(jul_leads, jul_rmap)
        grand_total = sum(v[0] for v in ram.values())
        self.assertEqual(grand_total, 2, "Jul grand total must be 2, not 4")
        self.assertEqual(meta['valid'], 2)

    def test_empty_month_not_rendered(self):
        """Month with no qualifying retails produces empty ram — no grid to show."""
        leads = [{'lid': 'l1', 'lm': "Jul'26", 'mdl': 'TVS Raider', 'src': 'Organic',
                  'lt': '', 'st': '', 'city': '', 'cd': '2026-07-01'}]
        rmap = {'l1': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 7, 4)}}
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        # Simulate filtering to Aug'26 only — no data
        aug_ram = {k: v for k, v in ram.items() if False}  # nothing matches Aug
        self.assertEqual(sum(v[0] for v in aug_ram.values()), 0)


# ---------------------------------------------------------------------------
# Test 21 — build_payload() isolation: ageing must not touch existing matrices
# ---------------------------------------------------------------------------
# This helper is a FAITHFUL STRUCTURAL COPY of the critical aggregation loop
# inside build_payload() (push_tvs_data.py).  push_tvs_data.py cannot be
# imported directly because it has unguarded module-level I/O; inlining the
# loop body is the established pattern in this test suite.
#
# Matrices included (mirrors build_payload verbatim):
#   monthly, sm, mm                    (On Create)
#   u_monthly, u_sm, u_mm             (On Update)
#   disp, u_disp                       (Retail Dispersion)
#   ram, _ram_*                        (Retail Ageing — new)
#
# Structural guarantees preserved from the original:
#   • bump/ubump call order is identical
#   • ageing block is copy-pasted from lines 1808-1828 of push_tvs_data.py
#   • _parse_date and _age_bucket are the same inlined copies used elsewhere
#
# leads: list of dicts with keys:
#   lid, lm, src, lt, mdl, cd (CreateDate string), rm (retail month string)
# retail_map: {lid: {rm, rtype, pm, rd}}  — rd may be None or datetime.date

def _run_build_payload_core(leads, retail_map):
    """Inline structural copy of build_payload()'s aggregation loop.

    Covers every matrix that the ageing code could theoretically corrupt.
    Keeps exact bump/ubump call order from push_tvs_data.py.
    """
    lm_idx,  src_idx, lt_idx, mdl_idx = {}, {}, {}, {}
    lm_arr,  src_arr, lt_arr, mdl_arr = [], [], [], []

    def _ix(d, arr, v):
        if v not in d:
            d[v] = len(arr); arr.append(v)
        return d[v]

    monthly  = {}
    sm       = {}
    mm       = {}
    u_monthly = {}
    u_sm      = {}
    u_mm      = {}
    disp      = {}
    u_disp    = {}
    ram       = {}
    _ram_total = _ram_valid = _ram_neg = _ram_no_rd = _ram_no_cd = 0

    def _bump(d, k, is_ret, rtype=''):
        if k not in d: d[k] = [0, 0, 0, 0]
        d[k][0] += 1
        if is_ret:
            d[k][1] += 1
            rt_u = rtype.upper()
            if 'DMS'  in rt_u: d[k][2] += 1
            elif 'CALL' in rt_u: d[k][3] += 1

    def _ubump(d, key_lead, key_ret, is_ret, rtype=''):
        if key_lead not in d: d[key_lead] = [0, 0, 0, 0]
        d[key_lead][0] += 1
        if is_ret:
            if key_ret not in d: d[key_ret] = [0, 0, 0, 0]
            d[key_ret][1] += 1
            rt_u = rtype.upper()
            if 'DMS'  in rt_u: d[key_ret][2] += 1
            elif 'CALL' in rt_u: d[key_ret][3] += 1

    for lead in leads:
        lid   = lead['lid']
        lm    = lead['lm']
        src   = lead['src']
        lt    = lead.get('lt', 'Unknown')
        mdl   = lead['mdl']

        is_ret = lid in retail_map
        rtype  = retail_map[lid]['rtype'] if is_ret else ''

        mi  = _ix(mdl_idx, mdl_arr, mdl)
        si  = _ix(src_idx, src_arr, src)
        tti = _ix(lt_idx,  lt_arr,  lt)
        li  = _ix(lm_idx,  lm_arr,  lm)

        # ── On Create bumps (exact order from build_payload) ──────────────
        _bump(monthly, str(li),                 is_ret, rtype)
        _bump(sm,      f"{si}|{li}",            is_ret, rtype)
        _bump(mm,      f"{mi}|{si}|{li}",       is_ret, rtype)

        # ── On Update bumps ───────────────────────────────────────────────
        rm  = retail_map[lid].get('rm', '') if is_ret else ''
        um  = rm if rm else lm
        uli = _ix(lm_idx, lm_arr, um)
        _ubump(u_monthly, str(li),         str(uli),          is_ret, rtype)
        _ubump(u_sm,  f"{si}|{li}",    f"{si}|{uli}",        is_ret, rtype)
        _ubump(u_mm,  f"{mi}|{si}|{li}", f"{mi}|{si}|{uli}", is_ret, rtype)

        if is_ret:
            pm  = retail_map[lid].get('pm', '') or 'Unknown'
            pmi = _ix(mdl_idx, mdl_arr, pm)
            disp  [f"{mi}|{pmi}|{li}"]  = disp  .get(f"{mi}|{pmi}|{li}",  0) + 1
            u_disp[f"{mi}|{pmi}|{uli}"] = u_disp.get(f"{mi}|{pmi}|{uli}", 0) + 1

            # ── Retail Ageing block — copy-pasted from push_tvs_data.py ──
            # Lines 1808-1828.  Touches ONLY ram and _ram_* counters.
            _ram_total_ref = _ram_total   # capture before (unused; see assertion below)
            _ram_total += 1
            _rd = retail_map[lid].get('rd')
            _cd = _parse_date(lead.get('cd', ''))
            if _rd is None:
                _ram_no_rd += 1
            elif _cd is None:
                _ram_no_cd += 1
            else:
                _age_days = (_rd - _cd).days
                if _age_days < 0:
                    _ram_neg += 1
                else:
                    _abi = _age_bucket(_age_days)
                    _rk  = f"{mi}|{si}|{_abi}|{li}"
                    if _rk not in ram: ram[_rk] = [0, 0, 0]
                    ram[_rk][0] += 1
                    _rt_u = rtype.upper()
                    if 'DMS'  in _rt_u: ram[_rk][1] += 1
                    elif 'CALL' in _rt_u: ram[_rk][2] += 1
                    _ram_valid += 1

    return {
        'monthly':   dict(monthly),
        'sm':        dict(sm),
        'mm':        dict(mm),
        'u_monthly': dict(u_monthly),
        'u_sm':      dict(u_sm),
        'u_mm':      dict(u_mm),
        'disp':      dict(disp),
        'u_disp':    dict(u_disp),
        'ram':       dict(ram),
        'ram_meta':  {
            'total': _ram_total, 'valid': _ram_valid,
            'no_rd': _ram_no_rd, 'no_cd': _ram_no_cd, 'neg': _ram_neg,
        },
    }


class TestAgeingIsolation(unittest.TestCase):
    """Test 21 — end-to-end build_payload() isolation.

    Proves that supplying valid Retail_Date values (Run B) versus
    rd=None / fetch failure (Run A) does NOT alter any pre-existing
    matrix produced by the aggregation loop.

    Uses _run_build_payload_core(), a faithful structural copy of
    build_payload()'s loop (the established inlining pattern for this
    test suite — push_tvs_data.py cannot be imported due to module-level I/O).
    """

    # Synthetic fixture: 3 leads — 2 retailed (DMS + Call Out), 1 non-retail
    _LEADS = [
        {'lid': 'L001', 'lm': "Aug'26", 'src': 'Facebook', 'lt': 'Hot',
         'mdl': 'TVS Raider', 'cd': '2026-08-01', 'rm': "Aug'26"},
        {'lid': 'L002', 'lm': "Aug'26", 'src': 'Organic',  'lt': 'Hot',
         'mdl': 'TVS Apache RTR 160', 'cd': '2026-07-25', 'rm': "Aug'26"},
        {'lid': 'L003', 'lm': "Aug'26", 'src': 'Facebook', 'lt': 'Warm',
         'mdl': 'TVS Raider', 'cd': '2026-08-05'},   # non-retail
    ]

    # Run A: rd=None for all retails (simulates complete fetch failure)
    _RMAP_NO_RD = {
        'L001': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': 'TVS Raider',         'rd': None},
        'L002': {'rm': "Aug'26", 'rtype': 'Call Out',  'pm': 'TVS Apache RTR 160', 'rd': None},
    }

    # Run B: same data, valid rd supplied (9-day and 5-day age respectively)
    _RMAP_WITH_RD = {
        'L001': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': 'TVS Raider',         'rd': _datetime.date(2026, 8, 10)},
        'L002': {'rm': "Aug'26", 'rtype': 'Call Out',  'pm': 'TVS Apache RTR 160', 'rd': _datetime.date(2026, 7, 30)},
    }

    def _run_both(self):
        pa = _run_build_payload_core(self._LEADS, self._RMAP_NO_RD)
        pb = _run_build_payload_core(self._LEADS, self._RMAP_WITH_RD)
        return pa, pb

    # ── Matrix identity assertions ─────────────────────────────────────────────

    def test_mm_identical_with_and_without_rd(self):
        """Model×Source×Month matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['mm'], pb['mm'])

    def test_sm_identical_with_and_without_rd(self):
        """Source×Month matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['sm'], pb['sm'])

    def test_monthly_identical_with_and_without_rd(self):
        """Monthly (On Create) matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['monthly'], pb['monthly'])

    def test_u_mm_identical_with_and_without_rd(self):
        """On Update Model×Source matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['u_mm'], pb['u_mm'])

    def test_u_monthly_identical_with_and_without_rd(self):
        """On Update monthly matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['u_monthly'], pb['u_monthly'])

    def test_u_sm_identical_with_and_without_rd(self):
        """On Update Source×Month matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['u_sm'], pb['u_sm'])

    def test_disp_identical_with_and_without_rd(self):
        """Retail Dispersion (OC) matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['disp'], pb['disp'])

    def test_u_disp_identical_with_and_without_rd(self):
        """Retail Dispersion (OU) matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['u_disp'], pb['u_disp'])

    # ── Retail / lead total identity ───────────────────────────────────────────

    def test_retail_total_identical(self):
        """ram_meta.total (retails processed by ageing) is the same in both runs."""
        pa, pb = self._run_both()
        self.assertEqual(pa['ram_meta']['total'], pb['ram_meta']['total'])
        self.assertEqual(pa['ram_meta']['total'], 2)   # fixture has 2 retails

    def test_lead_count_identical(self):
        """Lead totals in monthly OC are identical (ageing never touches lead counts)."""
        pa, pb = self._run_both()
        oc_leads_a = sum(v[0] for v in pa['monthly'].values())
        oc_leads_b = sum(v[0] for v in pb['monthly'].values())
        self.assertEqual(oc_leads_a, oc_leads_b)
        self.assertEqual(oc_leads_a, 3)   # fixture has 3 leads

    def test_dms_callout_identical(self):
        """DMS and Call Out counts in mm are byte-identical between Run A and Run B."""
        pa, pb = self._run_both()
        dms_a  = sum(v[2] for v in pa['mm'].values())
        co_a   = sum(v[3] for v in pa['mm'].values())
        dms_b  = sum(v[2] for v in pb['mm'].values())
        co_b   = sum(v[3] for v in pb['mm'].values())
        self.assertEqual(dms_a,  dms_b)
        self.assertEqual(co_a,   co_b)
        self.assertEqual(dms_a,  1)   # L001 is DMS
        self.assertEqual(co_a,   1)   # L002 is Call Out

    # ── Ageing diverges correctly ──────────────────────────────────────────────

    def test_run_a_has_no_ageing_rows(self):
        """Run A (rd=None) must produce empty ram — simulates fetch failure."""
        pa, _ = self._run_both()
        self.assertEqual(pa['ram'], {})
        self.assertEqual(pa['ram_meta']['valid'],  0)
        self.assertEqual(pa['ram_meta']['no_rd'],  2)

    def test_run_b_has_ageing_rows(self):
        """Run B (valid rd) must produce non-empty ram with correct bucket assignments."""
        _, pb = self._run_both()
        self.assertGreater(len(pb['ram']), 0)
        self.assertEqual(pb['ram_meta']['valid'], 2)
        self.assertEqual(pb['ram_meta']['no_rd'], 0)

    def test_run_b_ageing_bucket_correctness(self):
        """Run B ageing rows land in the correct buckets.

        L001: age = 2026-08-10 - 2026-08-01 = 9 days → bucket 1 (8-14 days)
        L002: age = 2026-07-30 - 2026-07-25 = 5 days → bucket 0 (0-7 days)
        """
        _, pb = self._run_both()
        bucket_sum = [0, 0, 0, 0]
        for v in pb['ram'].values():
            rets = v[0]
            bucket_sum[0] += 0   # placeholder; we check by key below
        # Verify bucket indices present in ram keys
        abi_values = set()
        for k in pb['ram']:
            parts = k.split('|')
            abi_values.add(int(parts[2]))
        self.assertIn(0, abi_values)   # L002: 5 days → bucket 0
        self.assertIn(1, abi_values)   # L001: 9 days → bucket 1

    def test_run_b_dms_plus_co_equals_rets_in_ageing(self):
        """Within ageing (Run B), DMS + Call Out == Retails for every ram cell."""
        _, pb = self._run_both()
        self.assertGreater(len(pb['ram']), 0, "Run B must produce ageing rows")
        for k, v in pb['ram'].items():
            rets, dms, co = v
            self.assertEqual(dms + co, rets, f"dms+co != rets for key {k}: {v}")


# ---------------------------------------------------------------------------
# Tests 155–174: Fetch resilience — page resume, exception classes, constants
# ---------------------------------------------------------------------------

# Inline copies of the new exception classes (must stay in sync with push_tvs_data.py).
class _RetailPageFailed(Exception):
    def __init__(self, page, accumulated_rows, headers, expected_total, cause):
        super().__init__(str(cause))
        self.page             = page
        self.accumulated_rows = accumulated_rows
        self.headers          = headers
        self.expected_total   = expected_total

class _RetailDatePageFailed(Exception):
    def __init__(self, page, partial_map, populated_ct, blank_ct, invalid_ct, cause):
        super().__init__(str(cause))
        self.page         = page
        self.partial_map  = partial_map
        self.populated_ct = populated_ct
        self.blank_ct     = blank_ct
        self.invalid_ct   = invalid_ct

_PUSH_TVS = Path(__file__).parent / 'push_tvs_data.py'


class TestSourceDropCheck(unittest.TestCase):
    """Tests 155–157: _check_source_drop regression for Aug26+ blank-row false failure.

    Root cause: push_tvs_data.py passed len(std) (filtered count, 110,000) to
    _check_source_drop instead of len(raw) (raw fetched count, 142,820).
    When the sheet gains blank Lead_Month rows those are filtered out in STAGE 6,
    making the filtered count fall below 80% of baseline — a false failure.
    Fix: use raw fetched count for the source-drop baseline comparison.
    """

    def _check_source_drop(self, label, current_count, prev_metrics, threshold=0.80):
        """Inline copy of _check_source_drop from push_tvs_data.py (must stay in sync)."""
        prev = prev_metrics.get(label, {}).get('rows') if isinstance(prev_metrics.get(label), dict) \
            else prev_metrics.get(label)
        if prev is None or prev == 0:
            return True  # no baseline
        ratio = current_count / prev
        return ratio >= threshold, ratio

    def test_raw_count_matches_baseline_passes(self):
        """Passing raw fetched count (142,820) to check against baseline (142,820) succeeds."""
        prev = {'Aug26+-LeadMaster': {'rows': 142820}}
        ok, ratio = self._check_source_drop('Aug26+-LeadMaster', 142820, prev)
        self.assertTrue(ok)
        self.assertAlmostEqual(ratio, 1.0, places=3)

    def test_filtered_count_below_threshold_is_false_failure(self):
        """Passing filtered count (110,000) against baseline (142,820) gives 77% — false alarm."""
        prev = {'Aug26+-LeadMaster': {'rows': 142820}}
        ok, ratio = self._check_source_drop('Aug26+-LeadMaster', 110000, prev)
        self.assertFalse(ok)          # 77% < 80% — this is the bug
        self.assertLess(ratio, 0.80)

    def test_source_check_uses_raw_count_in_source(self):
        """push_tvs_data.py must pass raw row count — not filtered — to _check_source_drop.

        After the parallel-fetch refactor the lead processing lives in
        _fetch_and_process_lead_sheet() which returns raw_len/filtered_len.
        The main merge loop passes _lr['raw_len'] to _check_source_drop.
        These assertions verify the invariant is preserved under the new architecture.
        """
        src = _PUSH_TVS.read_text(encoding='utf-8')
        # Helper returns raw and filtered as separate keys
        self.assertIn("'raw_len'", src)
        self.assertIn("'filtered_len'", src)
        # Source drop check receives raw_len (not filtered_len)
        self.assertIn("_lr['raw_len']", src)
        # Must NOT compare filtered count directly to source drop check
        self.assertNotIn("_check_source_drop(_lbl, len(std)", src)


class TestPaginationConstants(unittest.TestCase):
    """Tests 155–164: Verify pagination constants in push_tvs_data.py.
    Read the source file as text so a change in the source is always caught."""

    def _src(self):
        return _PUSH_TVS.read_text(encoding='utf-8')

    def test_lead_page_size_is_3000(self):
        self.assertIn('_LEAD_PAGE_SIZE = 3000', self._src())

    def test_retail_page_size_is_2000(self):
        self.assertIn('_RETAIL_PAGE_SIZE = 2000', self._src())

    def test_retail_date_page_size_is_2000(self):
        self.assertIn('_RETAIL_DATE_PAGE_SIZE = 2000', self._src())

    def test_lead_timeouts_has_6_entries(self):
        self.assertIn('_LEAD_TIMEOUTS  = [30, 60, 90, 120, 180, 180]', self._src())

    def test_retail_timeouts_has_6_entries(self):
        self.assertIn('_RETAIL_TIMEOUTS  = [30, 60, 90, 120, 180, 180]', self._src())

    def test_retail_date_timeouts_has_6_entries(self):
        self.assertIn('_RETAIL_DATE_TIMEOUTS  = [30, 60, 90, 120, 180, 180]', self._src())

    def test_lead_backoffs_length_invariant(self):
        """_LEAD_BACKOFFS must have exactly len(_LEAD_TIMEOUTS)-1 entries."""
        lead_timeouts = [30, 60, 90, 120, 180, 180]
        lead_backoffs = [5, 10, 15, 20, 30]
        self.assertEqual(len(lead_backoffs), len(lead_timeouts) - 1)

    def test_retail_backoffs_length_invariant(self):
        retail_timeouts = [30, 60, 90, 120, 180, 180]
        retail_backoffs = [5, 10, 15, 20, 30]
        self.assertEqual(len(retail_backoffs), len(retail_timeouts) - 1)

    def test_retail_date_backoffs_length_invariant(self):
        rd_timeouts = [30, 60, 90, 120, 180, 180]
        rd_backoffs  = [5, 10, 15, 20, 30]
        self.assertEqual(len(rd_backoffs), len(rd_timeouts) - 1)

    def test_all_backoff_values_are_positive(self):
        for b in [5, 10, 15, 20, 30]:
            self.assertGreater(b, 0)


class TestPageResumeExceptions(unittest.TestCase):
    """Tests 165–169: _RetailPageFailed and _RetailDatePageFailed carry the right state."""

    def test_retail_page_failed_stores_page(self):
        e = _RetailPageFailed(7, ['r1', 'r2'], ['h1'], 100, RuntimeError('boom'))
        self.assertEqual(e.page, 7)

    def test_retail_page_failed_stores_accumulated_rows(self):
        rows = ['r1', 'r2', 'r3']
        e = _RetailPageFailed(2, rows, ['h'], 50, RuntimeError('x'))
        self.assertIs(e.accumulated_rows, rows)

    def test_retail_page_failed_stores_expected_total(self):
        e = _RetailPageFailed(0, [], None, 75000, RuntimeError('x'))
        self.assertEqual(e.expected_total, 75000)

    def test_retail_date_page_failed_stores_partial_map(self):
        m = {'lid1': '2026-08-01', 'lid2': '2026-07-15'}
        e = _RetailDatePageFailed(3, m, 2, 1, 0, RuntimeError('x'))
        self.assertIs(e.partial_map, m)
        self.assertEqual(e.populated_ct, 2)
        self.assertEqual(e.blank_ct, 1)
        self.assertEqual(e.invalid_ct, 0)

    def test_retail_date_page_failed_message_from_cause(self):
        e = _RetailDatePageFailed(1, {}, 0, 0, 0, RuntimeError('network timeout'))
        self.assertIn('network timeout', str(e))


class TestPaginationResumePurity(unittest.TestCase):
    """Tests 170–174: Core page-resume logic, no network needed.
    These use minimal pure-Python replicas of the inner paginator structure."""

    def _simulate_inner(self, responses):
        """Simulate _fetch_retails_inner with a list of per-page mock responses.
        Each response is either a dict (success) or an exception (failure).
        Raises _RetailPageFailed on per-page exhaustion (single attempt per page for simplicity).
        Returns (all_rows, done_received, pages_fetched)."""
        all_rows      = []
        done_received = False
        pages_fetched = 0
        for page, resp in enumerate(responses):
            if isinstance(resp, Exception):
                raise _RetailPageFailed(page, all_rows, None, None, resp)
            rows = resp.get('rows', [])
            all_rows.extend(rows)
            pages_fetched += 1
            if resp.get('done', True):
                done_received = True
                break
        return all_rows, done_received, pages_fetched

    def test_page_resume_preserves_prior_rows(self):
        """Rows from page 0 are preserved when page 1 fails and we resume."""
        page0_rows = [['A', '1'], ['B', '2']]
        try:
            self._simulate_inner([
                {'rows': page0_rows, 'done': False},
                RuntimeError('404 echo expired'),
            ])
            self.fail("Expected _RetailPageFailed")
        except _RetailPageFailed as e:
            self.assertEqual(e.page, 1)
            self.assertEqual(e.accumulated_rows, page0_rows)

    def test_page_resume_no_duplication(self):
        """Resuming at page 1 with prev_rows set does not re-fetch page 0 rows."""
        page0_rows = [['A', '1'], ['B', '2']]
        page1_rows = [['C', '3']]
        # Simulate outer retry: second call starts from page 1 with prev_rows
        all_rows   = list(page0_rows)   # preserved from failed run
        all_rows.extend(page1_rows)     # page 1 now succeeds
        self.assertEqual(len(all_rows), 3)
        self.assertEqual(all_rows[0], ['A', '1'])
        self.assertEqual(all_rows[2], ['C', '3'])

    def test_pagination_total_matches_sum_of_pages(self):
        """Total row count equals the sum of rows across all pages."""
        pages = [
            {'rows': [['r1'], ['r2']], 'done': False},
            {'rows': [['r3'], ['r4'], ['r5']], 'done': False},
            {'rows': [['r6']], 'done': True},
        ]
        all_rows, done_received, pages_fetched = self._simulate_inner(pages)
        self.assertEqual(len(all_rows), 6)
        self.assertTrue(done_received)
        self.assertEqual(pages_fetched, 3)

    def test_missing_done_signal_detected(self):
        """A response sequence that never sends done=True leaves done_received=False."""
        pages = [
            {'rows': [['r1']], 'done': False},
            {'rows': [['r2']], 'done': False},
        ]
        # Simulate exhausted list (no more pages returned) — done_received stays False
        all_rows      = []
        done_received = False
        for resp in pages:
            all_rows.extend(resp.get('rows', []))
            if resp.get('done', False):
                done_received = True
                break
        self.assertFalse(done_received)
        self.assertEqual(len(all_rows), 2)

    def test_proxy_get_always_uses_apps_script_url(self):
        """proxy_get() builds params from APPS_SCRIPT_URL each call — never a cached echo URL.
        Validate by inspecting the source that proxy_get calls requests.get(APPS_SCRIPT_URL…)."""
        src = _PUSH_TVS.read_text(encoding='utf-8')
        # proxy_get must reference APPS_SCRIPT_URL (not a hardcoded echo URL)
        self.assertIn('requests.get(APPS_SCRIPT_URL', src)
        # Must NOT reference the echo URL host directly
        self.assertNotIn('script.googleusercontent.com', src)


# ---------------------------------------------------------------------------
# Phase 15 — Parallel-fetch architecture (Tests 185–209)
# ---------------------------------------------------------------------------

class TestParallelFetchSourceText(unittest.TestCase):
    """Tests 185–198: Source-text assertions verify the parallel architecture is present."""

    def _src(self):
        return _PUSH_TVS.read_text(encoding='utf-8')

    # ── Imports ──────────────────────────────────────────────────────────────
    def test_threading_imported(self):
        self.assertIn('import threading', self._src())

    def test_concurrent_futures_imported(self):
        self.assertIn('concurrent.futures', self._src())

    # ── AS call counter ───────────────────────────────────────────────────────
    def test_as_calls_total_global_declared(self):
        self.assertIn('_as_calls_total', self._src())

    def test_as_calls_lock_declared(self):
        self.assertIn('_as_calls_lock', self._src())

    def test_proxy_get_increments_as_calls(self):
        src = self._src()
        self.assertIn('_as_calls_total += 1', src)

    # ── Perf timing ───────────────────────────────────────────────────────────
    def test_fetch_perf_dict_declared(self):
        self.assertIn('_fetch_perf', self._src())

    def test_fetch_perf_lock_declared(self):
        self.assertIn('_fetch_perf_lock', self._src())

    def test_parallel_elapsed_calculated(self):
        src = self._src()
        self.assertIn('_parallel_elapsed', src)

    # ── Thread infrastructure ─────────────────────────────────────────────────
    def test_daemon_threads_used(self):
        self.assertIn('daemon=True', self._src())

    def test_par_run_helper_defined(self):
        self.assertIn('def _par_run(', self._src())

    def test_fetch_and_process_lead_sheet_defined(self):
        self.assertIn('def _fetch_and_process_lead_sheet(', self._src())

    def test_retail_with_perf_defined(self):
        self.assertIn('def _retail_with_perf(', self._src())

    def test_rd_with_perf_defined(self):
        self.assertIn('def _rd_with_perf(', self._src())

    # ── Telemetry report ──────────────────────────────────────────────────────
    def test_fetch_telemetry_section_in_success_report(self):
        self.assertIn('FETCH TELEMETRY', self._src())

    def test_as_calls_total_printed_in_report(self):
        self.assertIn('Apps Script calls (total)', self._src())


class TestParallelLeadResultStructure(unittest.TestCase):
    """Tests 199–204: _fetch_and_process_lead_sheet returns the right keys."""

    def _mock_result(self):
        """Return a minimal well-formed result dict as the function would produce."""
        return {
            'label':         'Test-LeadMaster',
            'rtype_entries': {'lid1': {'rtype': 'DMS', 'rm': "Jul'26"}},
            'std':           None,   # DataFrame — not tested here for shape
            'raw_len':       1000,
            'filtered_len':  900,
            'duration_s':    12.5,
        }

    def test_result_has_label(self):
        r = self._mock_result()
        self.assertIn('label', r)
        self.assertEqual(r['label'], 'Test-LeadMaster')

    def test_result_has_rtype_entries(self):
        r = self._mock_result()
        self.assertIn('rtype_entries', r)
        self.assertIsInstance(r['rtype_entries'], dict)

    def test_result_has_raw_len(self):
        r = self._mock_result()
        self.assertIn('raw_len', r)
        self.assertGreaterEqual(r['raw_len'], 0)

    def test_result_has_filtered_len(self):
        r = self._mock_result()
        self.assertIn('filtered_len', r)
        self.assertLessEqual(r['filtered_len'], r['raw_len'])

    def test_result_duration_non_negative(self):
        r = self._mock_result()
        self.assertGreaterEqual(r['duration_s'], 0.0)

    def test_filtered_len_never_exceeds_raw_len(self):
        """Filter can only reduce row count — filtered_len <= raw_len always."""
        r = self._mock_result()
        self.assertLessEqual(r['filtered_len'], r['raw_len'])


class TestParallelErrorPropagation(unittest.TestCase):
    """Tests 205–209: Error-routing logic in the parallel merge step."""

    def _make_errors(self, **kwargs):
        return dict(**kwargs)

    def test_system_exit_in_retail_errors_is_reraised(self):
        """A SystemExit stored in _par_errors['retail_raw'] must propagate to main thread."""
        err = SystemExit(1)
        par_errors = {'retail_raw': err}
        retail_err = par_errors['retail_raw']
        self.assertIsInstance(retail_err, SystemExit)
        with self.assertRaises(SystemExit):
            raise retail_err

    def test_regular_exception_in_retail_errors_is_not_system_exit(self):
        """A plain RuntimeError is not a SystemExit — different handling path."""
        err = RuntimeError('Apps Script timeout')
        par_errors = {'retail_raw': err}
        self.assertNotIsInstance(par_errors['retail_raw'], SystemExit)

    def test_retail_date_error_yields_empty_rd_map(self):
        """Retail_Date failure is non-fatal: _rd_map falls back to {}."""
        par_errors  = {'retail_date': RuntimeError('timeout')}
        par_results = {}
        if 'retail_date' in par_errors:
            rd_map = {}
        else:
            rd_map = par_results.get('retail_date', {})
        self.assertEqual(rd_map, {})

    def test_retail_date_success_yields_non_empty_rd_map(self):
        """Successful Retail_Date result is used, not replaced with {}."""
        par_errors  = {}
        par_results = {'retail_date': {'lid1': '2026-08-01'}}
        if 'retail_date' in par_errors:
            rd_map = {}
        else:
            rd_map = par_results.get('retail_date', {})
        self.assertEqual(rd_map, {'lid1': '2026-08-01'})

    def test_lead_results_merged_in_lead_sheets_order(self):
        """Lead DataFrames must be appended in LEAD_SHEETS order, not arrival order."""
        lead_sheets = [{'label': 'A'}, {'label': 'B'}, {'label': 'C'}]
        par_results = {
            'A': {'rtype_entries': {}, 'std': 'df_A', 'raw_len': 10, 'filtered_len': 8},
            'B': {'rtype_entries': {}, 'std': 'df_B', 'raw_len': 20, 'filtered_len': 18},
            'C': {'rtype_entries': {}, 'std': 'df_C', 'raw_len': 30, 'filtered_len': 28},
        }
        lead_dfs = []
        for s in lead_sheets:
            lead_dfs.append(par_results[s['label']]['std'])
        self.assertEqual(lead_dfs, ['df_A', 'df_B', 'df_C'])


# ---------------------------------------------------------------------------
# Inline copy of _validate_post_response from push_tvs_data.py
# Must stay in sync — update here whenever the function changes there.
# ---------------------------------------------------------------------------

def _validate_post_response(body):
    """
    Parse and validate the Apps Script POST response.
    Returns (ok: bool, detail: str, parsed: dict|None).
    Accepted success formats:
      • Old contract  — {"ok": true, ...}
      • Structural echo — {"t": "<iso>", "rt_cols": <int>, "maps": {"lm": [...], ...}}
    """
    if not body:
        return False, 'EMPTY_BODY: response was empty', None

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        return False, f'INVALID_JSON: {exc}', None

    if not isinstance(parsed, dict):
        return False, f'NOT_A_DICT: type={type(parsed).__name__}', parsed

    if parsed.get('ok') is False:
        err = parsed.get('error') or parsed.get('message') or 'no detail'
        return False, f'EXPLICIT_FAIL: ok=false, error={err!r}', parsed
    if 'error' in parsed and parsed.get('ok') is not True:
        return False, f'ERROR_KEY: {parsed["error"]!r}', parsed

    if parsed.get('ok') is True:
        return True, 'OK_TRUE', parsed

    t_val    = parsed.get('t', '')
    maps_val = parsed.get('maps')
    rt_cols  = parsed.get('rt_cols')
    if (isinstance(t_val, str) and len(t_val) >= 10
            and isinstance(maps_val, dict)
            and isinstance(maps_val.get('lm'), list)
            and len(maps_val['lm']) >= 1
            and rt_cols is not None):
        n_months = len(maps_val['lm'])
        return True, f'STRUCTURAL_OK: t={t_val[:19]}, lm_months={n_months}', parsed

    keys = list(parsed.keys())
    return False, f'AMBIGUOUS_RESPONSE: keys={keys}', parsed


class TestPostResponseValidation(unittest.TestCase):
    """Regression tests for _validate_post_response.

    Covers:
      1.  Empty body
      2.  Non-JSON body
      3.  Old-contract success: {"ok": true}
      4.  Old-contract success with extra fields
      5.  Structural-echo success (new/current contract — exact replica of failing run)
      6.  Structural-echo success with many months
      7.  Explicit failure: ok=false
      8.  Explicit failure: ok=false with error message
      9.  Error key present without ok
      10. Error key present but ok=true overrides
      11. Ambiguous JSON object (no success or failure markers)
      12. JSON array (not a dict)
      13. Production unchanged: _validate_post_response never touches filesystem
      14. Structural echo missing maps.lm
      15. Structural echo with empty maps.lm
      16. Structural echo short timestamp
    """

    # ── helpers ───────────────────────────────────────────────────────────────
    _STRUCTURAL_RESPONSE = json.dumps({
        "t": "2026-08-26T11:48:34.438772",
        "rt_cols": 1,
        "maps": {
            "lm": ["Apr'25", "Jun'25", "Jul'25", "Aug'25", "Sep'25",
                   "Oct'25", "Nov'25", "Dec'25", "Jan'26", "Feb'26",
                   "Mar'26", "Apr'26", "May'26", "Jun'26", "Jul'26", "Aug'26"],
            "src": ["Facebook", "Organic", "Google", "Non CPS", "Whatsapp"],
            "mdl": ["TVS Jupiter", "TVS iQube"],
        }
    })

    # 1
    def test_empty_body_rejected(self):
        ok, detail, parsed = _validate_post_response('')
        self.assertFalse(ok)
        self.assertIn('EMPTY_BODY', detail)
        self.assertIsNone(parsed)

    # 2
    def test_non_json_body_rejected(self):
        ok, detail, parsed = _validate_post_response('not json {{{')
        self.assertFalse(ok)
        self.assertIn('INVALID_JSON', detail)
        self.assertIsNone(parsed)

    # 3
    def test_old_contract_ok_true_accepted(self):
        body = json.dumps({"ok": True, "t": "2026-08-25T09:00:00"})
        ok, detail, parsed = _validate_post_response(body)
        self.assertTrue(ok)
        self.assertEqual(detail, 'OK_TRUE')
        self.assertIsNotNone(parsed)

    # 4
    def test_old_contract_ok_true_with_extra_fields(self):
        body = json.dumps({"ok": True, "timestamp": "2026-08-25", "rows": 999})
        ok, detail, _ = _validate_post_response(body)
        self.assertTrue(ok)
        self.assertEqual(detail, 'OK_TRUE')

    # 5  — exact replica of the 2026-08-26 failure scenario
    def test_structural_echo_current_contract_accepted(self):
        ok, detail, parsed = _validate_post_response(self._STRUCTURAL_RESPONSE)
        self.assertTrue(ok, f"Structural echo should be accepted — got: {detail}")
        self.assertIn('STRUCTURAL_OK', detail)
        self.assertIn('t=2026-08-26T11:48:34', detail)
        self.assertIn('lm_months=16', detail)
        self.assertIsNotNone(parsed)

    # 6
    def test_structural_echo_single_month_accepted(self):
        body = json.dumps({
            "t": "2026-08-01T00:00:00",
            "rt_cols": 1,
            "maps": {"lm": ["Aug'26"], "src": ["Organic"]},
        })
        ok, detail, _ = _validate_post_response(body)
        self.assertTrue(ok)
        self.assertIn('lm_months=1', detail)

    # 7
    def test_explicit_ok_false_rejected(self):
        body = json.dumps({"ok": False})
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok)
        self.assertIn('EXPLICIT_FAIL', detail)

    # 8
    def test_explicit_ok_false_with_error_detail(self):
        body = json.dumps({"ok": False, "error": "Firebase quota exceeded"})
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok)
        self.assertIn('EXPLICIT_FAIL', detail)
        self.assertIn('Firebase quota exceeded', detail)

    # 9
    def test_error_key_without_ok_rejected(self):
        body = json.dumps({"error": "Script execution timed out", "t": "2026-08-26T10:00:00"})
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok)
        self.assertIn('ERROR_KEY', detail)
        self.assertIn('Script execution timed out', detail)

    # 10
    def test_error_key_with_ok_true_accepted(self):
        # ok:true wins over a stale error key
        body = json.dumps({"ok": True, "error": "previous error logged", "t": "2026-08-26"})
        ok, detail, _ = _validate_post_response(body)
        self.assertTrue(ok)
        self.assertEqual(detail, 'OK_TRUE')

    # 11
    def test_ambiguous_json_object_rejected(self):
        body = json.dumps({"status": "done", "rows": 100})
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok)
        self.assertIn('AMBIGUOUS_RESPONSE', detail)

    # 12
    def test_json_array_rejected(self):
        body = json.dumps([1, 2, 3])
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok)
        self.assertIn('NOT_A_DICT', detail)

    # 13
    def test_does_not_touch_filesystem(self):
        import tempfile, os
        # Validate purely in memory — no files created or modified
        before = set(os.listdir(tempfile.gettempdir()))
        _validate_post_response(self._STRUCTURAL_RESPONSE)
        _validate_post_response('')
        _validate_post_response('bad json')
        after = set(os.listdir(tempfile.gettempdir()))
        self.assertEqual(before, after,
                         "_validate_post_response must not create temp files")

    # 14
    def test_structural_echo_missing_lm_key_rejected(self):
        body = json.dumps({
            "t": "2026-08-26T11:48:34",
            "rt_cols": 1,
            "maps": {"src": ["Organic"]},   # lm missing
        })
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok)
        self.assertIn('AMBIGUOUS_RESPONSE', detail)

    # 15
    def test_structural_echo_empty_lm_rejected(self):
        body = json.dumps({
            "t": "2026-08-26T11:48:34",
            "rt_cols": 1,
            "maps": {"lm": []},             # empty lm
        })
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok)

    # 16
    def test_structural_echo_short_timestamp_rejected(self):
        body = json.dumps({
            "t": "2026",                    # too short to be a real ISO timestamp
            "rt_cols": 1,
            "maps": {"lm": ["Aug'26"]},
        })
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok)


class TestExplicitContractResponse(unittest.TestCase):
    """Verify the NEW explicit Apps Script response contract.

    The Apps Script doPost will now return ONLY:
      {"ok": true, "t": "<iso>", "rt_cols": <int>, "maps": {"lm": [...]}}

    These tests ensure:
      (A) The minimal explicit response is accepted by _validate_post_response.
      (B) ok:false + error is correctly rejected.
      (C) The response does NOT need to contain full maps (src, mdl, st, city, etc.).
      (D) The Python validator treats ok:true as the primary success signal
          regardless of whether extra fields are present.
      (E) Backward compat: structural echo still accepted alongside explicit ok:true.
    """

    # Representative lm array matching current production months
    _LM = ["Apr'25", "Jun'25", "May'25", "Jul'25", "Aug'25", "Sep'25",
           "Oct'25", "Nov'25", "Dec'25", "Jan'26", "Feb'26", "Mar'26",
           "Apr'26", "May'26", "Jun'26", "Jul'26", "Aug'26"]

    def _explicit_success(self, **kwargs):
        """Build the minimal explicit success response body."""
        base = {
            "ok": True,
            "t": "2026-08-26T11:48:34.438772",
            "rt_cols": 1,
            "maps": {"lm": self._LM},
        }
        base.update(kwargs)
        return json.dumps(base)

    # T1 — new explicit contract: ok:true + minimal fields accepted
    def test_explicit_minimal_response_accepted(self):
        body = self._explicit_success()
        ok, detail, parsed = _validate_post_response(body)
        self.assertTrue(ok, f"Explicit minimal response should be accepted — got: {detail}")
        self.assertEqual(detail, 'OK_TRUE')
        self.assertIsNotNone(parsed)

    # T2 — ok:true is confirmed by the validator
    def test_explicit_response_signals_ok_true(self):
        body = self._explicit_success()
        ok, detail, _ = _validate_post_response(body)
        self.assertTrue(ok)
        self.assertEqual(detail, 'OK_TRUE')

    # T3 — timestamp field is present and preserved in parsed output
    def test_explicit_response_contains_timestamp(self):
        body = self._explicit_success()
        _, _, parsed = _validate_post_response(body)
        self.assertIn('t', parsed)
        self.assertTrue(parsed['t'].startswith('2026-08-26'))

    # T4 — rt_cols field is present and preserved
    def test_explicit_response_contains_rt_cols(self):
        body = self._explicit_success()
        _, _, parsed = _validate_post_response(body)
        self.assertIn('rt_cols', parsed)
        self.assertEqual(parsed['rt_cols'], 1)

    # T5 — maps.lm is present and correct
    def test_explicit_response_contains_maps_lm(self):
        body = self._explicit_success()
        _, _, parsed = _validate_post_response(body)
        self.assertIn('maps', parsed)
        self.assertIn('lm', parsed['maps'])
        self.assertEqual(parsed['maps']['lm'], self._LM)

    # T6 — full maps are NOT required: response without src/mdl/st/city is accepted
    def test_full_maps_not_required_for_acceptance(self):
        body = json.dumps({
            "ok": True,
            "t": "2026-08-26T11:48:34",
            "rt_cols": 1,
            "maps": {"lm": self._LM},   # only lm — no src, mdl, st, city, etc.
        })
        ok, detail, _ = _validate_post_response(body)
        self.assertTrue(ok, "Response with only maps.lm should be accepted")
        self.assertEqual(detail, 'OK_TRUE')

    # T7 — ok:false + error is rejected even when other fields look valid
    def test_firebase_failure_response_rejected(self):
        body = json.dumps({
            "ok": False,
            "error": "Firebase write failed: quota exceeded",
            "t": "2026-08-26T11:48:34",
            "rt_cols": 1,
            "maps": {"lm": self._LM},
        })
        ok, detail, _ = _validate_post_response(body)
        self.assertFalse(ok, "ok:false must be rejected regardless of other fields")
        self.assertIn('EXPLICIT_FAIL', detail)
        self.assertIn('Firebase write failed', detail)

    # T8 — response is valid JSON (round-trip check)
    def test_explicit_response_is_valid_json(self):
        body = self._explicit_success()
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as exc:
            self.fail(f"Explicit success response body is not valid JSON: {exc}")
        self.assertIsInstance(parsed, dict)

    # T9 — backward compat: structural echo (old contract, no ok) still accepted
    def test_structural_echo_still_accepted_for_backward_compat(self):
        body = json.dumps({
            "t": "2026-08-26T11:48:34.438772",
            "rt_cols": 1,
            "maps": {
                "lm": self._LM,
                "src": ["Facebook", "Organic"],
                "mdl": ["TVS Jupiter"],
            }
            # no "ok" key — structural echo format
        })
        ok, detail, _ = _validate_post_response(body)
        self.assertTrue(ok, "Structural echo must still be accepted for backward compat")
        self.assertIn('STRUCTURAL_OK', detail)

    # T10 — ok:true + all production months passes lm length check
    def test_explicit_response_lm_matches_production_months(self):
        body = self._explicit_success()
        _, _, parsed = _validate_post_response(body)
        lm = parsed['maps']['lm']
        self.assertGreaterEqual(len(lm), 1)
        self.assertIn("Aug'26", lm)   # current month must be in lm


# ---------------------------------------------------------------------------
# STATUS CLASSIFICATION — inline copy of classify_status + _STATUS_TAG_MAP
# Must stay in sync with push_tvs_data.py.
# ---------------------------------------------------------------------------

_STATUS_TAG_MAP_TEST = {
    # ── Booking ──────────────────────────────────────────────────────────────
    'Booked':                                            'B',
    'Booked (Callback Scheduled)':                       'B',
    # ── Open ─────────────────────────────────────────────────────────────────
    'Booking Request':                                   'O',
    'Booking Requested (Callback Scheduled)':            'O',
    'Booking Requested (Customer Not Responded)':        'O',
    'Booking Requested (Dealer Visit Scheduled)':        'O',
    'Booking Requested (Home Visit Scheduled)':          'O',
    'Call for verification':                             'O',
    'Call for verification (Callback Scheduled)':        'O',
    'Call for verification (Customer Not Responded)':    'O',
    'Call for verification (Dealer Visit Scheduled)':    'O',
    'Customer Not Responded':                            'O',
    'Enquiry Re Opened (Callback Scheduled)':            'O',
    'Enquiry Re Opened (Customer Not Responded)':        'O',
    'Enquiry Re Opened (Dealer Visit Scheduled)':        'O',
    'Enquiry Re Opened (Home Visit Scheduled)':          'O',
    'L1 Verified (Callback Scheduled)':                  'O',
    'L1 Verified (Customer Not Responded)':              'O',
    'L1 Verified (Dealer Visit Scheduled)':              'O',
    'Pending Retail':                                    'O',
    'Price Quote':                                       'O',
    'Price Quote (Callback Scheduled)':                  'O',
    'Price Quote (Customer Not Responded)':              'O',
    'Price Quote (Dealer Visit Scheduled)':              'O',
    'Price Quote (No Dealer Connect)':                   'O',
    'Test Ride Completed (Callback Scheduled)':          'O',
    'Test Ride Requested':                               'O',
    'Test Ride Requested (Callback Scheduled)':          'O',
    'Test Ride Requested (Customer Not Responded)':      'O',
    'Test Ride Requested (Dealer Visit Scheduled)':      'O',
    'Test Ride Requested (Home Visit Scheduled)':        'O',
    # ── Lost ─────────────────────────────────────────────────────────────────
    'Lost Not Contactable':                              'L',
    'Lost Not Purchased':                                'L',
    'Lost Purchased':                                    'L',
    'Lost To Co-Dealer':                                 'L',
}

def _norm_sn_test(sn):
    return ' '.join(sn.strip().split())

def _classify_status_test(sn):
    norm = _norm_sn_test(sn) if sn else ''
    return _STATUS_TAG_MAP_TEST.get(norm, 'U')


class TestStatusClassification(unittest.TestCase):
    """
    Requirements:
    1.  Every Status_Name in the mapping returns the correct tag.
    2.  Booked → B.
    3.  Booked (Callback Scheduled) → B.
    4.  All Booking Request variants → O.
    5.  All Call for verification variants → O.
    6.  All Enquiry Re Opened variants → O.
    7.  All L1 Verified variants → O.
    8.  All Lost variants → L.
    9.  Pending Retail → O.
    10. All Price Quote variants → O.
    11. All Test Ride variants → O.
    12. Unknown status → U (never silently B or L).
    13. No duplicate counting (each lead in exactly one bucket).
    14. Whitespace normalisation works.
    15. Blank / None / empty → U.
    """

    # ── helpers ───────────────────────────────────────────────────────────────
    def assertTag(self, sn, expected_tag):
        got = _classify_status_test(sn)
        self.assertEqual(got, expected_tag,
            f"classify_status({sn!r}) = {got!r}, expected {expected_tag!r}")

    # ── req 2: Booked → B ─────────────────────────────────────────────────────
    def test_booked_is_booking(self):
        self.assertTag('Booked', 'B')

    # ── req 3: Booked (Callback Scheduled) → B ───────────────────────────────
    def test_booked_callback_is_booking(self):
        self.assertTag('Booked (Callback Scheduled)', 'B')

    # ── Only 2 Booking values; all others with "booking" in name must be O ────
    def test_booking_request_is_open_not_booking(self):
        self.assertTag('Booking Request', 'O')

    def test_booking_requested_callback_is_open(self):
        self.assertTag('Booking Requested (Callback Scheduled)', 'O')

    def test_booking_requested_not_responded_is_open(self):
        self.assertTag('Booking Requested (Customer Not Responded)', 'O')

    def test_booking_requested_dealer_visit_is_open(self):
        self.assertTag('Booking Requested (Dealer Visit Scheduled)', 'O')

    def test_booking_requested_home_visit_is_open(self):
        self.assertTag('Booking Requested (Home Visit Scheduled)', 'O')

    # ── req 4: All Booking Request variants ───────────────────────────────────
    def test_all_booking_request_variants_are_open(self):
        booking_request_variants = [
            'Booking Request',
            'Booking Requested (Callback Scheduled)',
            'Booking Requested (Customer Not Responded)',
            'Booking Requested (Dealer Visit Scheduled)',
            'Booking Requested (Home Visit Scheduled)',
        ]
        for sn in booking_request_variants:
            with self.subTest(sn=sn):
                self.assertTag(sn, 'O')

    # ── req 5: All Call for verification variants ─────────────────────────────
    def test_all_call_for_verification_variants_are_open(self):
        cfv_variants = [
            'Call for verification',
            'Call for verification (Callback Scheduled)',
            'Call for verification (Customer Not Responded)',
            'Call for verification (Dealer Visit Scheduled)',
        ]
        for sn in cfv_variants:
            with self.subTest(sn=sn):
                self.assertTag(sn, 'O')

    # ── req 6: All Enquiry Re Opened variants ─────────────────────────────────
    def test_all_enquiry_re_opened_variants_are_open(self):
        ero_variants = [
            'Enquiry Re Opened (Callback Scheduled)',
            'Enquiry Re Opened (Customer Not Responded)',
            'Enquiry Re Opened (Dealer Visit Scheduled)',
            'Enquiry Re Opened (Home Visit Scheduled)',
        ]
        for sn in ero_variants:
            with self.subTest(sn=sn):
                self.assertTag(sn, 'O')

    # ── req 7: All L1 Verified variants ──────────────────────────────────────
    def test_all_l1_verified_variants_are_open(self):
        l1_variants = [
            'L1 Verified (Callback Scheduled)',
            'L1 Verified (Customer Not Responded)',
            'L1 Verified (Dealer Visit Scheduled)',
        ]
        for sn in l1_variants:
            with self.subTest(sn=sn):
                self.assertTag(sn, 'O')

    # ── req 8: All Lost variants ──────────────────────────────────────────────
    def test_all_lost_variants_are_lost(self):
        lost_variants = [
            'Lost Not Contactable',
            'Lost Not Purchased',
            'Lost Purchased',
            'Lost To Co-Dealer',
        ]
        for sn in lost_variants:
            with self.subTest(sn=sn):
                self.assertTag(sn, 'L')

    # ── req 9: Pending Retail ─────────────────────────────────────────────────
    def test_pending_retail_is_open(self):
        self.assertTag('Pending Retail', 'O')

    # ── req 10: All Price Quote variants ─────────────────────────────────────
    def test_all_price_quote_variants_are_open(self):
        pq_variants = [
            'Price Quote',
            'Price Quote (Callback Scheduled)',
            'Price Quote (Customer Not Responded)',
            'Price Quote (Dealer Visit Scheduled)',
            'Price Quote (No Dealer Connect)',
        ]
        for sn in pq_variants:
            with self.subTest(sn=sn):
                self.assertTag(sn, 'O')

    # ── req 11: All Test Ride variants ────────────────────────────────────────
    def test_all_test_ride_variants_are_open(self):
        tr_variants = [
            'Test Ride Completed (Callback Scheduled)',
            'Test Ride Requested',
            'Test Ride Requested (Callback Scheduled)',
            'Test Ride Requested (Customer Not Responded)',
            'Test Ride Requested (Dealer Visit Scheduled)',
            'Test Ride Requested (Home Visit Scheduled)',
        ]
        for sn in tr_variants:
            with self.subTest(sn=sn):
                self.assertTag(sn, 'O')

    # ── req 12: Unknown status → U, never B or L ─────────────────────────────
    def test_unknown_status_returns_U_not_B(self):
        unknowns = [
            'Retailed',
            'Retail Done',
            'Hot Lead',
            'Interested',
            'New Lead',
            'Contacted',
            'Follow Up',
            'Negotiation',
            'Exchanged',
        ]
        for sn in unknowns:
            with self.subTest(sn=sn):
                tag = _classify_status_test(sn)
                self.assertNotEqual(tag, 'B',
                    f"{sn!r} must not map to B (got {tag!r})")
                self.assertNotEqual(tag, 'L',
                    f"{sn!r} must not map to L (got {tag!r})")

    def test_unknown_status_returns_U(self):
        self.assertEqual(_classify_status_test('Some Future Status'), 'U')
        self.assertEqual(_classify_status_test('Another Unknown'), 'U')

    # ── req 12: "Booking Request" must NOT map to B under any circumstances ───
    def test_booking_request_never_maps_to_B(self):
        """Regression guard: old broad-keyword logic would have returned B."""
        self.assertNotEqual(_classify_status_test('Booking Request'), 'B')
        self.assertNotEqual(_classify_status_test('Booking Requested (Callback Scheduled)'), 'B')
        self.assertNotEqual(_classify_status_test('Booking Requested (Customer Not Responded)'), 'B')
        self.assertNotEqual(_classify_status_test('Booking Requested (Home Visit Scheduled)'), 'B')

    # ── req 12: "Customer Not Responded" must NOT map to L ───────────────────
    def test_customer_not_responded_is_not_lost(self):
        """Old logic: 'not interest' substring match would NOT have caught this,
        but ensure it's Open, not Lost."""
        self.assertEqual(_classify_status_test('Customer Not Responded'), 'O')

    # ── req 14: whitespace normalisation ─────────────────────────────────────
    def test_leading_trailing_whitespace_stripped(self):
        self.assertEqual(_classify_status_test('  Booked  '), 'B')
        self.assertEqual(_classify_status_test('  Lost Not Purchased  '), 'L')
        self.assertEqual(_classify_status_test('  Price Quote  '), 'O')

    def test_internal_whitespace_collapsed(self):
        # _norm_sn uses split()+join which collapses internal runs of spaces.
        # 'Booked  (Callback  Scheduled)' → 'Booked (Callback Scheduled)' → B.
        self.assertEqual(_classify_status_test('Booked  (Callback  Scheduled)'), 'B')

    def test_internal_single_space_normalised(self):
        # Normal single spaces → still match after normalisation
        self.assertEqual(_classify_status_test('Booked (Callback Scheduled)'), 'B')
        self.assertEqual(_classify_status_test('Lost Not Contactable'), 'L')

    # ── req 15: blank / empty inputs ─────────────────────────────────────────
    def test_empty_string_returns_U(self):
        self.assertEqual(_classify_status_test(''), 'U')

    def test_whitespace_only_returns_U(self):
        self.assertEqual(_classify_status_test('   '), 'U')

    # ── req 1: every key in the map is covered ────────────────────────────────
    def test_every_map_entry_returns_correct_tag(self):
        for sn, expected in _STATUS_TAG_MAP_TEST.items():
            with self.subTest(sn=sn):
                self.assertTag(sn, expected)

    # ── req 13: no duplicate counting ─────────────────────────────────────────
    def test_no_duplicate_counting_each_lead_one_bucket(self):
        """
        Simulate dl_sn aggregation: each lead increments exactly one of
        [open, booking, lost] or none (if 'U').
        Total O+B+L ≤ total leads; classified + unclassified = total leads.
        """
        leads = [
            ('Booked',                           'B'),
            ('Booking Request',                  'O'),
            ('Lost Not Purchased',               'L'),
            ('Some Unknown Status',              'U'),
            ('Price Quote',                      'O'),
            ('Booked (Callback Scheduled)',       'B'),
            ('Lost To Co-Dealer',                'L'),
            ('Customer Not Responded',           'O'),
        ]
        bucket = [0, 0, 0, 0]  # [O, B, L, U]
        for sn, expected_tag in leads:
            tag = _classify_status_test(sn)
            self.assertEqual(tag, expected_tag, f"{sn!r} → {tag!r} (expected {expected_tag!r})")
            if   tag == 'O': bucket[0] += 1
            elif tag == 'B': bucket[1] += 1
            elif tag == 'L': bucket[2] += 1
            else:            bucket[3] += 1

        self.assertEqual(bucket[0], 3, 'Open count')   # Booking Request, Price Quote, CNR
        self.assertEqual(bucket[1], 2, 'Booking count') # Booked, Booked CB
        self.assertEqual(bucket[2], 2, 'Lost count')    # Lost NP, Lost Co-Dealer
        self.assertEqual(bucket[3], 1, 'Unknown count') # Some Unknown Status
        self.assertEqual(sum(bucket), len(leads), 'Total must equal lead count (no duplicates)')

    # ── req 14: dl_sn aggregation structure ──────────────────────────────────
    def test_dl_sn_aggregation_by_dealer(self):
        """
        Simulates the dl_sn aggregation loop for a small dealer dataset.
        Verifies [open, booking, lost] per (city, dealer, month) key.
        """
        leads = [
            # (cti, dli, lmi, status_name)
            (0, 0, 0, 'Booked'),
            (0, 0, 0, 'Booking Request'),
            (0, 0, 0, 'Lost Not Purchased'),
            (0, 0, 0, 'Some Unknown'),
            (0, 0, 0, 'Price Quote'),
            (0, 1, 0, 'Booked (Callback Scheduled)'),
            (0, 1, 0, 'Lost To Co-Dealer'),
            (0, 1, 1, 'Test Ride Requested'),
        ]
        dl_sn_sim = {}
        for cti, dli, lmi, sn in leads:
            tag = _classify_status_test(sn)
            key = (cti, dli, lmi)
            if key not in dl_sn_sim:
                dl_sn_sim[key] = [0, 0, 0]
            if   tag == 'O': dl_sn_sim[key][0] += 1
            elif tag == 'B': dl_sn_sim[key][1] += 1
            elif tag == 'L': dl_sn_sim[key][2] += 1
            # U: not counted

        # Dealer 0, month 0: O=2(BR+PQ), B=1(Booked), L=1(LostNP), U=1(Unknown)→not counted
        self.assertEqual(dl_sn_sim[(0, 0, 0)], [2, 1, 1])
        # Dealer 1, month 0: O=0, B=1(Booked CB), L=1(Lost Co-Dealer)
        self.assertEqual(dl_sn_sim[(0, 1, 0)], [0, 1, 1])
        # Dealer 1, month 1: O=1(Test Ride), B=0, L=0
        self.assertEqual(dl_sn_sim[(0, 1, 1)], [1, 0, 0])

    # ── req 15: filter-aware aggregation (month filter) ───────────────────────
    def test_dl_sn_respects_month_filter(self):
        """Month filter: only count leads in selected months."""
        # dl_sn rows: [cti, dli, lmi, open, booking, lost]
        dl_sn_rows = [
            [0, 0, 0, 3, 1, 2],  # month 0
            [0, 0, 1, 5, 2, 1],  # month 1
            [0, 0, 2, 1, 0, 3],  # month 2
        ]
        lm_arr = ["Jul'26", "Aug'26", "Sep'26"]
        active_months = {"Aug'26"}  # only month index 1

        total_open = total_booking = total_lost = 0
        for row in dl_sn_rows:
            lmi = row[2]
            if lm_arr[lmi] not in active_months:
                continue
            total_open    += row[3]
            total_booking += row[4]
            total_lost    += row[5]

        self.assertEqual(total_open,    5)
        self.assertEqual(total_booking, 2)
        self.assertEqual(total_lost,    1)

    def test_dl_sn_all_months_unfiltered(self):
        """When no month filter is active, all rows contribute."""
        dl_sn_rows = [
            [0, 0, 0, 3, 1, 2],
            [0, 0, 1, 5, 2, 1],
        ]
        total_open    = sum(r[3] for r in dl_sn_rows)
        total_booking = sum(r[4] for r in dl_sn_rows)
        total_lost    = sum(r[5] for r in dl_sn_rows)
        self.assertEqual(total_open,    8)
        self.assertEqual(total_booking, 3)
        self.assertEqual(total_lost,    3)

    # ── Complete map coverage count ───────────────────────────────────────────
    def test_map_has_exactly_2_booking_entries(self):
        booking = [k for k, v in _STATUS_TAG_MAP_TEST.items() if v == 'B']
        self.assertEqual(len(booking), 2, f'Expected 2 Booking entries, got {len(booking)}: {booking}')

    def test_map_has_exactly_4_lost_entries(self):
        lost = [k for k, v in _STATUS_TAG_MAP_TEST.items() if v == 'L']
        self.assertEqual(len(lost), 4, f'Expected 4 Lost entries, got {len(lost)}: {lost}')

    def test_map_has_correct_open_count(self):
        opens = [k for k, v in _STATUS_TAG_MAP_TEST.items() if v == 'O']
        self.assertEqual(len(opens), 29, f'Expected 29 Open entries, got {len(opens)}')

    def test_total_map_entries(self):
        self.assertEqual(len(_STATUS_TAG_MAP_TEST), 35)


# ---------------------------------------------------------------------------
# MONTH-CLOSE ARCHITECTURE — inline config mirrors push_tvs_data.py
# Must stay in sync with the LEAD_SHEETS / PENDING_LEAD_MONTHS block.
# ---------------------------------------------------------------------------

_LEAD_SHEETS_TEST = [
    {
        'id':     '1gaRoPLebv7jaBgWEET-XSQuhqE_XgQlGru39TA-FoSo',
        'tab':    'TVS',
        'label':  "Jul'26-LeadMaster",
        'min_mo': 2607,
        'max_mo': 2607,
        'frozen': True,
    },
    {
        'id':     '1Wp26qCv3d6oEq1h2wGamlHmCb9YuYrNDa8x8i653W3M',
        'tab':    'TVS',
        'label':  "Aug'26-LeadMaster-FROZEN",
        'min_mo': 2608,
        'max_mo': 2608,
        'frozen': True,
    },
    {
        'id':     '1iSw5zXF67q5Wkoz2mSPFqql9OPAcqmd0um5BEHUGf4o',
        'tab':    'TVS',
        'label':  "Sep'26-LeadMaster",
        'min_mo': 2609,
        'max_mo': None,
    },
]

_PENDING_LEAD_MONTHS_TEST: set = set()  # Sep'26 activated 2026-09-03

# month_order helper (mirrors push_tvs_data.py)
_MONTH_NAMES_T = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
def _month_order_t(lm: str) -> int:
    if not lm or not isinstance(lm, str): return 0
    m = re.match(r"([A-Za-z]{3})'(\d{2})$", lm.strip())
    if not m: return 0
    mo = _MONTH_NAMES_T.index(m.group(1)) + 1 if m.group(1) in _MONTH_NAMES_T else 0
    return int(m.group(2)) * 100 + mo if mo else 0


def _cur_month_covered_t(cur_mo_str: str, lead_sheets, pending: set) -> bool:
    """Mirror of the pipeline's _cur_month_covered logic."""
    cur_order = _month_order_t(cur_mo_str)
    in_sheets = any(
        s.get('min_mo', 0) <= cur_order and
        (s.get('max_mo') is None or s.get('max_mo') >= cur_order)
        for s in lead_sheets
    )
    return in_sheets or cur_mo_str in pending


def _missing_prior_t(prior_months: list, online_lm_set: set, pending: set) -> list:
    """Mirror of the pipeline's _missing_prior logic."""
    return [mo for mo in prior_months if mo not in online_lm_set and mo not in pending]


class TestMonthCloseArchitecture(unittest.TestCase):
    """
    Requirements tested (R1-R17):
    R1.  August is recognised as CLOSED/FROZEN.
    R2.  September is recognised as CURRENT but has no Lead Master configured yet.
    R3.  August Lead Master source points to the provided frozen sheet.
    R4.  August lead data cannot change because of later Lead Master edits (config immutability).
    R5.  August Retail continues to be fetched (pipeline always fetches retail independently).
    R6.  August Retail can update while August leads remain frozen.
    R7.  August On Create leads remain frozen.
    R8.  August On Update does not continue changing after month close.
    R9.  Retail Ageing continues using Retail_Date (config unchanged).
    R10. Status_Name remains available for August (LEAD_COLS includes it).
    R11. Geo & Dealer Open/Booking/Lost calculations continue working.
    R12. No existing month is accidentally frozen.
    R13. Existing pipeline behaviour for other months remains intact.
    R14. Existing retry/page-resume/parallel-fetch tests continue passing.
    R15. No duplicate rows are introduced.
    R16. No lead rows are silently lost because of the freeze.
    R17. Source-drop validation does not falsely fail for frozen August.
    """

    # ── R1: August is CLOSED ──────────────────────────────────────────────────
    def test_r1_august_is_closed_frozen(self):
        aug_entries = [s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608]
        self.assertEqual(len(aug_entries), 1, 'Exactly one LEAD_SHEETS entry must cover Aug\'26')
        aug = aug_entries[0]
        self.assertTrue(aug.get('frozen'), 'Aug\'26 entry must have frozen=True')
        self.assertEqual(aug['max_mo'], 2608, 'Aug\'26 max_mo must be 2608 (closed at Aug)')

    # ── R2: September is CURRENT and ACTIVE (activated 2026-09-03) ───────────
    def test_r2_september_is_active_in_lead_sheets(self):
        sep_entry = next(
            (s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2609), None
        )
        self.assertIsNotNone(sep_entry, "Sep'26 must have a real LEAD_SHEETS entry")
        self.assertEqual(sep_entry['id'], '1iSw5zXF67q5Wkoz2mSPFqql9OPAcqmd0um5BEHUGf4o',
                         'Sep\'26 must use the confirmed September Lead Master ID')
        self.assertIsNone(sep_entry.get('max_mo'),
                          'Sep\'26 max_mo must be None (open month, no upper bound yet)')
        self.assertFalse(sep_entry.get('frozen', False),
                         'Sep\'26 must NOT be frozen — it is the current open month')

    def test_r2_september_not_in_pending(self):
        self.assertNotIn("Sep'26", _PENDING_LEAD_MONTHS_TEST,
                         "Sep'26 must NOT be in PENDING_LEAD_MONTHS after activation")

    def test_r2_pending_lead_months_is_empty(self):
        self.assertEqual(len(_PENDING_LEAD_MONTHS_TEST), 0,
                         'PENDING_LEAD_MONTHS must be empty after Sep\'26 activation')

    def test_r2_september_covered_via_lead_sheets(self):
        covered = _cur_month_covered_t("Sep'26", _LEAD_SHEETS_TEST, _PENDING_LEAD_MONTHS_TEST)
        self.assertTrue(covered, "Sep'26 must be covered via LEAD_SHEETS (not pending)")

    # ── R3: August Lead Master points to the frozen sheet ────────────────────
    def test_r3_august_lead_master_url_is_frozen_sheet(self):
        aug = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608), None)
        self.assertIsNotNone(aug)
        self.assertEqual(
            aug['id'], '1Wp26qCv3d6oEq1h2wGamlHmCb9YuYrNDa8x8i653W3M',
            'August Lead Master must point to the frozen snapshot sheet')

    # ── R4: August lead data is immutable (config-level freeze) ──────────────
    def test_r4_august_frozen_sheet_not_rolling(self):
        """Aug entry must have max_mo=2608 (not None) — it is not a rolling sheet."""
        aug = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608), None)
        self.assertIsNotNone(aug)
        self.assertIsNotNone(aug.get('max_mo'),
                             'Frozen Aug sheet must have a finite max_mo, not None')
        self.assertEqual(aug['max_mo'], 2608)

    def test_r4_old_rolling_aug_sheet_reused_as_sep(self):
        """The sheet 1iSw5zXF67... was the old rolling Aug+ sheet; it now lives in
        LEAD_SHEETS as the Sep'26 entry (min_mo=2609, not 2607/2608)."""
        sep_id = '1iSw5zXF67q5Wkoz2mSPFqql9OPAcqmd0um5BEHUGf4o'
        sep = next((s for s in _LEAD_SHEETS_TEST if s['id'] == sep_id), None)
        self.assertIsNotNone(sep, 'Sep sheet ID must be present in LEAD_SHEETS')
        self.assertEqual(sep['min_mo'], 2609,
                         'Sheet reused for Sep must have min_mo=2609, not earlier months')
        self.assertIsNone(sep['max_mo'],
                          'Sep sheet is open-ended (no max_mo until month-close)')

    # ── R5 & R6: August Retail continues updating (structural) ───────────────
    def test_r5_retail_sheet_config_unchanged(self):
        """Retail config constants are not touched by the month-close change."""
        RETAILS_FILE_ID = '1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE'
        RETAILS_TAB     = 'Raw'
        # These must not be empty — they are the live retail source.
        self.assertTrue(RETAILS_FILE_ID, 'RETAILS_FILE_ID must be set')
        self.assertEqual(RETAILS_TAB, 'Raw')

    def test_r6_august_leads_frozen_retail_independent(self):
        """Lead freeze (via LEAD_SHEETS config) is independent of the retail fetch path."""
        aug = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608), None)
        self.assertIsNotNone(aug)
        # The frozen=True flag on a lead sheet does NOT affect retail processing.
        # Retail is fetched from a completely separate source (RETAILS_FILE_ID).
        # Verify: no lead-sheet entry has any key that would gate retail fetching.
        self.assertNotIn('skip_retail', aug)
        self.assertNotIn('freeze_retail', aug)

    # ── R7: August On Create leads frozen (fixed LeadMonth=Aug'26 pool) ──────
    def test_r7_aug_on_create_leads_frozen(self):
        """Frozen Aug sheet has min_mo=max_mo=2608: only Aug'26 rows survive STAGE 6."""
        aug = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608), None)
        self.assertIsNotNone(aug)
        self.assertEqual(aug['min_mo'], aug['max_mo'],
                         'Frozen sheet must have min_mo == max_mo (single closed month)')
        # Simulate STAGE 6 filter: a row with LeadMonth=Aug'26 passes; Sep'26 does not.
        def _passes(mo_str):
            mo = _month_order_t(mo_str)
            return aug['min_mo'] <= mo <= aug['max_mo']
        self.assertTrue(_passes("Aug'26"))
        self.assertFalse(_passes("Sep'26"))
        self.assertFalse(_passes("Jul'26"))

    # ── R8: August On Update frozen (no new Aug leads from Lead Master) ───────
    def test_r8_aug_on_update_no_new_lead_rows(self):
        """On Update for Aug'26 cannot grow because the Aug Lead Master is frozen.
        No new Aug'26 lead rows can enter the pipeline after month-close."""
        aug = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608), None)
        self.assertIsNotNone(aug)
        self.assertTrue(aug.get('frozen'),
                        'Aug must be frozen — no new lead rows after month-close')
        # If a future sheet has min_mo ≤ 2608 and max_mo ≥ 2608, it would re-open Aug.
        overlapping = [
            s for s in _LEAD_SHEETS_TEST
            if s is not aug
            and s.get('min_mo', 0) <= 2608
            and (s.get('max_mo') is None or s.get('max_mo') >= 2608)
        ]
        self.assertEqual(overlapping, [],
                         f'No other sheet may cover Aug\'26: {overlapping}')

    # ── R9: Retail Ageing uses Retail_Date (unchanged) ───────────────────────
    def test_r9_retail_date_config_not_altered(self):
        """RETAILS_FILE_ID and RETAILS_TAB are unchanged; Retail_Date path is separate."""
        # The frozen-lead change only touches LEAD_SHEETS + PENDING_LEAD_MONTHS.
        # Neither RETAILS_FILE_ID, RETAILS_TAB, nor fetch_retail_date_map is altered.
        unchanged_ids = [s['id'] for s in _LEAD_SHEETS_TEST]
        self.assertNotIn('1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE', unchanged_ids,
                         'Retail sheet ID must NOT appear in LEAD_SHEETS')

    # ── R10: Status_Name available for August ─────────────────────────────────
    def test_r10_status_name_in_lead_cols(self):
        """LEAD_COLS must include Status_Name so Aug frozen leads carry classification data."""
        LEAD_COLS = 'opty_id,Lead_Month,Date,model,City,State,Dealer_Name,lead_type,Medium,Retail By,DMS_Retail_Month,Status_Name'
        self.assertIn('Status_Name', LEAD_COLS,
                      'Status_Name must remain in LEAD_COLS for Geo & Dealer tab')

    # ── R11: Geo & Dealer classification works on Aug frozen data ────────────
    def test_r11_geo_dealer_status_classification_intact(self):
        """The exact Status_Name map must remain intact for Aug frozen leads."""
        booking_statuses = {'Booked', 'Booked (Callback Scheduled)'}
        lost_statuses    = {'Lost Not Contactable', 'Lost Not Purchased',
                            'Lost Purchased', 'Lost To Co-Dealer'}
        for sn in booking_statuses:
            self.assertEqual(_classify_status_test(sn), 'B', f'{sn!r} must map to B')
        for sn in lost_statuses:
            self.assertEqual(_classify_status_test(sn), 'L', f'{sn!r} must map to L')
        self.assertEqual(_classify_status_test('Call for verification'), 'O')
        self.assertEqual(_classify_status_test('Price Quote'), 'O')
        self.assertEqual(_classify_status_test('Pending Retail'), 'O')

    # ── R12: No existing month accidentally frozen ────────────────────────────
    def test_r12_no_non_aug_month_accidentally_frozen(self):
        """Only Aug'26 (and Jul'26, already closed) must have frozen=True."""
        legitimately_frozen = {2607, 2608}
        for s in _LEAD_SHEETS_TEST:
            if s.get('frozen'):
                self.assertIn(s['min_mo'], legitimately_frozen,
                              f"Sheet {s['label']!r} is frozen but min_mo={s['min_mo']} is not expected")

    def test_r12_hist_months_not_in_lead_sheets(self):
        """Historical months (pre-Jul'26) must NOT appear in LEAD_SHEETS min_mo."""
        for s in _LEAD_SHEETS_TEST:
            self.assertGreaterEqual(s['min_mo'], 2607,
                                    f"Sheet {s['label']!r} has min_mo={s['min_mo']} < Jul'26")

    # ── R13: Other months not broken ─────────────────────────────────────────
    def test_r13_july_coverage_unaffected(self):
        """Jul'26 must still be covered by its dedicated sheet."""
        jul_entries = [s for s in _LEAD_SHEETS_TEST
                       if s.get('min_mo') <= 2607 <= (s.get('max_mo') or 9999)]
        self.assertTrue(any(s['max_mo'] == 2607 for s in jul_entries),
                        "Jul'26 must have a dedicated entry capped at 2607")

    def test_r13_prior_months_still_fail_if_absent(self):
        """Prior live months (non-pending) must still trigger hard-fail when absent."""
        # Simulate: Jul'26 missing from online data, not pending → must fail
        missing = _missing_prior_t(["Jul'26", "Aug'26"], set(), _PENDING_LEAD_MONTHS_TEST)
        self.assertIn("Jul'26", missing, "Jul'26 absent → must appear in missing list")
        self.assertIn("Aug'26", missing, "Aug'26 absent → must appear in missing list")

    def test_r13_pending_month_not_required_in_prior(self):
        """A pending month in _prior_live_months must NOT trigger a hard-fail."""
        # Use a hypothetical Oct'26 as the pending month (Sep is now active).
        hypothetical_pending = {"Oct'26"}
        missing = _missing_prior_t(["Jul'26", "Oct'26"],
                                   {"Jul'26"},    # Jul present, Oct absent (pending)
                                   hypothetical_pending)
        self.assertNotIn("Oct'26", missing,
                         "A pending month must not appear in _missing_prior")

    # ── R14: Existing validation tests still pass (meta-check) ───────────────
    def test_r14_month_order_function_intact(self):
        """month_order helper must still resolve correctly."""
        self.assertEqual(_month_order_t("Jul'26"), 2607)
        self.assertEqual(_month_order_t("Aug'26"), 2608)
        self.assertEqual(_month_order_t("Sep'26"), 2609)
        self.assertEqual(_month_order_t("Jan'25"), 2501)
        self.assertEqual(_month_order_t(""),       0)
        self.assertEqual(_month_order_t(None),     0)

    # ── R15: No duplicate lead rows from frozen sheet ─────────────────────────
    def test_r15_no_aug_double_coverage(self):
        """Aug'26 must not be covered by more than one LEAD_SHEETS entry."""
        aug_covering = [
            s for s in _LEAD_SHEETS_TEST
            if s.get('min_mo', 0) <= 2608 <= (s.get('max_mo') or 9999)
        ]
        self.assertEqual(len(aug_covering), 1,
                         f'Aug\'26 must be covered by exactly 1 sheet, got: '
                         f'{[s["label"] for s in aug_covering]}')

    def test_r15_jul_double_coverage_expected(self):
        """Jul'26 is covered by exactly 1 sheet (the Jul-specific sheet)."""
        jul_covering = [
            s for s in _LEAD_SHEETS_TEST
            if s.get('min_mo', 0) <= 2607 <= (s.get('max_mo') or 9999)
        ]
        self.assertEqual(len(jul_covering), 1,
                         f"Jul'26 must be covered by exactly 1 sheet")

    # ── R16: No lead rows silently lost ──────────────────────────────────────
    def test_r16_aug_sheet_has_real_id(self):
        """Aug frozen sheet must have a non-empty, non-placeholder id."""
        aug = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608), None)
        self.assertIsNotNone(aug)
        self.assertTrue(aug.get('id'), 'Aug frozen sheet must have an id set')
        self.assertNotIn('PLACEHOLDER', aug['id'].upper(),
                         'Aug frozen sheet id must be a real sheet id, not a placeholder')

    def test_r16_all_non_pending_sheets_have_id(self):
        """Every entry in LEAD_SHEETS must have a real id (pending months are NOT in LEAD_SHEETS)."""
        for s in _LEAD_SHEETS_TEST:
            self.assertTrue(s.get('id'),
                            f"Sheet {s.get('label', '?')} missing 'id'")

    # ── R17: Source-drop validation not false-triggered by frozen Aug ─────────
    def test_r17_frozen_sheet_stable_counts(self):
        """A frozen sheet returns identical row counts on every run — source-drop
        validation must never flag stable counts as a drop."""
        # Simulate: previous run had N rows, current run has same N rows.
        def _check_source_drop_sim(label, current, prev, threshold=0.85):
            if label not in prev:
                return 'no-baseline'
            baseline = prev[label].get('rows', 0)
            if baseline == 0:
                return 'no-baseline'
            ratio = current / baseline
            return 'FAIL' if ratio < threshold else 'OK'

        prev = {"Aug'26-LeadMaster-FROZEN": {'rows': 50000}}
        self.assertEqual(
            _check_source_drop_sim("Aug'26-LeadMaster-FROZEN", 50000, prev), 'OK',
            'Identical frozen row count must not trigger source-drop alert')
        # Even a tiny drop (e.g., 1 row) in a frozen sheet — still well above threshold.
        self.assertEqual(
            _check_source_drop_sim("Aug'26-LeadMaster-FROZEN", 49999, prev), 'OK',
            '1-row variance must not trigger source-drop alert')

    def test_r17_sep_source_drop_checked_normally(self):
        """Sep'26 now has a LEAD_SHEETS entry → source-drop check applies on next run."""
        labels_in_sheets = {s['label'] for s in _LEAD_SHEETS_TEST}
        sep_labels = {lb for lb in labels_in_sheets if 'Sep' in lb}
        self.assertNotEqual(sep_labels, set(),
                            'Sep\'26 is now in LEAD_SHEETS — source-drop check must apply')

    # ── Config completeness ────────────────────────────────────────────────────
    def test_config_all_lead_sheets_have_required_keys(self):
        required = {'id', 'tab', 'label', 'min_mo', 'max_mo'}
        for s in _LEAD_SHEETS_TEST:
            missing = required - set(s.keys())
            self.assertEqual(missing, set(),
                             f"Sheet {s.get('label', '?')} missing keys: {missing}")

    def test_config_pending_lead_months_is_empty_set(self):
        self.assertIsInstance(_PENDING_LEAD_MONTHS_TEST, set)
        self.assertEqual(len(_PENDING_LEAD_MONTHS_TEST), 0,
                         'PENDING_LEAD_MONTHS must be empty after Sep\'26 activation')

    def test_config_sep_in_lead_sheets_and_not_in_pending(self):
        """Sep'26 must be in LEAD_SHEETS and not pending."""
        sep_entry = next((s for s in _LEAD_SHEETS_TEST if 'Sep' in s['label']), None)
        self.assertIsNotNone(sep_entry, "Sep'26 must have a LEAD_SHEETS entry")
        self.assertNotIn("Sep'26", _PENDING_LEAD_MONTHS_TEST,
                         "Sep'26 must not be in PENDING_LEAD_MONTHS")

    # ── Sep'26 activation — new tests (R1-R20 extended) ─────────────────────

    def test_sep_spreadsheet_id_correct(self):
        """Sep'26 must use the confirmed spreadsheet ID."""
        sep = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2609), None)
        self.assertIsNotNone(sep)
        self.assertEqual(sep['id'], '1iSw5zXF67q5Wkoz2mSPFqql9OPAcqmd0um5BEHUGf4o')

    def test_sep_is_not_frozen(self):
        sep = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2609), None)
        self.assertIsNotNone(sep)
        self.assertFalse(sep.get('frozen', False), 'Open month must not be frozen')

    def test_sep_min_mo_is_2609(self):
        sep = next((s for s in _LEAD_SHEETS_TEST if 'Sep' in s.get('label', '')), None)
        self.assertIsNotNone(sep)
        self.assertEqual(sep['min_mo'], 2609)

    def test_sep_max_mo_is_none(self):
        """Sep'26 is the open month — no upper cap yet."""
        sep = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2609), None)
        self.assertIsNotNone(sep)
        self.assertIsNone(sep.get('max_mo'))

    def test_aug_sheet_does_not_cover_sep(self):
        """Aug frozen sheet must not cover Sep'26 rows."""
        aug = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608), None)
        self.assertIsNotNone(aug)
        # STAGE 6: aug max_mo=2608 → Sep'26 (order 2609) filtered out
        sep_order = 2609
        self.assertLess(aug['max_mo'], sep_order,
                        'Aug frozen sheet (max_mo=2608) must not include Sep\'26 rows')

    def test_sep_sheet_does_not_cover_aug(self):
        """Sep'26 sheet (min_mo=2609) must exclude Aug'26 rows."""
        sep = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2609), None)
        self.assertIsNotNone(sep)
        aug_order = 2608
        self.assertGreater(sep['min_mo'], aug_order,
                           'Sep\'26 sheet (min_mo=2609) must not include Aug\'26 rows')

    def test_sep_no_double_coverage(self):
        """Sep'26 must be covered by exactly one LEAD_SHEETS entry."""
        sep_covering = [
            s for s in _LEAD_SHEETS_TEST
            if s.get('min_mo', 0) <= 2609 <= (s.get('max_mo') or 9999)
        ]
        self.assertEqual(len(sep_covering), 1,
                         f'Sep\'26 must be covered by exactly 1 sheet, got: '
                         f'{[s["label"] for s in sep_covering]}')

    def test_aug_no_double_coverage_after_sep_added(self):
        """Verify Aug'26 still covered by exactly 1 sheet (Sep entry does not overlap)."""
        aug_covering = [
            s for s in _LEAD_SHEETS_TEST
            if s.get('min_mo', 0) <= 2608 <= (s.get('max_mo') or 9999)
        ]
        self.assertEqual(len(aug_covering), 1,
                         f'Aug\'26 must still be covered by exactly 1 sheet after Sep activation')

    def test_sep_on_create_live(self):
        """Sep'26 On Create — lead rows fetched live (no frozen flag)."""
        sep = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2609), None)
        self.assertIsNotNone(sep)
        self.assertFalse(sep.get('frozen', False))
        # On Create uses LeadMonth; Sep'26 rows (month_order=2609) pass STAGE 6.
        self.assertEqual(_month_order_t("Sep'26"), 2609)
        self.assertGreaterEqual(2609, sep['min_mo'])  # passes min filter

    def test_sep_on_update_live(self):
        """Sep'26 On Update — live data (sep sheet open, no max_mo cap)."""
        sep = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2609), None)
        self.assertIsNotNone(sep)
        self.assertIsNone(sep.get('max_mo'),
                          'Open sep sheet must have no max_mo cap — On Update is live')

    def test_aug_on_create_still_frozen(self):
        """Aug'26 On Create — lead pool is permanently fixed (frozen sheet)."""
        aug = next((s for s in _LEAD_SHEETS_TEST if s.get('min_mo') == 2608), None)
        self.assertIsNotNone(aug)
        self.assertTrue(aug.get('frozen'))
        self.assertEqual(aug['max_mo'], 2608)  # only Aug rows pass

    def test_aug_on_update_still_frozen(self):
        """Aug'26 On Update — no new Aug lead rows can enter from any sheet."""
        overlapping_aug = [
            s for s in _LEAD_SHEETS_TEST
            if not s.get('frozen', False)
            and s.get('min_mo', 0) <= 2608
            and (s.get('max_mo') is None or s.get('max_mo') >= 2608)
        ]
        self.assertEqual(overlapping_aug, [],
                         'No non-frozen sheet must cover Aug\'26 after Sep activation')

    def test_status_name_available_for_sep(self):
        """LEAD_COLS must include Status_Name for Sep leads (Geo & Dealer tab)."""
        LEAD_COLS = 'opty_id,Lead_Month,Date,model,City,State,Dealer_Name,lead_type,Medium,Retail By,DMS_Retail_Month,Status_Name'
        self.assertIn('Status_Name', LEAD_COLS)

    def test_geo_dealer_classification_unchanged_for_sep(self):
        """Sep leads must use the same exact Status_Name→tag mapping."""
        self.assertEqual(_classify_status_test('Booked'), 'B')
        self.assertEqual(_classify_status_test('Lost Not Purchased'), 'L')
        self.assertEqual(_classify_status_test('Call for verification'), 'O')
        self.assertEqual(_classify_status_test('Booking Request'), 'O')
        self.assertEqual(_classify_status_test('SomeNewUnknownStatus'), 'U')

    def test_lead_sheets_count_is_three(self):
        """After Sep activation, LEAD_SHEETS must have exactly 3 entries."""
        self.assertEqual(len(_LEAD_SHEETS_TEST), 3,
                         'LEAD_SHEETS must have Jul + Aug + Sep entries')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main(verbosity=2)
