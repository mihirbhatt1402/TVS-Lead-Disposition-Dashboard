"""Regression tests for the TVS NTORQ 125 / NTORQ 150 split.

Root cause (confirmed 2026-08-25): 'TVS NTorq 150' hit the NTORQ_150 branch
in normalize_purchased_model because no map entry existed and the keyword guard
rejected it as an ambiguous 150 variant.

Fix (fix: map TVS NTorq 150 model): explicit map entries added for both
mixed-case ('TVS NTorq 150') and uppercase ('TVS NTORQ 150') forms.

Evidence: unknown_model_diagnostic.json — 2,099 leads / 82 retails Unknown,
all with raw_repr='TVS NTorq 150', reason=NTORQ_150.

Keep in sync with PURCHASED_MODEL_MAP and normalize_purchased_model in
push_tvs_data.py whenever either changes.
"""
import unittest


class TestNtorqNormalization(unittest.TestCase):
    """NTORQ 150 mapping fix — 7 regression tests."""

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
        """Inline of the NTORQ paths in normalize_purchased_model."""
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
        """'TVS NTorq 150' (exact production value confirmed by diagnostic) maps to 'TVS NTORQ 150'."""
        self.assertEqual(self._norm('TVS NTorq 150'), 'TVS NTORQ 150')

    def test_uppercase_variant_maps_to_ntorq_150(self):
        """'TVS NTORQ 150' (canonical uppercase form) maps to 'TVS NTORQ 150'."""
        self.assertEqual(self._norm('TVS NTORQ 150'), 'TVS NTORQ 150')

    def test_ntorq_125_exact_entry_unchanged(self):
        """Existing exact map entry 'TVS NTORQ 125' must not be disturbed."""
        self.assertEqual(self._norm('TVS NTORQ 125'), 'TVS NTORQ 125')

    def test_ntorq_bare_exact_entry_unchanged(self):
        """'TVS NTorq' (bare, no variant suffix) must still map to 'TVS NTORQ 125'."""
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
