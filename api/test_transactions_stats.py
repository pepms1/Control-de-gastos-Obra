import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

from bson import ObjectId  # noqa: E402

import main  # noqa: E402
import test_estimations_phase1 as phase1  # noqa: E402


class TransactionsStatsTests(unittest.TestCase):
    def setUp(self):
        self.project_id = str(ObjectId())
        self.user = {'username': 'stats-user', 'role': 'SUPERADMIN'}

    def _run(self, docs, type_value='EXPENSE'):
        fake_db = SimpleNamespace(
            transactions=phase1.FakeCollection(docs),
            projects=phase1.FakeCollection([{'_id': ObjectId(self.project_id)}]),
        )
        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ):
            return main.transactions_stats(type=type_value, projectId=self.project_id, project=None, user=self.user)

    def _tx(self, amount, date, subtotal=None, **extra):
        tx = {'_id': str(ObjectId()), 'amount': amount, 'date': date}
        if subtotal is not None:
            tx['tax'] = {'subtotal': subtotal, 'iva': round(abs(amount) - abs(subtotal), 2), 'totalFactura': abs(amount)}
        tx.update(extra)
        return tx

    def test_totals_with_and_without_iva_and_by_month(self):
        stats = self._run([
            self._tx(116.0, '2026-09-10', subtotal=100.0),
            self._tx(232.0, '2026-09-20', subtotal=200.0),
            self._tx(58.0, '2026-10-01', subtotal=50.0),
        ])
        self.assertEqual(stats['count'], 3)
        self.assertEqual(stats['totalConIva'], 406.0)
        self.assertEqual(stats['totalSinIva'], 350.0)
        self.assertEqual(stats['monthly'], [{'month': '2026-10', 'value': 50.0}, {'month': '2026-09', 'value': 300.0}])

    def test_negative_amounts_keep_their_sign(self):
        stats = self._run([self._tx(116.0, '2026-09-10', subtotal=100.0), self._tx(-58.0, '2026-09-11', subtotal=50.0)])
        self.assertEqual(stats['totalConIva'], 58.0)
        self.assertEqual(stats['totalSinIva'], 50.0)

    def test_excluded_expenses_are_skipped_and_cash_without_tax_counts_as_zero_subtotal(self):
        stats = self._run([
            self._tx(116.0, '2026-09-10', subtotal=100.0),
            self._tx(500.0, '2026-09-11', subtotal=431.03, excludeFromExpenseViews=True),
            self._tx(900.0, '2026-09-12', financialKind='contribution_withdrawal'),
            self._tx(40.0, '2026-09-13'),
        ])
        self.assertEqual(stats['totalConIva'], 156.0)
        self.assertEqual(stats['totalSinIva'], 100.0)

    def test_invalid_project_access_for_viewer_returns_empty(self):
        fake_db = SimpleNamespace(transactions=phase1.FakeCollection([]), projects=phase1.FakeCollection([{'_id': ObjectId(self.project_id)}]))
        viewer = {'username': 'v', 'role': 'VIEWER', 'allowedProjectIds': []}
        with patch.object(main, 'db', fake_db):
            stats = main.transactions_stats(type='EXPENSE', projectId=self.project_id, project=None, user=viewer)
        self.assertEqual(stats['count'], 0)


if __name__ == '__main__':
    unittest.main()
