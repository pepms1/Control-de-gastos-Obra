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


class SapSubtotalFallbackTests(TransactionsStatsTests):
    def test_sap_movements_without_safe_breakdown_use_the_sap_invoice_subtotal(self):
        stats = self._run([
            self._tx(6275.0, '2026-09-30', source='sap-sbo', sap={'invoiceSubtotal': 6275.0, 'invoiceIva': 0.0, 'invoiceTotal': 6275.0}),
            self._tx(154587.64, '2026-09-30', source='sap-sbo', sap={'invoiceSubtotal': 152776.95, 'invoiceIva': 1810.69, 'invoiceTotal': 154587.64}),
            self._tx(1000.0, '2026-08-01', source='sap-sbo'),
        ])
        self.assertEqual(stats['totalSinIva'], 6275.0 + 152776.95)
        self.assertEqual(stats['monthly'], [{'month': '2026-09', 'value': 159051.95}, {'month': '2026-08', 'value': 0.0}])
        self.assertEqual(stats['totalConIva'], 161862.64)


class AggregationWiringTests(TransactionsStatsTests):
    """El dashboard usa los mismos totales (agregación) que Buscar y la serie mensual sale del mismo cálculo."""

    def test_dashboard_totals_come_from_the_search_totals_aggregation(self):
        pipelines = []

        class AggCollection(phase1.FakeCollection):
            def aggregate(self, pipeline):
                pipelines.append(pipeline)
                if any(stage.get('$group', {}).get('_id') is None for stage in pipeline if '$group' in stage):
                    return iter([{'expensesGross': 1160.0, 'expensesTax': 160.0, 'expensesWithoutTax': 1000.0, 'incomeGross': 0.0, 'net': -1000.0}])
                return iter([{'_id': '2026-09', 'value': 700.0}, {'_id': '2026-08', 'value': 300.0}, {'_id': '', 'value': 5.0}])

        fake_db = SimpleNamespace(transactions=AggCollection([]), projects=phase1.FakeCollection([{'_id': ObjectId(self.project_id)}]))
        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ):
            stats = main.transactions_stats(type='EXPENSE', projectId=self.project_id, project=None, user={'username': 'agg', 'role': 'SUPERADMIN'})
        self.assertEqual(stats['totalSinIva'], 1000.0)
        self.assertEqual(stats['totalConIva'], 1160.0)
        self.assertEqual(stats['monthly'], [{'month': '2026-09', 'value': 700.0}, {'month': '2026-08', 'value': 300.0}])
        monthly_pipeline = next(p for p in pipelines if any('dateMonth' in stage.get('$project', {}) for stage in p))
        self.assertIn('montoSinIva', monthly_pipeline[1]['$project'])  # reutiliza el cálculo de los totales
