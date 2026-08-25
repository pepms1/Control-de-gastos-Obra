import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402
from test_budgets_phase1 import FakeCollection  # noqa: E402


SUPERADMIN = {'role': 'SUPERADMIN', 'username': 'admin'}


class _FakeCursor(list):
    """Minimal stand-in for a pymongo Cursor: supports the chained
    .sort().limit() calls used by the estimaciones endpoints, on top of
    FakeCollection's plain-list find() results."""

    def sort(self, key, direction=1):
        super().sort(key=lambda doc: doc.get(key), reverse=(direction == -1))
        return self

    def limit(self, n):
        del self[n:]
        return self


class FakeCollectionWithCursor(FakeCollection):
    def find(self, query=None, projection=None):
        return _FakeCursor(super().find(query, projection))

    def count_documents(self, query=None):
        return len(super().find(query or {}))


class EstimationsPhase1Tests(unittest.TestCase):
    def setUp(self):
        self.project_id = str(ObjectId())

    def _fake_db(self, estimation_budgets=None, estimations=None, projects=None):
        return SimpleNamespace(
            estimationBudgets=FakeCollectionWithCursor(estimation_budgets or []),
            estimations=FakeCollectionWithCursor(estimations or []),
            projects=FakeCollection(projects or [{'_id': ObjectId(self.project_id)}]),
        )

    def _base_budget_payload(self, **overrides):
        payload = {
            'projectId': self.project_id,
            'supplierCardCode': 'P001',
            'businessPartner': 'ACERO SA',
            'supplierName': 'Acero SA',
            'name': 'Plomería',
            'retentionPct': 5,
            'advanceAmortizationEnabled': True,
            'advanceAmount': 1000,
            'lineItems': [
                {'description': 'Tubería', 'unit': 'ml', 'quantity': 100, 'unitPrice': 60},
                {'description': 'Conexiones', 'unit': 'pza', 'quantity': 20, 'unitPrice': 200},
            ],
        }
        payload.update(overrides)
        return payload

    def _create_budget(self, fake_db, **overrides):
        with patch.object(main, 'db', fake_db):
            return main.create_estimation_budget(
                self._base_budget_payload(**overrides),
                request=SimpleNamespace(headers={}, query_params={}),
                user=SUPERADMIN,
            )

    def _create_estimation(self, fake_db, budget_id, quantities_by_concepto_id, **overrides):
        payload = {
            'periodStart': '2026-01-01',
            'periodEnd': '2026-01-07',
            'lineItems': [
                {'conceptoId': concepto_id, 'periodQuantity': quantity}
                for concepto_id, quantity in quantities_by_concepto_id.items()
            ],
        }
        payload.update(overrides)
        with patch.object(main, 'db', fake_db):
            return main.create_estimation(budget_id, payload, user=SUPERADMIN)

    # ---- estimation budget (presupuesto por conceptos) ----

    def test_create_budget_computes_totals_and_advance_pct(self):
        fake_db = self._fake_db()
        created = self._create_budget(fake_db)

        self.assertEqual(created['totalContractedAmount'], 10000.0)
        self.assertEqual(created['advancePct'], 10.0)
        self.assertEqual(len(created['lineItems']), 2)
        self.assertEqual(created['remainingAdvanceBalance'], 1000.0)
        self.assertEqual(created['estimationsCount'], 0)

    def test_advance_pct_is_zero_when_total_contracted_is_zero(self):
        totals = main.compute_estimation_budget_totals([], advance_amount=500)
        self.assertEqual(totals['totalContractedAmount'], 0.0)
        self.assertEqual(totals['advancePct'], 0.0)

    # ---- money math (compute_estimation_money_fields) in isolation ----

    def test_compute_estimation_money_fields_caps_advance_at_remaining_balance(self):
        estimation_budget = {
            '_id': ObjectId(),
            'retentionPct': 5,
            'advanceAmortizationEnabled': True,
            'advancePct': 10.0,
            'advanceAmount': 100,
        }
        fake_db = self._fake_db(estimations=[])
        with patch.object(main, 'db', fake_db):
            fields = main.compute_estimation_money_fields(
                estimation_budget,
                [{'periodAmount': 5000}],
            )
        # 10% of 5000 would be 500, but only 100 of advance remains.
        self.assertEqual(fields['periodSubtotal'], 5000.0)
        self.assertEqual(fields['retentionAmount'], 250.0)
        self.assertEqual(fields['advanceAmortizationAmount'], 100.0)
        self.assertEqual(fields['totalToPay'], 4650.0)

    def test_compute_estimation_money_fields_disabled_forces_zero_amortization(self):
        estimation_budget = {
            '_id': ObjectId(),
            'retentionPct': 0,
            'advanceAmortizationEnabled': False,
            'advancePct': 50.0,
            'advanceAmount': 10000,
        }
        fake_db = self._fake_db(estimations=[])
        with patch.object(main, 'db', fake_db):
            fields = main.compute_estimation_money_fields(estimation_budget, [{'periodAmount': 1000}])
        self.assertEqual(fields['advanceAmortizationAmount'], 0.0)
        self.assertEqual(fields['totalToPay'], 1000.0)

    # ---- end-to-end folio / cumulative / advance-balance wiring ----

    def test_two_estimations_carry_cumulative_quantities_and_drain_advance_balance(self):
        fake_db = self._fake_db()
        budget = self._create_budget(fake_db)
        tuberia_id = budget['lineItems'][0]['id']
        conexiones_id = budget['lineItems'][1]['id']

        first = self._create_estimation(fake_db, budget['id'], {tuberia_id: 30, conexiones_id: 5})
        self.assertEqual(first['folio'], 1)
        self.assertTrue(first['isLatest'])
        for line in first['lineItems']:
            self.assertEqual(line['previousCumulativeQuantity'], 0)
        self.assertEqual(first['periodSubtotal'], 2800.0)  # 30*60 + 5*200
        self.assertEqual(first['retentionAmount'], 140.0)  # 5% of 2800
        self.assertEqual(first['advanceAmortizationAmount'], 280.0)  # 10% of 2800, within 1000 balance
        self.assertEqual(first['totalToPay'], 2380.0)

        second = self._create_estimation(fake_db, budget['id'], {tuberia_id: 40, conexiones_id: 5})
        self.assertEqual(second['folio'], 2)
        lines_by_concepto = {line['conceptoId']: line for line in second['lineItems']}
        self.assertEqual(lines_by_concepto[tuberia_id]['previousCumulativeQuantity'], 30)
        self.assertEqual(lines_by_concepto[tuberia_id]['cumulativeQuantity'], 70)
        self.assertEqual(lines_by_concepto[conexiones_id]['previousCumulativeQuantity'], 5)
        self.assertEqual(second['periodSubtotal'], 3400.0)  # 40*60 + 5*200
        # remaining advance balance before this estimation was 1000 - 280 = 720
        self.assertEqual(second['advanceAmortizationAmount'], 340.0)  # 10% of 3400, within 720 balance

        with patch.object(main, 'db', fake_db):
            budget_detail = main.get_estimation_budget(budget['id'], user=SUPERADMIN)
        self.assertEqual(budget_detail['remainingAdvanceBalance'], 380.0)  # 1000 - 280 - 340
        self.assertEqual(budget_detail['estimationsCount'], 2)

        # first estimation is no longer the latest once a second one exists
        with patch.object(main, 'db', fake_db):
            first_reloaded = main.get_estimation(budget['id'], first['id'], user=SUPERADMIN)
        self.assertFalse(first_reloaded['isLatest'])

    def test_only_latest_estimation_can_be_edited_or_deleted(self):
        fake_db = self._fake_db()
        budget = self._create_budget(fake_db)
        tuberia_id = budget['lineItems'][0]['id']
        conexiones_id = budget['lineItems'][1]['id']

        first = self._create_estimation(fake_db, budget['id'], {tuberia_id: 10, conexiones_id: 1})
        second = self._create_estimation(fake_db, budget['id'], {tuberia_id: 10, conexiones_id: 1})

        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as ctx:
                main.update_estimation(
                    budget['id'],
                    first['id'],
                    {'periodQuantity': 99, 'lineItems': [{'conceptoId': tuberia_id, 'periodQuantity': 99}]},
                    user=SUPERADMIN,
                )
        self.assertEqual(ctx.exception.status_code, 400)

        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as ctx:
                main.delete_estimation(budget['id'], first['id'], user=SUPERADMIN)
        self.assertEqual(ctx.exception.status_code, 409)

        with patch.object(main, 'db', fake_db):
            updated_second = main.update_estimation(
                budget['id'],
                second['id'],
                {
                    'notes': 'ajuste',
                    'lineItems': [
                        {'conceptoId': tuberia_id, 'periodQuantity': 20},
                        {'conceptoId': conexiones_id, 'periodQuantity': 1},
                    ],
                },
                user=SUPERADMIN,
            )
        self.assertEqual(updated_second['notes'], 'ajuste')
        lines_by_concepto = {line['conceptoId']: line for line in updated_second['lineItems']}
        self.assertEqual(lines_by_concepto[tuberia_id]['periodQuantity'], 20)
        self.assertEqual(lines_by_concepto[tuberia_id]['previousCumulativeQuantity'], 10)  # excludes itself

        with patch.object(main, 'db', fake_db):
            result = main.delete_estimation(budget['id'], second['id'], user=SUPERADMIN)
        self.assertEqual(result, {'ok': True})
        with patch.object(main, 'db', fake_db):
            remaining = main.list_estimations(budget['id'], user=SUPERADMIN)
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]['id'], first['id'])

    # ---- concepto shrink guard ----

    def test_concepto_shrink_blocked_only_when_it_has_recorded_progress(self):
        fake_db = self._fake_db()
        budget = self._create_budget(fake_db)
        tuberia_id = budget['lineItems'][0]['id']
        conexiones_id = budget['lineItems'][1]['id']
        self._create_estimation(fake_db, budget['id'], {tuberia_id: 10, conexiones_id: 0})

        # Reducing the quantity of a concepto with recorded progress must fail.
        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as ctx:
                main.update_estimation_budget(
                    budget['id'],
                    {
                        'lineItems': [
                            {'id': tuberia_id, 'description': 'Tubería', 'unit': 'ml', 'quantity': 5, 'unitPrice': 60},
                            {'id': conexiones_id, 'description': 'Conexiones', 'unit': 'pza', 'quantity': 20, 'unitPrice': 200},
                        ]
                    },
                    user=SUPERADMIN,
                )
        self.assertEqual(ctx.exception.status_code, 409)

        # A concepto with zero recorded progress can be removed freely.
        with patch.object(main, 'db', fake_db):
            updated = main.update_estimation_budget(
                budget['id'],
                {
                    'lineItems': [
                        {'id': tuberia_id, 'description': 'Tubería', 'unit': 'ml', 'quantity': 100, 'unitPrice': 65},
                    ]
                },
                user=SUPERADMIN,
            )
        self.assertEqual(len(updated['lineItems']), 1)
        self.assertEqual(updated['totalContractedAmount'], 6500.0)

    def test_delete_estimation_budget_blocked_when_estimations_exist(self):
        fake_db = self._fake_db()
        budget = self._create_budget(fake_db)
        tuberia_id = budget['lineItems'][0]['id']
        self._create_estimation(fake_db, budget['id'], {tuberia_id: 1})

        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as ctx:
                main.delete_estimation_budget(budget['id'], user=SUPERADMIN)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_viewer_role_is_rejected(self):
        with self.assertRaises(HTTPException) as ctx:
            main.require_admin_or_superadmin(user={'role': 'VIEWER'})
        self.assertEqual(ctx.exception.status_code, 403)


if __name__ == '__main__':
    unittest.main()
