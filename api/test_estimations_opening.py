import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402
import test_estimations_phase1 as phase1  # noqa: E402

SUPERADMIN = phase1.SUPERADMIN
CAPTURIST = {'role': 'VIEWER', 'username': 'mps', 'canCaptureEstimations': True}


class EstimationsOpeningBalanceTests(phase1.EstimationsPhase1Tests):
    """Presupuestos que ya traen pagos al implementar el módulo (a media obra)."""

    def setUp(self):
        super().setUp()
        self.transactions = []
        self.fake_db = self._fake_db(transactions=self.transactions)
        # Total contratado: 100*60 + 20*200 = 10,000; retención 10%.
        self.budget = self._create_budget(
            self.fake_db, advanceAmortizationEnabled=False, advanceAmount=0, retentionPct=10
        )
        self.bid = self.budget['id']

    def _call(self, fn, *args, **kwargs):
        with patch.object(main, 'db', self.fake_db), patch.object(
            main, 'with_legacy_project_filter', side_effect=lambda q, _p: q
        ), patch.object(main, 'build_transactions_query', return_value={}):
            return fn(*args, **kwargs)

    def _set_opening(self, payload, user=SUPERADMIN):
        return self._call(main.set_estimation_budget_opening_balance, self.bid, payload, user=user)

    def _first_estimation(self, pct, user=SUPERADMIN, **extra):
        body = {
            'periodStart': '2026-03-01', 'periodEnd': '2026-03-07',
            'captureMode': 'global', 'globalProgressPct': pct, **extra,
        }
        return self._call(main.create_estimation, self.bid, body, user=user)

    # ---- release amount recognizes what was already paid ----

    def test_manual_opening_amounts_set_advance_and_discount_prior_payments(self):
        saved = self._set_opening({'manualAdvanceAmount': 1000, 'manualPriorPaidAmount': 2000})
        self.assertEqual(saved['openingAdvanceAmount'], 1000)
        self.assertEqual(saved['openingPriorPaidAmount'], 2000)
        self.assertEqual(saved['advanceAmount'], 1000)
        self.assertEqual(saved['advancePct'], 10)  # 1,000 / 10,000
        self.assertTrue(saved['advanceAmortizationEnabled'])
        self.assertEqual(saved['openingPaidPct'], 30)  # (1,000 + 2,000) / 10,000

        est = self._first_estimation(50)
        self.assertEqual(est['periodSubtotal'], 5000)
        self.assertEqual(est['retentionAmount'], 500)
        self.assertEqual(est['advanceAmortizationAmount'], 500)  # 10% del avance
        self.assertEqual(est['netBeforePriorPayments'], 4000)
        self.assertEqual(est['priorPaidApplied'], 2000)
        self.assertEqual(est['totalToPay'], 2000)  # lo que se libera

        budget = self._call(main.get_estimation_budget, self.bid, user=SUPERADMIN)
        self.assertEqual(budget['remainingOpeningPaidBalance'], 0)
        self.assertEqual(budget['remainingAdvanceBalance'], 500)

    def test_prior_payments_above_earned_amount_carry_over_to_next_estimation(self):
        self._set_opening({'manualPriorPaidAmount': 6000})
        first = self._first_estimation(50)  # neto 5,000 - 500 retención = 4,500
        self.assertEqual(first['priorPaidApplied'], 4500)
        self.assertEqual(first['totalToPay'], 0)

        self._call(main.submit_estimation, self.bid, first['id'], user=SUPERADMIN)
        self._call(main.approve_estimation, self.bid, first['id'], {}, user=SUPERADMIN)
        second = self._call(
            main.create_estimation, self.bid,
            {'periodStart': '2026-03-08', 'periodEnd': '2026-03-14', 'captureMode': 'global', 'globalProgressPct': 70},
            user=SUPERADMIN,
        )
        # 20% más = 2,000 - 200 retención = 1,800; quedaban 1,500 por descontar.
        self.assertEqual(second['priorPaidApplied'], 1500)
        self.assertEqual(second['totalToPay'], 300)

    def test_percentage_by_concept_also_recognizes_prior_payments(self):
        self._set_opening({'manualPriorPaidAmount': 1000})
        tuberia_id = self.budget['lineItems'][0]['id']
        est = self._call(
            main.create_estimation, self.bid,
            {
                'periodStart': '2026-03-01', 'periodEnd': '2026-03-07', 'captureMode': 'concept',
                'lineItems': [{'conceptoId': tuberia_id, 'progressPct': 100}],
            },
            user=CAPTURIST | {'allowedProjectIds': [self.project_id]},
        )
        self.assertEqual(est['periodSubtotal'], 6000)  # 100 ml x 60
        self.assertEqual(est['priorPaidApplied'], 1000)
        self.assertEqual(est['totalToPay'], 6000 - 600 - 1000)

    def test_editing_the_draft_does_not_discount_prior_payments_twice(self):
        self._set_opening({'manualPriorPaidAmount': 2000})
        est = self._first_estimation(50)
        self.assertEqual(est['priorPaidApplied'], 2000)
        edited = self._call(
            main.update_estimation, self.bid, est['id'],
            {'captureMode': 'global', 'globalProgressPct': 60}, user=SUPERADMIN,
        )
        self.assertEqual(edited['priorPaidApplied'], 2000)
        self.assertEqual(edited['totalToPay'], 6000 - 600 - 2000)

    def test_without_opening_balance_nothing_changes(self):
        est = self._first_estimation(50)
        self.assertEqual(est['priorPaidApplied'], 0)
        self.assertEqual(est['totalToPay'], est['netBeforePriorPayments'])

    # ---- opening balance built from real payments ----

    def _add_payments(self, *amounts):
        txs = [self._acero_transaction(amount) for amount in amounts]
        self.transactions.extend(txs)
        self.fake_db.transactions.docs.extend(txs)
        return [tx['_id'] for tx in txs]

    def test_opening_from_selected_payments_plus_manual_amount(self):
        anticipo, a_cuenta_1, a_cuenta_2 = self._add_payments(1500, 700, 300)
        saved = self._set_opening({
            'advanceTransactionIds': [anticipo],
            'priorPaymentTransactionIds': [a_cuenta_1, a_cuenta_2],
            'manualPriorPaidAmount': 500,
        })
        self.assertEqual(saved['openingAdvanceAmount'], 1500)
        self.assertEqual(saved['openingPriorPaidAmount'], 1500)  # 700 + 300 + 500
        self.assertEqual(saved['advanceAmount'], 1500)
        self.assertEqual(saved['openingAdvanceTransactionIds'], [anticipo])

    def test_unknown_payment_is_rejected(self):
        with self.assertRaises(HTTPException) as ctx:
            self._set_opening({'advanceTransactionIds': ['64b000000000000000000000']})
        self.assertEqual(ctx.exception.status_code, 400)

    def test_same_payment_cannot_be_advance_and_prior(self):
        tx_id, = self._add_payments(100)
        with self.assertRaises(HTTPException) as ctx:
            self._set_opening({'advanceTransactionIds': [tx_id], 'priorPaymentTransactionIds': [tx_id]})
        self.assertEqual(ctx.exception.status_code, 400)

    def test_payment_assigned_to_another_budget_is_rejected(self):
        tx_id, = self._add_payments(100)
        second = self._call(self._create_budget, self.fake_db, name='Segundo contrato mismo proveedor')
        self._call(main.replace_estimation_budget_transaction_links, second['id'], {'selectedTransactionIds': [tx_id]}, user=SUPERADMIN)
        with self.assertRaises(HTTPException) as ctx:
            self._set_opening({'priorPaymentTransactionIds': [tx_id]})
        self.assertEqual(ctx.exception.status_code, 409)

    def test_multiple_active_budgets_require_payments_assigned_first(self):
        tx_id, = self._add_payments(1000)
        self._call(self._create_budget, self.fake_db, name='Segundo contrato mismo proveedor')
        with self.assertRaises(HTTPException) as ctx:
            self._set_opening({'priorPaymentTransactionIds': [tx_id]})
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn('Asignar pagos', ctx.exception.detail)

        self._call(main.replace_estimation_budget_transaction_links, self.bid, {'selectedTransactionIds': [tx_id]}, user=SUPERADMIN)
        saved = self._set_opening({'priorPaymentTransactionIds': [tx_id]})
        self.assertEqual(saved['openingPriorPaidAmount'], 1000)

    # ---- guards ----

    def test_opening_balance_is_locked_after_the_first_estimation(self):
        self._first_estimation(10)
        with self.assertRaises(HTTPException) as ctx:
            self._set_opening({'manualPriorPaidAmount': 100})
        self.assertEqual(ctx.exception.status_code, 409)

    def test_opening_balance_can_be_reset_before_first_estimation(self):
        self._set_opening({'manualAdvanceAmount': 500, 'manualPriorPaidAmount': 800})
        saved = self._set_opening({})
        self.assertEqual(saved['openingAdvanceAmount'], 0)
        self.assertEqual(saved['openingPriorPaidAmount'], 0)

    def test_negative_manual_amounts_are_rejected(self):
        with self.assertRaises(HTTPException) as ctx:
            self._set_opening({'manualPriorPaidAmount': -5})
        self.assertEqual(ctx.exception.status_code, 400)

    def test_capture_only_user_cannot_set_opening_balance(self):
        with self.assertRaises(HTTPException) as ctx:
            main.require_admin_or_superadmin(user=CAPTURIST)
        self.assertEqual(ctx.exception.status_code, 403)


# Inherited phase-1 tests already run in their own module.
for _name in [n for n in dir(phase1.EstimationsPhase1Tests) if n.startswith('test_')]:
    setattr(EstimationsOpeningBalanceTests, _name, None)


if __name__ == '__main__':
    unittest.main()
