import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402
import test_estimations_phase1 as phase1  # noqa: E402

ADMIN = {'role': 'ADMIN', 'username': 'boss'}
CAPTURIST = {'role': 'VIEWER', 'username': 'mps', 'canCaptureEstimations': True}


class BudgetDiscountTests(phase1.EstimationsPhase1Tests):
    def setUp(self):
        super().setUp()
        self.fake_db = self._fake_db()
        self.capturist = dict(CAPTURIST, allowedProjectIds=[self.project_id])

    def _create(self, user=None, **overrides):
        overrides.setdefault('lineItems', [{'description': 'Yeso', 'unit': 'm2', 'quantity': 10, 'unitPrice': 100}])
        base = {'advanceAmortizationEnabled': False, 'advanceAmount': 0, 'retentionPct': 0}
        base.update(overrides)
        payload = self._base_budget_payload(**base)
        with patch.object(main, 'db', self.fake_db):
            return main.create_estimation_budget(payload, SimpleNamespace(headers={}, query_params={}), user=user or ADMIN)

    def _update(self, budget, payload, user=None):
        with patch.object(main, 'db', self.fake_db):
            return main.update_estimation_budget(budget['id'], payload, user=user or ADMIN)

    def test_percentage_discount_is_applied_to_unit_prices_and_totals(self):
        budget = self._create(discountMode='pct', discountPct=10)
        item = budget['lineItems'][0]
        self.assertEqual((item['listUnitPrice'], item['unitPrice'], item['amount']), (100.0, 90.0, 900.0))
        self.assertEqual(budget['totalContractedAmount'], 900.0)
        self.assertEqual((budget['discountMode'], budget['discountPct'], budget['discountAmount'], budget['listSubtotal']), ('pct', 10.0, 100.0, 1000.0))

    def test_closed_amount_discount_computes_the_percentage(self):
        budget = self._create(discountMode='amount', discountAmount=150)
        self.assertEqual(budget['discountPct'], 15.0)
        self.assertEqual(budget['lineItems'][0]['unitPrice'], 85.0)
        self.assertEqual(budget['totalContractedAmount'], 850.0)
        self.assertEqual(budget['discountAmount'], 150.0)

    def test_amount_discount_keeps_the_total_exact_with_awkward_percentages(self):
        budget = self._create(
            discountMode='amount', discountAmount=1234.56,
            lineItems=[
                {'description': 'A', 'unit': 'm2', 'quantity': 37.5, 'unitPrice': 413.27},
                {'description': 'B', 'unit': 'pza', 'quantity': 12, 'unitPrice': 1860.0},
            ],
        )
        subtotal = 37.5 * 413.27 + 12 * 1860.0
        self.assertAlmostEqual(budget['totalContractedAmount'], round(subtotal - 1234.56, 2), delta=0.05)

    def test_no_discount_by_default_and_invalid_values_are_rejected(self):
        plain = self._create()
        self.assertIsNone(plain['discountMode'])
        self.assertEqual(plain['totalContractedAmount'], 1000.0)
        self.assertNotIn('listUnitPrice', plain['lineItems'][0])
        for kwargs in (
            {'discountMode': 'pct', 'discountPct': 0}, {'discountMode': 'pct', 'discountPct': 100}, {'discountMode': 'pct', 'discountPct': 'x'},
            {'discountMode': 'amount', 'discountAmount': 1000}, {'discountMode': 'amount', 'discountAmount': 0}, {'discountMode': 'otro'},
        ):
            with self.assertRaises(HTTPException) as ctx:
                self._create(**kwargs)
            self.assertEqual(ctx.exception.status_code, 400, kwargs)

    def test_changing_or_removing_the_discount_always_starts_from_list_prices(self):
        budget = self._create(discountMode='pct', discountPct=10)
        changed = self._update(budget, {'discountMode': 'pct', 'discountPct': 20})
        self.assertEqual(changed['lineItems'][0]['unitPrice'], 80.0)  # no se encima sobre el 10 % anterior
        self.assertEqual(changed['lineItems'][0]['listUnitPrice'], 100.0)
        removed = self._update(budget, {'discountMode': None})
        self.assertEqual(removed['lineItems'][0]['unitPrice'], 100.0)
        self.assertNotIn('listUnitPrice', removed['lineItems'][0])
        self.assertEqual(removed['totalContractedAmount'], 1000.0)
        self.assertEqual(removed['discountPct'], 0.0)

    def test_editing_list_prices_keeps_the_discount(self):
        budget = self._create(discountMode='pct', discountPct=10)
        items = [dict(budget['lineItems'][0], unitPrice=200, listUnitPrice=None)]
        items[0].pop('listUnitPrice')
        edited = self._update(budget, {'lineItems': items})
        self.assertEqual(edited['lineItems'][0]['unitPrice'], 180.0)
        self.assertEqual(edited['lineItems'][0]['listUnitPrice'], 200.0)
        self.assertEqual(edited['totalContractedAmount'], 1800.0)

    def test_amount_discount_adjusts_its_percentage_when_prices_change(self):
        budget = self._create(discountMode='amount', discountAmount=100)  # 10 %
        items = [dict(budget['lineItems'][0], unitPrice=200)]
        items[0].pop('listUnitPrice')
        edited = self._update(budget, {'lineItems': items})
        self.assertEqual(edited['discountAmount'], 100.0)
        self.assertEqual(edited['discountPct'], 5.0)
        self.assertEqual(edited['totalContractedAmount'], 1900.0)

    def test_extras_are_not_discounted(self):
        budget = self._create(discountMode='pct', discountPct=10)
        items = [dict(budget['lineItems'][0], unitPrice=100)]
        items[0].pop('listUnitPrice')
        items.append({'description': 'Extra', 'unit': 'pza', 'quantity': 2, 'unitPrice': 50, 'isExtra': True})
        edited = self._update(budget, {'lineItems': items})
        extra = [i for i in edited['lineItems'] if i.get('isExtra')][0]
        self.assertEqual((extra['unitPrice'], extra['amount']), (50.0, 100.0))
        self.assertNotIn('listUnitPrice', extra)
        self.assertEqual(edited['totalContractedAmount'], 1000.0)  # 900 + 100
        self.assertEqual(edited['listSubtotal'], 1000.0)

    def test_estimations_use_the_discounted_prices(self):
        budget = self._create(discountMode='pct', discountPct=10)
        with patch.object(main, 'db', self.fake_db):
            created = main.create_estimation(
                budget['id'],
                {'periodStart': '2026-02-01', 'periodEnd': '2026-02-07', 'captureMode': 'global', 'globalProgressPct': 50},
                user=ADMIN,
            )
        self.assertEqual(created['periodSubtotal'], 450.0)  # 50 % de $900, no de $1,000

    def test_a_capturist_changing_the_discount_needs_reauthorization(self):
        budget = self._create(discountMode='pct', discountPct=10)
        self.assertEqual(budget['approvalStatus'], 'AUTORIZADO')
        changed = self._update(budget, {'discountMode': 'pct', 'discountPct': 12}, user=self.capturist)
        self.assertEqual(changed['approvalStatus'], 'PENDIENTE')


class BudgetAdvanceInputTests(BudgetDiscountTests):
    """El anticipo se captura en $ o en % del presupuesto (solo sirve para amortizar)."""

    def test_advance_as_percentage_of_the_total(self):
        budget = self._create(advanceAmortizationEnabled=True, advanceMode='pct', advancePct=20)
        self.assertEqual((budget['advanceMode'], budget['advancePctInput']), ('pct', 20.0))
        self.assertEqual(budget['advanceAmount'], 200.0)
        self.assertEqual(budget['advancePct'], 20.0)

    def test_percentage_advance_is_computed_on_the_discounted_total(self):
        budget = self._create(advanceAmortizationEnabled=True, advanceMode='pct', advancePct=10, discountMode='pct', discountPct=10)
        self.assertEqual(budget['totalContractedAmount'], 900.0)
        self.assertEqual(budget['advanceAmount'], 90.0)

    def test_percentage_advance_follows_the_total_when_concepts_or_discount_change(self):
        budget = self._create(advanceAmortizationEnabled=True, advanceMode='pct', advancePct=20)
        items = [dict(budget['lineItems'][0], unitPrice=200)]
        edited = self._update(budget, {'lineItems': items})
        self.assertEqual(edited['advanceAmount'], 400.0)
        discounted = self._update(budget, {'discountMode': 'pct', 'discountPct': 50})
        self.assertEqual(discounted['advanceAmount'], 200.0)  # 20 % de $1,000

    def test_switching_modes_and_plain_amount_still_work(self):
        plain = self._create(advanceAmortizationEnabled=True, advanceAmount=150)
        self.assertEqual((plain['advanceMode'], plain['advanceAmount'], plain['advancePct']), ('amount', 150.0, 15.0))
        to_pct = self._update(plain, {'advanceMode': 'pct', 'advancePct': 30})
        self.assertEqual((to_pct['advanceMode'], to_pct['advanceAmount']), ('pct', 300.0))
        back = self._update(plain, {'advanceMode': 'amount', 'advanceAmount': 120})
        self.assertEqual((back['advanceMode'], back['advanceAmount'], back['advancePctInput']), ('amount', 120.0, None))

    def test_invalid_percentage_is_rejected(self):
        for bad in (-1, 101, 'x'):
            with self.assertRaises(HTTPException) as ctx:
                self._create(advanceAmortizationEnabled=True, advanceMode='pct', advancePct=bad)
            self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException):
            self._create(advanceMode='raro')
