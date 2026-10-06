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
import test_estimations_phase1 as phase1  # noqa: E402

ADMIN = {'role': 'ADMIN', 'username': 'boss'}
CAPTURIST = {'role': 'VIEWER', 'username': 'mps', 'canCaptureEstimations': True}


class SupplierEstimationTests(phase1.EstimationsPhase1Tests):
    """Una estimación por proveedor, con avance de varios de sus presupuestos."""

    def setUp(self):
        super().setUp()
        self.fake_db = self._fake_db()
        base = dict(advanceAmortizationEnabled=False, advanceAmount=0, retentionPct=10)
        self.depto1 = self._create_budget(self.fake_db, name='Depto 201', **base)
        self.depto2 = self._create_budget(self.fake_db, name='Depto 202', **base)
        self.other_supplier = self._create_budget(
            self.fake_db, name='Pintura', supplierCardCode='P002', businessPartner='OTRO SA', supplierName='Otro SA', **base
        )
        self.supplier_key = self.depto1['supplierKey']
        self.capturist = dict(CAPTURIST, allowedProjectIds=[self.project_id])

    def _call(self, fn, *args, **kwargs):
        with patch.object(main, 'db', self.fake_db):
            return fn(*args, **kwargs)

    def _part(self, budget, pct, **extra):
        return {'estimationBudgetId': budget['id'], 'captureMode': 'global', 'globalProgressPct': pct, **extra}

    def _create(self, parts, user=None, supplier_key=None, **extra):
        payload = {
            'projectId': self.project_id,
            'supplierKey': supplier_key or self.supplier_key,
            'periodStart': '2026-02-01',
            'periodEnd': '2026-02-07',
            'parts': parts,
            **extra,
        }
        return self._call(main.create_supplier_estimation, payload, user=user or self.capturist)

    def _close(self, batch, authorized=None, note=''):
        self._call(main.submit_supplier_estimation, batch['id'], user=self.capturist)
        body = {} if authorized is None else {'authorizedAmount': authorized, 'authorizationNote': note}
        return self._call(main.approve_supplier_estimation, batch['id'], body, user=ADMIN)

    def test_one_estimation_covers_several_budgets_of_the_supplier(self):
        batch = self._create([self._part(self.depto1, 50), self._part(self.depto2, 20)])
        self.assertEqual(batch['folio'], 1)
        self.assertEqual(batch['workflowStatus'], 'BORRADOR')
        self.assertEqual(len(batch['parts']), 2)
        # 50% de 10,000 y 20% de 10,000, con 10 % de retención cada uno
        self.assertEqual(batch['periodSubtotal'], 7000.0)
        self.assertEqual(batch['retentionAmount'], 700.0)
        self.assertEqual(batch['totalToPay'], 6300.0)
        self.assertEqual({p['batchId'] for p in batch['parts']}, {batch['id']})
        self.assertEqual({p['folio'] for p in batch['parts']}, {1})

    def test_folio_is_consecutive_per_supplier_not_per_budget(self):
        first = self._create([self._part(self.depto1, 50)])
        self._close(first)
        second = self._create([self._part(self.depto2, 30)])  # otro presupuesto, mismo proveedor
        self.assertEqual(second['folio'], 2)
        self._close(second)
        # otro proveedor arranca en 1
        other = self._create([self._part(self.other_supplier, 10)], supplier_key=self.other_supplier['supplierKey'])
        self.assertEqual(other['folio'], 1)

    def test_only_one_open_estimation_per_supplier(self):
        self._create([self._part(self.depto1, 10)])
        with self.assertRaises(HTTPException) as ctx:
            self._create([self._part(self.depto2, 10)])
        self.assertEqual(ctx.exception.status_code, 409)
        # pero otro proveedor sí puede abrir la suya
        self._create([self._part(self.other_supplier, 10)], supplier_key=self.other_supplier['supplierKey'])

    def test_budgets_of_other_suppliers_and_complete_ones_are_rejected(self):
        with self.assertRaises(HTTPException) as ctx:
            self._create([self._part(self.other_supplier, 10)])
        self.assertEqual(ctx.exception.status_code, 400)
        done = self._create([self._part(self.depto1, 100)])
        self._close(done)
        self.assertTrue(self._call(main.get_estimation_budget, self.depto1['id'], user=ADMIN)['isComplete'])
        self.assertFalse(self._call(main.get_estimation_budget, self.depto2['id'], user=ADMIN)['isComplete'])
        with self.assertRaises(HTTPException) as ctx:
            self._create([self._part(self.depto1, 100), self._part(self.depto2, 10)])
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn('100', ctx.exception.detail)
        self.assertEqual(len(self._create([self._part(self.depto2, 10)])['parts']), 1)

    def test_budget_selected_without_progress_creates_no_part(self):
        batch = self._create([self._part(self.depto1, 40), self._part(self.depto2, 0)])
        self.assertEqual([p['estimationBudgetId'] for p in batch['parts']], [self.depto1['id']])

    def test_single_authorized_total_is_split_and_summed(self):
        batch = self._create([self._part(self.depto1, 50), self._part(self.depto2, 20)], requestedAmount=6000)
        self.assertEqual(batch['requestedAmount'], 6000.0)
        self._call(main.submit_supplier_estimation, batch['id'], user=self.capturist)
        with self.assertRaises(HTTPException) as ctx:  # menos de lo solicitado sin motivo
            self._call(main.approve_supplier_estimation, batch['id'], {'authorizedAmount': 5000}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException) as ctx:  # más de lo solicitado
            self._call(main.approve_supplier_estimation, batch['id'], {'authorizedAmount': 7000, 'authorizationNote': 'x'}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 400)
        approved = self._call(
            main.approve_supplier_estimation, batch['id'], {'authorizedAmount': 5000, 'authorizationNote': 'ajuste'}, user=ADMIN
        )
        self.assertEqual(approved['workflowStatus'], 'APROBADA')
        self.assertEqual(approved['authorizedAmount'], 5000.0)
        self.assertEqual(round(sum(p['authorizedAmount'] for p in approved['parts']), 2), 5000.0)
        self.assertEqual(approved['approvedBy'], 'boss')
        self.assertEqual(approved['paymentStatus'], 'POR_PAGAR')

    def test_return_and_edit_the_draft_keeps_the_folio(self):
        batch = self._create([self._part(self.depto1, 50)])
        self._call(main.submit_supplier_estimation, batch['id'], user=self.capturist)
        returned = self._call(main.return_supplier_estimation, batch['id'], {'reason': 'faltan m2'}, user=ADMIN)
        self.assertEqual(returned['workflowStatus'], 'BORRADOR')
        edited = self._call(
            main.update_supplier_estimation,
            batch['id'],
            {'periodStart': '2026-02-01', 'periodEnd': '2026-02-08', 'parts': [self._part(self.depto1, 60), self._part(self.depto2, 10)]},
            user=self.capturist,
        )
        self.assertEqual(edited['id'], batch['id'])
        self.assertEqual(edited['folio'], 1)
        self.assertEqual(len(edited['parts']), 2)
        self.assertEqual(edited['periodSubtotal'], 7000.0)
        listed = self._call(main.list_supplier_estimations, self.supplier_key, self.project_id, user=ADMIN)
        self.assertEqual(len(listed), 1)

    def test_payments_reconcile_at_supplier_level(self):
        first = self._close(self._create([self._part(self.depto1, 50)]))        # 4,500
        second = self._close(self._create([self._part(self.depto2, 50)]))       # 4,500
        self.assertEqual((first['authorizedAmount'], second['authorizedAmount']), (4500.0, 4500.0))

        def listing(paid_by_budget):
            def fake_paid(project_id, supplier_key, budget_id, **kwargs):
                return paid_by_budget.get(budget_id, 0.0)
            with patch.object(main, 'db', self.fake_db), patch.object(main, 'compute_estimation_budget_paid_amount', side_effect=fake_paid):
                return main.list_supplier_estimations(self.supplier_key, self.project_id, user=ADMIN)

        self.assertEqual([b['paymentStatus'] for b in listing({})], ['POR_PAGAR', 'POR_PAGAR'])
        # un pago de 4,500 asignado a cualquiera de los presupuestos cubre la primera estimación
        self.assertEqual([b['paymentStatus'] for b in listing({self.depto2['id']: 4500.0})], ['PAGADA', 'POR_PAGAR'])
        self.assertEqual([b['paymentStatus'] for b in listing({self.depto1['id']: 4500.0, self.depto2['id']: 4500.0})], ['PAGADA', 'PAGADA'])
        self.assertEqual([b['paymentStatus'] for b in listing({})], ['POR_PAGAR', 'POR_PAGAR'])  # se revierte al quitar los pagos

    def test_supplier_folio_can_be_changed_and_keeps_order(self):
        first = self._close(self._create([self._part(self.depto1, 30)]))
        second = self._close(self._create([self._part(self.depto2, 30)]))
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.set_supplier_estimation_folio, first['id'], {'folio': 2}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(self._call(main.set_supplier_estimation_folio, second['id'], {'folio': 32}, user=ADMIN)['folio'], 32)
        self.assertEqual(self._call(main.set_supplier_estimation_folio, first['id'], {'folio': 31}, user=ADMIN)['folio'], 31)
        nxt = self._create([self._part(self.depto2, 10)])
        self.assertEqual(nxt['folio'], 33)

    def test_pending_summary_counts_each_estimation_once(self):
        batch = self._create([self._part(self.depto1, 30), self._part(self.depto2, 30)])
        self._call(main.submit_supplier_estimation, batch['id'], user=self.capturist)
        summary = self._call(main.estimations_pending_summary, user=ADMIN)
        self.assertEqual(summary['pendingReview'], 1)
        queue = self._call(main.list_estimations_queue, projectId=self.project_id, status='ENVIADA', user=ADMIN)
        self.assertEqual(len(queue['items']), 1)
        self.assertEqual(len(queue['items'][0]['parts']), 2)

    def test_old_per_budget_endpoints_delegate_for_batched_parts(self):
        batch = self._create([self._part(self.depto1, 30), self._part(self.depto2, 30)])
        part = batch['parts'][0]
        self._call(main.submit_estimation, part['estimationBudgetId'], part['id'], user=self.capturist)
        self.assertEqual(self._call(main.get_supplier_estimation, batch['id'], user=ADMIN)['workflowStatus'], 'ENVIADA')
        approved = self._call(main.approve_estimation, part['estimationBudgetId'], part['id'], {}, user=ADMIN)
        self.assertEqual(approved['workflowStatus'], 'APROBADA')
        self.assertEqual(len(approved['parts']), 2)

    def test_draft_can_be_deleted_only_if_latest(self):
        first = self._close(self._create([self._part(self.depto1, 30)]))
        second = self._create([self._part(self.depto2, 30)])
        with self.assertRaises(HTTPException):
            self._call(main.delete_supplier_estimation, first['id'], user=self.capturist)  # aprobada
        self.assertEqual(self._call(main.delete_supplier_estimation, second['id'], user=self.capturist), {'ok': True})
        self.assertEqual(len(self._call(main.list_supplier_estimations, self.supplier_key, self.project_id, user=ADMIN)), 1)
