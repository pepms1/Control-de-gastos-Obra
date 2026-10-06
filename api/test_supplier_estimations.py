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
SUPERADMIN = phase1.SUPERADMIN
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
            with patch.object(main, 'db', self.fake_db), patch.object(
                main, 'compute_supplier_paid_amount', return_value=sum(paid_by_budget.values())
            ):
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


class AdvanceInEstimationTests(SupplierEstimationTests):
    """Anticipo entregado como parte de la estimación (sin ir al presupuesto)."""

    def _advance(self, budget, amount, **extra):
        return {'estimationBudgetId': budget['id'], 'advanceAmount': amount, 'noProgress': True, **extra}

    def test_advance_only_part_is_authorized_paid_and_registered_as_the_budget_advance(self):
        batch = self._create([self._advance(self.depto1, 3000)])
        self.assertEqual(batch['periodSubtotal'], 0.0)
        self.assertEqual(batch['retentionAmount'], 0.0)
        self.assertEqual(batch['advanceGivenAmount'], 3000.0)
        self.assertEqual(batch['totalToPay'], 3000.0)
        approved = self._close(batch)
        self.assertEqual(approved['authorizedAmount'], 3000.0)
        budget = self._call(main.get_estimation_budget, self.depto1['id'], user=ADMIN)
        self.assertEqual(budget['advanceAmount'], 3000.0)
        self.assertTrue(budget['advanceAmortizationEnabled'])
        self.assertEqual(budget['advanceGivenAmount'], 3000.0)
        self.assertEqual(budget['remainingAdvanceBalance'], 3000.0)

    def test_the_delivered_advance_is_amortized_in_the_next_progress_estimation(self):
        self._close(self._create([self._advance(self.depto1, 3000)]))
        nxt = self._create([self._part(self.depto1, 50)])
        part = nxt['parts'][0]
        # 30 % del avance de $5,000 = $1,500 de amortización; retención 10 % = $500
        self.assertEqual(part['advanceAmortizationAmount'], 1500.0)
        self.assertEqual(part['totalToPay'], 3000.0)

    def test_progress_and_advance_in_the_same_estimation(self):
        batch = self._create([
            self._part(self.depto1, 50, advanceAmount=1000),
            self._advance(self.depto2, 2000),
        ])
        self.assertEqual(batch['advanceGivenAmount'], 3000.0)
        self.assertEqual(batch['totalToPay'], 4500.0 + 1000.0 + 2000.0)
        self._call(main.submit_supplier_estimation, batch['id'], user=self.capturist)
        listed = self._call(main.get_supplier_estimation, batch['id'], user=ADMIN)
        self.assertEqual(listed['totalToPay'], 7500.0)  # el refresco no pierde el anticipo

    def test_advance_payment_is_matched_inside_the_sequence(self):
        batch = self._close(self._create([self._advance(self.depto1, 3000)]))

        def listing(paid):
            with patch.object(main, 'db', self.fake_db), patch.object(
                main, 'compute_supplier_paid_amount', return_value=sum(paid.values())
            ):
                return main.list_supplier_estimations(self.supplier_key, self.project_id, user=ADMIN)

        self.assertEqual(listing({})[0]['paymentStatus'], 'POR_PAGAR')
        self.assertEqual(listing({self.depto1['id']: 2999.0})[0]['paymentStatus'], 'POR_PAGAR')
        self.assertEqual(listing({self.depto1['id']: 3000.0})[0]['paymentStatus'], 'PAGADA')
        self.assertEqual(batch['authorizedAmount'], 3000.0)

    def test_zero_or_invalid_advance_without_progress_is_rejected(self):
        for bad in (0, '', None):
            with self.assertRaises(HTTPException):
                self._create([self._advance(self.depto1, bad)])
        with self.assertRaises(HTTPException) as ctx:
            self._create([self._advance(self.depto1, -5)])
        self.assertEqual(ctx.exception.status_code, 400)

    def test_advance_can_still_be_given_to_a_budget_at_100_percent_only_if_needed(self):
        # un presupuesto completo sigue sin admitir avance, pero sí un anticipo
        self._close(self._create([self._part(self.depto1, 100)]))
        with self.assertRaises(HTTPException):
            self._create([self._part(self.depto1, 100)])
        self.assertEqual(self._create([self._advance(self.depto1, 500)])['advanceGivenAmount'], 500.0)


class _SettingsCollection(phase1.FakeCollectionWithCursor):
    def update_one(self, query, update, upsert=False):
        if upsert and not self.find(query):
            self.insert_one({**query, **update.get('$set', {})})
            return
        super().update_one(query, update)


class SupplierPaidTests(phase1.EstimationsPhase1Tests):
    """Pagado a la fecha del proveedor: todos sus pagos, salvo los desasignados."""

    def setUp(self):
        super().setUp()
        self.txs = [
            self._acero_transaction(4500),
            self._acero_transaction(200),
            self._acero_transaction(300),
        ]
        self.fake_db = self._fake_db(transactions=self.txs)
        self.fake_db.supplierPaymentSettings = _SettingsCollection([])
        base = dict(advanceAmortizationEnabled=False, advanceAmount=0, retentionPct=10)
        self.patches = [
            patch.object(main, 'db', self.fake_db),
            patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q),
            patch.object(main, 'build_transactions_query', return_value={}),
        ]
        for p in self.patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self.patches])
        self.depto1 = self._create_budget(self.fake_db, name='Depto 201', **base)
        self.depto2 = self._create_budget(self.fake_db, name='Depto 202', **base)
        self.key = self.depto1['supplierKey']

    def test_all_supplier_payments_count_by_default_even_with_several_budgets(self):
        self.assertEqual(main.compute_supplier_paid_amount(self.project_id, self.key), 5000.0)
        budget = main.get_estimation_budget(self.depto1['id'], user=ADMIN)
        self.assertEqual(budget['supplierPaidAmount'], 5000.0)
        # con varios presupuestos activos el pagado POR presupuesto sigue siendo la asignación manual
        self.assertEqual(budget['paidAmount'], 0.0)
        listed = main.list_estimation_budgets(projectId=self.project_id, user=ADMIN)
        self.assertEqual({row['supplierPaidAmount'] for row in listed}, {5000.0})

    def test_unassigning_a_payment_removes_it_from_the_supplier_total(self):
        listing = main.list_estimation_supplier_payments(self.key, self.project_id, user=ADMIN)
        self.assertEqual(listing['paidAmount'], 5000.0)
        self.assertFalse(any(row['isExcluded'] for row in listing['items']))
        result = main.set_estimation_supplier_payments(
            {'projectId': self.project_id, 'supplierKey': self.key, 'excludedTransactionIds': [self.txs[0]['_id']]}, user=ADMIN
        )
        self.assertEqual(result['paidAmount'], 500.0)
        listing = main.list_estimation_supplier_payments(self.key, self.project_id, user=ADMIN)
        self.assertEqual((listing['paidAmount'], listing['excludedAmount']), (500.0, 4500.0))
        self.assertEqual([row['isExcluded'] for row in listing['items'] if row['id'] == self.txs[0]['_id']], [True])
        # volver a asignarlo
        main.set_estimation_supplier_payments(
            {'projectId': self.project_id, 'supplierKey': self.key, 'excludedTransactionIds': []}, user=ADMIN
        )
        self.assertEqual(main.compute_supplier_paid_amount(self.project_id, self.key), 5000.0)

    def test_payments_of_other_suppliers_cannot_be_excluded_here(self):
        other = self._acero_transaction(10, sap={'cardCode': 'P999', 'businessPartner': 'OTRO'})
        self.fake_db.transactions.docs.append(other)
        with self.assertRaises(HTTPException) as ctx:
            main.set_estimation_supplier_payments(
                {'projectId': self.project_id, 'supplierKey': self.key, 'excludedTransactionIds': [other['_id']]}, user=ADMIN
            )
        self.assertEqual(ctx.exception.status_code, 400)

    def _estimate(self, *budgets_pcts, advance=None):
        parts = [
            {'estimationBudgetId': budget['id'], 'captureMode': 'global', 'globalProgressPct': pct}
            for budget, pct in budgets_pcts
        ]
        batch = main.create_supplier_estimation(
            {'projectId': self.project_id, 'supplierKey': self.key, 'periodStart': '2026-02-01', 'periodEnd': '2026-02-07', 'parts': parts},
            user=SUPERADMIN,
        )
        main.submit_supplier_estimation(batch['id'], user=SUPERADMIN)
        return main.approve_supplier_estimation(batch['id'], {}, user=ADMIN)

    def test_everything_paid_to_the_supplier_is_discounted_from_what_is_released(self):
        # Pagado al proveedor: $5,000 sin asignar a ningún presupuesto. Avance 50 % de $10,000
        # con 10 % de retención = $4,500 netos, que ya están cubiertos por lo pagado.
        approved = self._estimate((self.depto1, 50))
        part = approved['parts'][0]
        self.assertEqual(part['priorPaidApplied'], 4500.0)
        self.assertEqual(part['priorPoolApplied'], 4500.0)
        self.assertEqual(approved['totalToPay'], 0.0)
        self.assertEqual(approved['authorizedAmount'], 0.0)
        # el sobrante ($500) se aplica a la siguiente estimación, aunque sea de otro presupuesto
        second = self._estimate((self.depto2, 50))
        self.assertEqual(second['priorPaidApplied'], 500.0)
        self.assertEqual(second['totalToPay'], 4000.0)
        # ya no queda nada por descontar
        third = self._estimate((self.depto1, 80))  # +30 % = $3,000 - 10 % = $2,700
        self.assertEqual(third['priorPaidApplied'], 0.0)
        self.assertEqual(third['totalToPay'], 2700.0)

    def test_the_pool_is_shared_across_the_parts_of_one_estimation(self):
        batch = main.create_supplier_estimation(
            {'projectId': self.project_id, 'supplierKey': self.key, 'periodStart': '2026-02-01', 'periodEnd': '2026-02-07',
             'parts': [{'estimationBudgetId': self.depto1['id'], 'captureMode': 'global', 'globalProgressPct': 50},
                       {'estimationBudgetId': self.depto2['id'], 'captureMode': 'global', 'globalProgressPct': 50}]},
            user=SUPERADMIN,
        )
        first, second = batch['parts']
        self.assertEqual((first['priorPaidApplied'], second['priorPaidApplied']), (4500.0, 500.0))
        self.assertEqual(batch['priorPaidApplied'], 5000.0)
        self.assertEqual(batch['totalToPay'], 4000.0)
        # editar el borrador no cuenta dos veces los mismos pagos
        edited = main.update_supplier_estimation(
            batch['id'],
            {'periodStart': '2026-02-01', 'periodEnd': '2026-02-08',
             'parts': [{'estimationBudgetId': self.depto1['id'], 'captureMode': 'global', 'globalProgressPct': 50},
                       {'estimationBudgetId': self.depto2['id'], 'captureMode': 'global', 'globalProgressPct': 50}]},
            user=SUPERADMIN,
        )
        self.assertEqual(edited['priorPaidApplied'], 5000.0)
        self.assertEqual(edited['totalToPay'], 4000.0)
        # y el listado vuelve a calcular lo mismo
        self.assertEqual(main.get_supplier_estimation(batch['id'], user=ADMIN)['totalToPay'], 4000.0)

    def test_payments_marked_as_advance_are_separated_from_the_pool(self):
        # $4,500 de lo pagado se asigna como anticipo del depto 1: no es «pago a cuenta» del proveedor
        main.set_estimation_budget_opening_balance(self.depto1['id'], {'advanceTransactionIds': [self.txs[0]['_id']]}, user=ADMIN)
        budget = main.get_estimation_budget(self.depto2['id'], user=ADMIN)
        self.assertTrue(budget['supplierUsesPriorPool'])
        self.assertEqual(budget['supplierPriorPoolRemaining'], 500.0)
        approved = self._estimate((self.depto2, 50))
        self.assertEqual(approved['priorPaidApplied'], 500.0)
        self.assertEqual(approved['totalToPay'], 4000.0)

    def test_the_approved_estimation_is_marked_paid_when_the_supplier_payments_cover_it(self):
        approved = self._estimate((self.depto1, 50))
        self.assertEqual(approved['authorizedAmount'], 0.0)
        self.assertEqual(main.get_supplier_estimation(approved['id'], user=ADMIN)['paymentStatus'], 'PAGADA')
        # si se desasigna el pago grande, ya no está cubierta
        main.set_estimation_supplier_payments(
            {'projectId': self.project_id, 'supplierKey': self.key, 'excludedTransactionIds': [self.txs[0]['_id']]}, user=ADMIN
        )
        listed = main.list_supplier_estimations(self.key, self.project_id, user=ADMIN)
        self.assertEqual(listed[0]['paymentStatus'], 'POR_PAGAR')

    def test_a_payment_for_an_authorized_amount_is_not_discounted_again(self):
        # solo $200 + $300 pagados (se desasigna el pago de $4,500)
        main.set_estimation_supplier_payments(
            {'projectId': self.project_id, 'supplierKey': self.key, 'excludedTransactionIds': [self.txs[0]['_id']]}, user=ADMIN
        )
        first = self._estimate((self.depto1, 50))  # $4,500 netos - $500 ya pagados = $4,000
        self.assertEqual((first['priorPaidApplied'], first['authorizedAmount']), (500.0, 4000.0))
        # llega el pago de $4,000 de esa estimación: no se descuenta de la siguiente
        self.fake_db.transactions.docs.append(self._acero_transaction(4000))
        second = self._estimate((self.depto2, 50))
        self.assertEqual((second['priorPaidApplied'], second['totalToPay']), (0.0, 4500.0))

    def test_assigning_a_payment_as_advance_brings_back_an_unassigned_payment(self):
        # el pago se desasignó del proveedor...
        main.set_estimation_supplier_payments(
            {'projectId': self.project_id, 'supplierKey': self.key, 'excludedTransactionIds': [self.txs[0]['_id']]}, user=ADMIN
        )
        self.assertEqual(main.compute_supplier_paid_amount(self.project_id, self.key), 500.0)
        # ...y se puede asignar como anticipo a cualquier presupuesto: vuelve a contar
        saved = main.set_estimation_budget_opening_balance(
            self.depto2['id'], {'advanceTransactionIds': [self.txs[0]['_id']]}, user=ADMIN
        )
        self.assertEqual(saved['openingAdvanceAmount'], 4500.0)
        self.assertEqual(main.compute_supplier_paid_amount(self.project_id, self.key), 5000.0)
        links = self.fake_db.estimationPaymentLinks.find({'transactionId': self.txs[0]['_id']})
        self.assertEqual([link['estimationBudgetId'] for link in links], [self.depto2['id']])

    def test_budget_level_exclusions_do_not_shrink_the_pool_of_a_multi_budget_supplier(self):
        # restos del modo anterior: se «desasignaron» todos los pagos de un presupuesto
        for budget_id in (self.depto1['id'], self.depto2['id']):
            self.fake_db.estimationBudgets.update_one(
                {'_id': ObjectId(budget_id)}, {'$set': {'excludedTransactionIds': [tx['_id'] for tx in self.txs]}}
            )
        self.assertEqual(main.compute_supplier_paid_amount(self.project_id, self.key), 5000.0)

    def test_new_sap_payments_count_automatically_for_the_supplier(self):
        main.set_estimation_supplier_payments(
            {'projectId': self.project_id, 'supplierKey': self.key, 'excludedTransactionIds': [self.txs[0]['_id']]}, user=ADMIN
        )
        self.assertEqual(main.compute_supplier_paid_amount(self.project_id, self.key), 500.0)
        # llega un pago nuevo de SAP: no hay que asignarlo a nada, cuenta solo
        new_tx = self._acero_transaction(1000)
        self.fake_db.transactions.docs.append(new_tx)
        self.assertEqual(main.compute_supplier_paid_amount(self.project_id, self.key), 1500.0)
        self.assertEqual(main.get_estimation_budget(self.depto1['id'], user=ADMIN)['supplierPaidAmount'], 1500.0)
        listing = main.list_estimation_supplier_payments(self.key, self.project_id, user=ADMIN)
        self.assertEqual([r['isExcluded'] for r in listing['items'] if r['id'] == new_tx['_id']], [False])
        # y se descuenta de la siguiente estimación sin ninguna asignación
        batch = main.create_supplier_estimation(
            {'projectId': self.project_id, 'supplierKey': self.key, 'periodStart': '2026-02-01', 'periodEnd': '2026-02-07',
             'parts': [{'estimationBudgetId': self.depto1['id'], 'captureMode': 'global', 'globalProgressPct': 50}]},
            user=SUPERADMIN,
        )
        self.assertEqual(batch['priorPaidApplied'], 1500.0)
        self.assertEqual(batch['totalToPay'], 3000.0)
        # y tras «Incluir todos» los pagos futuros también cuentan
        main.set_estimation_supplier_payments(
            {'projectId': self.project_id, 'supplierKey': self.key, 'excludedTransactionIds': []}, user=ADMIN
        )
        self.fake_db.transactions.docs.append(self._acero_transaction(250))
        self.assertEqual(main.compute_supplier_paid_amount(self.project_id, self.key), 6250.0)


class PlannedAdvanceInMultiBudgetSupplierTests(phase1.EstimationsPhase1Tests):
    """El anticipo capturado en un presupuesto solo se amortiza si de verdad se entregó."""

    def setUp(self):
        super().setUp()
        self.txs = [self._acero_transaction(3000), self._acero_transaction(2000)]
        self.fake_db = self._fake_db(transactions=self.txs)
        self.patches = [
            patch.object(main, 'db', self.fake_db),
            patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q),
            patch.object(main, 'build_transactions_query', return_value={}),
        ]
        for p in self.patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self.patches])
        base = dict(advanceAmortizationEnabled=True, advanceAmount=0, retentionPct=10)
        self.a = self._create_budget(self.fake_db, name='A', **base)
        self.b = self._create_budget(self.fake_db, name='B', **base)
        self.c = self._create_budget(self.fake_db, name='C', **{**base, 'advanceAmount': 2000})  # previsto, sin pagar
        self.d = self._create_budget(self.fake_db, name='D', **base)
        self.key = self.a['supplierKey']
        main.set_estimation_budget_opening_balance(self.a['id'], {'advanceTransactionIds': [self.txs[0]['_id']]}, user=ADMIN)
        main.set_estimation_budget_opening_balance(self.b['id'], {'advanceTransactionIds': [self.txs[1]['_id']]}, user=ADMIN)

    def _estimate_all(self, pct=50):
        parts = [{'estimationBudgetId': x['id'], 'captureMode': 'global', 'globalProgressPct': pct} for x in (self.a, self.b, self.c, self.d)]
        return main.create_supplier_estimation(
            {'projectId': self.project_id, 'supplierKey': self.key, 'periodStart': '2026-02-01', 'periodEnd': '2026-02-07', 'parts': parts},
            user=SUPERADMIN,
        )

    def test_only_delivered_advances_are_amortized_and_nothing_is_deducted_twice(self):
        batch = self._estimate_all()
        amort = {p['budgetName']: p['advanceAmortizationAmount'] for p in batch['parts']}
        # A entregó $3,000 (30 %), B $2,000 (20 %); C tiene $2,000 previstos pero no entregados; D no tiene
        self.assertEqual(amort, {'A': 1500.0, 'B': 1000.0, 'C': 0.0, 'D': 0.0})
        self.assertEqual(batch['priorPaidApplied'], 0.0)  # los $5,000 pagados son los anticipos
        self.assertEqual(batch['totalToPay'], 3000.0 + 3500.0 + 4500.0 + 4500.0)

    def test_budget_exposes_the_delivered_advance_and_what_is_left_to_amortize(self):
        c = main.get_estimation_budget(self.c['id'], user=ADMIN)
        a = main.get_estimation_budget(self.a['id'], user=ADMIN)
        self.assertEqual((c['advanceAmount'], c['advanceDeliveredAmount'], c['remainingAdvanceBalance']), (2000.0, 0.0, 0.0))
        self.assertEqual((a['advanceDeliveredAmount'], a['remainingAdvanceBalance']), (3000.0, 3000.0))

    def test_an_advance_given_in_the_estimation_is_amortized_afterwards(self):
        parts = [{'estimationBudgetId': self.c['id'], 'advanceAmount': 2000, 'noProgress': True}]
        first = main.create_supplier_estimation(
            {'projectId': self.project_id, 'supplierKey': self.key, 'periodStart': '2026-02-01', 'periodEnd': '2026-02-07', 'parts': parts},
            user=SUPERADMIN,
        )
        main.submit_supplier_estimation(first['id'], user=SUPERADMIN)
        main.approve_supplier_estimation(first['id'], {}, user=ADMIN)
        c = main.get_estimation_budget(self.c['id'], user=ADMIN)
        self.assertEqual((c['advanceDeliveredAmount'], c['remainingAdvanceBalance']), (2000.0, 2000.0))
        nxt = main.create_supplier_estimation(
            {'projectId': self.project_id, 'supplierKey': self.key, 'periodStart': '2026-03-01', 'periodEnd': '2026-03-07',
             'parts': [{'estimationBudgetId': self.c['id'], 'captureMode': 'global', 'globalProgressPct': 50}]},
            user=SUPERADMIN,
        )
        self.assertEqual(nxt['parts'][0]['advanceAmortizationAmount'], 1000.0)  # 20 % de $5,000


    def test_marking_planned_advance_as_delivered_amortizes_it(self):
        saved = main.update_estimation_budget(self.c['id'], {'advanceDelivered': True}, user=ADMIN)
        self.assertEqual(saved['advanceDeliveredAmount'], 2000.0)
        batch = self._estimate_all()
        amort = {p['budgetName']: p['advanceAmortizationAmount'] for p in batch['parts']}
        self.assertEqual(amort['C'], 1000.0)

    def test_unmarking_delivered_advance(self):
        main.update_estimation_budget(self.c['id'], {'advanceDelivered': True}, user=ADMIN)
        saved = main.update_estimation_budget(self.c['id'], {'advanceDelivered': False}, user=ADMIN)
        self.assertEqual(saved['advanceDeliveredAmount'], 0.0)
