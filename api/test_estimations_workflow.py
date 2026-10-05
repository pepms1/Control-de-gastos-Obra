import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402
import test_estimations_phase1 as phase1  # noqa: E402

SUPERADMIN = phase1.SUPERADMIN


CAPTURIST = {'role': 'VIEWER', 'username': 'mps', 'canCaptureEstimations': True}
PLAIN_VIEWER = {'role': 'VIEWER', 'username': 'viewer'}
ADMIN = {'role': 'ADMIN', 'username': 'boss'}


class EstimationsWorkflowTests(phase1.EstimationsPhase1Tests):
    """Reuses the fixtures of the phase-1 suite (fake db, budget payload)."""

    def setUp(self):
        super().setUp()
        self.fake_db = self._fake_db()
        self.budget = self._create_budget(self.fake_db, advanceAmortizationEnabled=False, advanceAmount=0, retentionPct=10)
        self.tuberia_id = self.budget['lineItems'][0]['id']  # 100 ml x 60
        self.conexiones_id = self.budget['lineItems'][1]['id']  # 20 pza x 200
        # allow the capturist (VIEWER) to reach this project
        self.capturist = dict(CAPTURIST, allowedProjectIds=[self.project_id])

    def _capture(self, payload, user=None):
        body = {'periodStart': '2026-02-01', 'periodEnd': '2026-02-07', **payload}
        with patch.object(main, 'db', self.fake_db):
            return main.create_estimation(self.budget['id'], body, user=user or self.capturist)

    def _call(self, fn, *args, **kwargs):
        with patch.object(main, 'db', self.fake_db):
            return fn(*args, **kwargs)

    # ---- permissions ----

    def test_capture_dependency_allows_flag_admins_and_rejects_plain_viewer(self):
        self.assertIs(main.require_estimation_capture(user=CAPTURIST), CAPTURIST)
        self.assertIs(main.require_estimation_capture(user=ADMIN), ADMIN)
        with self.assertRaises(HTTPException) as ctx:
            main.require_estimation_capture(user=PLAIN_VIEWER)
        self.assertEqual(ctx.exception.status_code, 403)

    def test_capturist_cannot_see_payment_fields_but_admin_can(self):
        with patch.object(main, 'db', self.fake_db), patch.object(
            main, 'compute_estimation_budget_paid_amount', return_value=123.0
        ):
            as_capturist = main.get_estimation_budget(self.budget['id'], user=self.capturist)
            as_admin = main.get_estimation_budget(self.budget['id'], user=ADMIN)
        self.assertNotIn('paidAmount', as_capturist)
        self.assertEqual(as_admin['paidAmount'], 123.0)

    def test_capturist_cannot_approve_return_or_mark_paid(self):
        # these endpoints are guarded by require_admin_or_superadmin
        with self.assertRaises(HTTPException) as ctx:
            main.require_admin_or_superadmin(user=CAPTURIST)
        self.assertEqual(ctx.exception.status_code, 403)

    def test_capturist_without_project_access_is_denied(self):
        outsider = dict(CAPTURIST, allowedProjectIds=[str(ObjectId())])
        with self.assertRaises(HTTPException) as ctx:
            self._capture({'captureMode': 'global', 'globalProgressPct': 10}, user=outsider)
        self.assertEqual(ctx.exception.status_code, 403)

    # ---- percentage capture ----

    def test_global_progress_pct_spreads_over_all_conceptos(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 25})
        self.assertEqual(created['workflowStatus'], 'BORRADOR')
        lines = {li['conceptoId']: li for li in created['lineItems']}
        self.assertEqual(lines[self.tuberia_id]['periodQuantity'], 25)
        self.assertEqual(lines[self.conexiones_id]['periodQuantity'], 5)
        self.assertEqual(lines[self.tuberia_id]['progressPct'], 25)
        # 25*60 + 5*200 = 2500
        self.assertEqual(created['periodSubtotal'], 2500)
        self.assertEqual(created['retentionAmount'], 250)
        self.assertEqual(created['totalToPay'], 2250)
        self.assertEqual(created['cumulativeProgressPct'], 25)

    def test_concept_progress_pct_is_cumulative_and_omitted_conceptos_do_not_advance(self):
        first = self._capture(
            {'captureMode': 'concept', 'lineItems': [{'conceptoId': self.tuberia_id, 'progressPct': 40}]}
        )
        lines = {li['conceptoId']: li for li in first['lineItems']}
        self.assertEqual(lines[self.tuberia_id]['periodQuantity'], 40)
        self.assertEqual(lines[self.conexiones_id]['periodQuantity'], 0)
        self._call(main.submit_estimation, self.budget['id'], first['id'], user=self.capturist)
        self._call(main.approve_estimation, self.budget['id'], first['id'], {}, user=ADMIN)

        second = self._capture(
            {'captureMode': 'concept', 'lineItems': [{'conceptoId': self.tuberia_id, 'progressPct': 70}]}
        )
        lines = {li['conceptoId']: li for li in second['lineItems']}
        self.assertEqual(lines[self.tuberia_id]['previousProgressPct'], 40)
        self.assertEqual(lines[self.tuberia_id]['periodQuantity'], 30)
        self.assertEqual(lines[self.tuberia_id]['progressPct'], 70)

    def test_concept_progress_pct_lower_than_previous_is_rejected(self):
        first = self._capture(
            {'captureMode': 'concept', 'lineItems': [{'conceptoId': self.tuberia_id, 'progressPct': 40}]}
        )
        self._call(main.submit_estimation, self.budget['id'], first['id'], user=self.capturist)
        self._call(main.approve_estimation, self.budget['id'], first['id'], {}, user=ADMIN)
        with self.assertRaises(HTTPException) as ctx:
            self._capture({'captureMode': 'concept', 'lineItems': [{'conceptoId': self.tuberia_id, 'progressPct': 30}]})
        self.assertEqual(ctx.exception.status_code, 400)

    def test_progress_pct_out_of_range_is_rejected(self):
        for bad in (-1, 101, 'abc'):
            with self.assertRaises(HTTPException) as ctx:
                self._capture({'captureMode': 'global', 'globalProgressPct': bad})
            self.assertEqual(ctx.exception.status_code, 400)

    def test_global_pct_does_not_lower_a_concepto_that_is_already_ahead(self):
        first = self._capture(
            {'captureMode': 'concept', 'lineItems': [{'conceptoId': self.tuberia_id, 'progressPct': 80}]}
        )
        self._call(main.submit_estimation, self.budget['id'], first['id'], user=self.capturist)
        self._call(main.approve_estimation, self.budget['id'], first['id'], {}, user=ADMIN)
        second = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        lines = {li['conceptoId']: li for li in second['lineItems']}
        self.assertEqual(lines[self.tuberia_id]['periodQuantity'], 0)
        self.assertEqual(lines[self.conexiones_id]['periodQuantity'], 10)

    # ---- workflow ----

    def test_full_flow_draft_submit_approve_pay(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        self.assertEqual(created['workflowStatus'], 'BORRADOR')

        submitted = self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        self.assertEqual(submitted['workflowStatus'], 'ENVIADA')
        self.assertEqual(submitted['submittedBy'], 'mps')

        approved = self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        self.assertEqual(approved['workflowStatus'], 'APROBADA')
        self.assertEqual(approved['paymentStatus'], 'POR_PAGAR')
        self.assertEqual(approved['authorizedAmount'], approved['totalToPay'])
        self.assertEqual(approved['approvedBy'], 'boss')

        paid = self._call(main.mark_estimation_paid, self.budget['id'], created['id'], {'note': 'SPEI 123'}, user=ADMIN)
        self.assertEqual(paid['paymentStatus'], 'PAGADA')
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.mark_estimation_paid, self.budget['id'], created['id'], {}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_approve_with_different_amount_requires_a_note(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.approve_estimation, self.budget['id'], created['id'], {'authorizedAmount': 1000}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 400)

        higher = created['totalToPay'] + 500
        approved = self._call(
            main.approve_estimation,
            self.budget['id'],
            created['id'],
            {'authorizedAmount': higher, 'authorizationNote': 'Trabajos extra aprobados en obra'},
            user=ADMIN,
        )
        self.assertEqual(approved['authorizedAmount'], higher)
        self.assertEqual(approved['authorizedDifference'], 500)

    def test_negative_authorized_amount_is_rejected(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        with self.assertRaises(HTTPException) as ctx:
            self._call(
                main.approve_estimation, self.budget['id'], created['id'],
                {'authorizedAmount': -5, 'authorizationNote': 'x'}, user=ADMIN,
            )
        self.assertEqual(ctx.exception.status_code, 400)

    def test_cannot_approve_a_draft_or_an_already_approved_estimation(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 409)
        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_submitted_and_approved_estimations_are_locked(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        for fn, args in (
            (main.update_estimation, ({'notes': 'x'},)),
            (main.delete_estimation, ()),
        ):
            with self.assertRaises(HTTPException) as ctx:
                self._call(fn, self.budget['id'], created['id'], *args, user=self.capturist)
            self.assertEqual(ctx.exception.status_code, 409)

    def test_return_requires_reason_and_reopens_draft_for_editing(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.return_estimation_to_draft, self.budget['id'], created['id'], {}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 400)

        returned = self._call(
            main.return_estimation_to_draft, self.budget['id'], created['id'], {'reason': 'Falta soporte'}, user=ADMIN
        )
        self.assertEqual(returned['workflowStatus'], 'BORRADOR')
        self.assertEqual(returned['returnReason'], 'Falta soporte')

        edited = self._call(
            main.update_estimation, self.budget['id'], created['id'],
            {'captureMode': 'global', 'globalProgressPct': 30}, user=self.capturist,
        )
        self.assertEqual(edited['cumulativeProgressPct'], 30)

    def test_draft_can_be_deleted_by_capturist(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        self._call(main.delete_estimation, self.budget['id'], created['id'], user=self.capturist)
        with patch.object(main, 'db', self.fake_db):
            self.assertEqual(main.list_estimations(self.budget['id'], user=self.capturist), [])

    def test_only_one_open_estimation_per_budget(self):
        self._capture({'captureMode': 'global', 'globalProgressPct': 10})
        with self.assertRaises(HTTPException) as ctx:
            self._capture({'captureMode': 'global', 'globalProgressPct': 20})
        self.assertEqual(ctx.exception.status_code, 409)

    def test_cannot_submit_an_estimation_without_progress(self):
        with self.assertRaises(HTTPException) as ctx:
            self._capture({'captureMode': 'global', 'globalProgressPct': 0, 'submit': True})
        self.assertEqual(ctx.exception.status_code, 400)
        draft = self._capture({'captureMode': 'global', 'globalProgressPct': 0})
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.submit_estimation, self.budget['id'], draft['id'], user=self.capturist)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_create_with_submit_flag_goes_straight_to_review(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 20, 'submit': True})
        self.assertEqual(created['workflowStatus'], 'ENVIADA')

    # ---- avance de obra para los KPI de la lista ----

    def test_approved_progress_counts_only_approved_estimations(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 40})
        draft_view = self._call(main.get_estimation_budget, self.budget['id'], user=ADMIN)
        self.assertEqual(draft_view['approvedProgressAmount'], 0)
        self.assertEqual(draft_view['approvedProgressPct'], 0)

        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        approved_view = self._call(main.get_estimation_budget, self.budget['id'], user=ADMIN)
        self.assertEqual(approved_view['approvedProgressAmount'], 4000)  # 40% de 10,000
        self.assertEqual(approved_view['approvedProgressPct'], 40)

        second = self._capture({'captureMode': 'global', 'globalProgressPct': 70})
        still_view = self._call(main.get_estimation_budget, self.budget['id'], user=ADMIN)
        self.assertEqual(still_view['approvedProgressPct'], 40)  # el borrador de 70% aun no cuenta
        self.assertEqual(second['cumulativeProgressPct'], 70)

    # ---- legacy estimations (created before the workflow existed) ----

    def test_legacy_estimation_is_admin_only(self):
        legacy_id = ObjectId()
        self.fake_db.estimations.docs.append(
            {
                '_id': legacy_id,
                'estimationBudgetId': self.budget['id'],
                'projectId': self.project_id,
                'folio': 1,
                'lineItems': [],
                'isDeleted': False,
                'status': 'Registrada',
            }
        )
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.update_estimation, self.budget['id'], str(legacy_id), {'notes': 'x'}, user=self.capturist)
        self.assertEqual(ctx.exception.status_code, 403)
        updated = self._call(main.update_estimation, self.budget['id'], str(legacy_id), {'notes': 'x'}, user=SUPERADMIN)
        self.assertEqual(updated['workflowStatus'], 'REGISTRADA')

    # ---- inbox ----

    def test_queue_lists_pending_review_and_payable_estimations(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50})
        with patch.object(main, 'db', self.fake_db):
            drafts = main.list_estimations_queue(projectId=self.project_id, user=self.capturist)
        self.assertEqual([row['id'] for row in drafts['items']], [created['id']])
        self.assertEqual(drafts['items'][0]['supplierName'], 'Acero SA')

        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        with patch.object(main, 'db', self.fake_db):
            open_rows = main.list_estimations_queue(projectId=self.project_id, user=ADMIN)
            payable = main.list_estimations_queue(
                projectId=self.project_id, status='APROBADA', paymentStatus='POR_PAGAR', user=ADMIN
            )
        self.assertEqual(open_rows['items'], [])
        self.assertEqual([row['id'] for row in payable['items']], [created['id']])

    def test_queue_rejects_unknown_status(self):
        with patch.object(main, 'db', self.fake_db), self.assertRaises(HTTPException) as ctx:
            main.list_estimations_queue(projectId=self.project_id, status='NOPE', user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 400)

    # ---- user flag ----

    def test_user_payload_exposes_capture_flag(self):
        payload = main.build_current_user_payload('mps', 'VIEWER', 'MPS', user_doc={'canCaptureEstimations': True})
        self.assertTrue(payload['canCaptureEstimations'])
        self.assertFalse(main.build_current_user_payload('x', 'VIEWER', 'X', user_doc={})['canCaptureEstimations'])


# The inherited phase-1 tests are already run by their own module.
for _name in [n for n in dir(phase1.EstimationsPhase1Tests) if n.startswith('test_')]:
    setattr(EstimationsWorkflowTests, _name, None)


if __name__ == '__main__':
    unittest.main()
