from types import SimpleNamespace
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

    # ---- monto solicitado por el contratista ----

    def _requested_flow(self, requested, pct=50, **extra):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': pct, 'requestedAmount': requested, **extra})
        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        return created

    def test_requested_amount_is_captured_with_the_estimation_and_optional(self):
        with_request = self._capture({'captureMode': 'global', 'globalProgressPct': 50, 'requestedAmount': 4000})
        self.assertEqual(with_request['requestedAmount'], 4000)
        edited = self._call(
            main.update_estimation, self.budget['id'], with_request['id'], {'requestedAmount': '3,500.50'}, user=self.capturist,
        )
        self.assertEqual(edited['requestedAmount'], 3500.50)
        cleared = self._call(main.update_estimation, self.budget['id'], with_request['id'], {'requestedAmount': ''}, user=self.capturist)
        self.assertIsNone(cleared['requestedAmount'])

    def test_requested_amount_is_validated_and_locked_after_submitting(self):
        with self.assertRaises(HTTPException) as ctx:
            self._capture({'captureMode': 'global', 'globalProgressPct': 50, 'requestedAmount': -1})
        self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException) as ctx:
            self._capture({'captureMode': 'global', 'globalProgressPct': 50, 'requestedAmount': 'abc'})
        self.assertEqual(ctx.exception.status_code, 400)
        created = self._requested_flow(4000)
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.update_estimation, self.budget['id'], created['id'], {'requestedAmount': 9999}, user=self.capturist)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_approval_defaults_to_the_requested_amount_not_the_calculated(self):
        created = self._requested_flow(3000)  # calculado: 50% de 10,000 - 10% retención = 4,500
        self.assertEqual(created['totalToPay'], 4500)
        approved = self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        self.assertEqual(approved['authorizedAmount'], 3000)
        self.assertEqual(approved['requestedAmount'], 3000)
        self.assertEqual(approved['authorizedVsRequested'], 0)
        self.assertEqual(approved['authorizedDifference'], -1500)  # contra lo calculado

    def test_reviewer_can_authorize_less_than_requested_with_a_reason(self):
        created = self._requested_flow(4500)
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.approve_estimation, self.budget['id'], created['id'], {'authorizedAmount': 4000}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 400)
        approved = self._call(
            main.approve_estimation, self.budget['id'], created['id'],
            {'authorizedAmount': 4000, 'authorizationNote': 'Se retienen 500 por trabajos pendientes'}, user=ADMIN,
        )
        self.assertEqual(approved['authorizedAmount'], 4000)
        self.assertEqual(approved['authorizedVsRequested'], -500)

    def test_reviewer_cannot_authorize_more_than_requested(self):
        created = self._requested_flow(3000)
        with self.assertRaises(HTTPException) as ctx:
            self._call(
                main.approve_estimation, self.budget['id'], created['id'],
                {'authorizedAmount': 3500, 'authorizationNote': 'x'}, user=ADMIN,
            )
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn('exceder', ctx.exception.detail)

    def test_contractor_asking_more_than_the_progress_needs_a_reason_to_pay_it(self):
        created = self._requested_flow(6000)  # calculado 4,500: pide 1,500 de más
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)  # aceptar lo pedido
        self.assertEqual(ctx.exception.status_code, 400)
        paid_as_calculated = self._call(
            main.approve_estimation, self.budget['id'], created['id'],
            {'authorizedAmount': 4500, 'authorizationNote': 'Se paga solo el avance'}, user=ADMIN,
        )
        self.assertEqual(paid_as_calculated['authorizedAmount'], 4500)
        self.assertEqual(paid_as_calculated['authorizedVsRequested'], -1500)

    def test_paying_above_the_progress_when_requested_is_allowed_with_a_reason(self):
        created = self._requested_flow(6000)
        approved = self._call(
            main.approve_estimation, self.budget['id'], created['id'],
            {'authorizationNote': 'Anticipo de materiales autorizado'}, user=ADMIN,
        )
        self.assertEqual(approved['authorizedAmount'], 6000)
        self.assertEqual(approved['authorizedDifference'], 1500)

    def test_without_a_requested_amount_authorization_works_as_before(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 50, 'submit': True})
        self.assertIsNone(created['requestedAmount'])
        approved = self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        self.assertEqual(approved['authorizedAmount'], approved['totalToPay'])
        self.assertIsNone(approved['authorizedVsRequested'])

    # ---- obras asignadas a cada admin para autorizar ----

    def _admin_for(self, project_ids):
        return {'role': 'ADMIN', 'username': 'boss2', 'estimationApprovalProjectIds': project_ids}

    def test_approval_scope_rules(self):
        other_project = str(ObjectId())
        self.assertTrue(main.can_approve_estimations({'role': 'SUPERADMIN'}, self.project_id))
        self.assertTrue(main.can_approve_estimations({'role': 'ADMIN'}, self.project_id))  # sin lista: todas, como antes
        self.assertTrue(main.can_approve_estimations(self._admin_for([self.project_id]), self.project_id))
        self.assertFalse(main.can_approve_estimations(self._admin_for([other_project]), self.project_id))
        self.assertFalse(main.can_approve_estimations(self._admin_for([]), self.project_id))  # lista vacía: ninguna
        self.assertFalse(main.can_approve_estimations(CAPTURIST, self.project_id))
        self.assertFalse(main.can_approve_estimations(None, self.project_id))

    def test_admin_without_the_project_cannot_approve_or_return(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 20, 'submit': True})
        outsider = self._admin_for([str(ObjectId())])
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=outsider)
        self.assertEqual(ctx.exception.status_code, 403)
        with self.assertRaises(HTTPException) as ctx:
            self._call(main.return_estimation_to_draft, self.budget['id'], created['id'], {'reason': 'x'}, user=outsider)
        self.assertEqual(ctx.exception.status_code, 403)

        assigned = self._admin_for([self.project_id])
        approved = self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=assigned)
        self.assertEqual(approved['workflowStatus'], 'APROBADA')

    def test_superadmin_and_unrestricted_admin_still_approve_everything(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 20, 'submit': True})
        approved = self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=SUPERADMIN)
        self.assertEqual(approved['workflowStatus'], 'APROBADA')
        second = self._capture({'captureMode': 'global', 'globalProgressPct': 30, 'submit': True})
        self.assertEqual(self._call(main.approve_estimation, self.budget['id'], second['id'], {}, user=ADMIN)['workflowStatus'], 'APROBADA')

    def test_marking_paid_is_not_limited_to_the_assigned_projects(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 20, 'submit': True})
        self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)
        outsider = self._admin_for([str(ObjectId())])
        paid = self._call(main.mark_estimation_paid, self.budget['id'], created['id'], {}, user=outsider)
        self.assertEqual(paid['paymentStatus'], 'PAGADA')

    def test_pending_summary_only_counts_the_projects_the_admin_approves(self):
        self._capture({'captureMode': 'global', 'globalProgressPct': 20, 'submit': True})
        with patch.object(main, 'db', self.fake_db):
            self.assertEqual(main.estimations_pending_summary(user=ADMIN)['pendingReview'], 1)
            self.assertEqual(main.estimations_pending_summary(user=self._admin_for([self.project_id]))['pendingReview'], 1)
            self.assertEqual(main.estimations_pending_summary(user=self._admin_for([str(ObjectId())]))['pendingReview'], 0)

    def test_user_payload_exposes_the_approval_projects(self):
        none_payload = main.build_current_user_payload('a', 'ADMIN', 'A', user_doc={})
        self.assertIsNone(none_payload['estimationApprovalProjectIds'])
        listed = main.build_current_user_payload('a', 'ADMIN', 'A', user_doc={'estimationApprovalProjectIds': [self.project_id, 'basura']})
        self.assertEqual(listed['estimationApprovalProjectIds'], [self.project_id])
        empty = main.build_current_user_payload('a', 'ADMIN', 'A', user_doc={'estimationApprovalProjectIds': []})
        self.assertEqual(empty['estimationApprovalProjectIds'], [])

    # ---- aviso para quien autoriza ----

    def test_pending_summary_counts_only_submitted_estimations(self):
        with patch.object(main, 'db', self.fake_db):
            self.assertEqual(main.estimations_pending_summary(user=ADMIN)['pendingReview'], 0)

        draft = self._capture({'captureMode': 'global', 'globalProgressPct': 20})
        with patch.object(main, 'db', self.fake_db):
            self.assertEqual(main.estimations_pending_summary(user=ADMIN)['pendingReview'], 0)  # un borrador no cuenta

        self._call(main.submit_estimation, self.budget['id'], draft['id'], user=self.capturist)
        with patch.object(main, 'db', self.fake_db):
            summary = main.estimations_pending_summary(user=ADMIN)
        self.assertEqual(summary['pendingReview'], 1)
        self.assertEqual(summary['byProject'], {self.project_id: 1})
        self.assertEqual(summary['submittedBy'], ['mps'])
        self.assertTrue(summary['oldestSubmittedAt'])

        self._call(main.approve_estimation, self.budget['id'], draft['id'], {}, user=ADMIN)
        with patch.object(main, 'db', self.fake_db):
            self.assertEqual(main.estimations_pending_summary(user=ADMIN)['pendingReview'], 0)  # aprobada: deja de avisar

    def test_returned_estimation_stops_flashing_for_the_reviewer(self):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': 20, 'submit': True})
        with patch.object(main, 'db', self.fake_db):
            self.assertEqual(main.estimations_pending_summary(user=ADMIN)['pendingReview'], 1)
        self._call(main.return_estimation_to_draft, self.budget['id'], created['id'], {'reason': 'Falta soporte'}, user=ADMIN)
        with patch.object(main, 'db', self.fake_db):
            self.assertEqual(main.estimations_pending_summary(user=ADMIN)['pendingReview'], 0)

    def test_pending_summary_is_for_admins_only(self):
        with self.assertRaises(HTTPException) as ctx:
            main.require_admin_or_superadmin(user=CAPTURIST)
        self.assertEqual(ctx.exception.status_code, 403)

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


class EstimationAutoPaidTests(EstimationsWorkflowTests):
    """La estimacion aprobada se marca PAGADA sola cuando los pagos asignados la cubren."""

    def _approved(self, pct=50):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': pct})
        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        return self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)

    def _list_with_paid(self, paid):
        with patch.object(main, 'db', self.fake_db), patch.object(
            main, 'compute_estimation_budget_paid_amount', return_value=paid
        ):
            return main.list_estimations(self.budget['id'], user=ADMIN)

    def test_stays_por_pagar_until_payments_cover_the_authorized_amount(self):
        approved = self._approved()  # 50% de 10,000 = 5,000 - 10% retención = 4,500
        self.assertEqual(approved['authorizedAmount'], 4500.0)
        self.assertEqual(self._list_with_paid(0)[0]['paymentStatus'], 'POR_PAGAR')
        self.assertEqual(self._list_with_paid(4000)[0]['paymentStatus'], 'POR_PAGAR')

    def test_marks_paid_automatically_when_the_payment_arrives(self):
        self._approved()
        rows = self._list_with_paid(4500)
        self.assertEqual(rows[0]['paymentStatus'], 'PAGADA')
        self.assertEqual(rows[0]['paidMarkedBy'], 'sistema')

    def test_reverts_if_the_payment_is_unassigned_but_keeps_manual_marks(self):
        self._approved()
        self.assertEqual(self._list_with_paid(4500)[0]['paymentStatus'], 'PAGADA')
        self.assertEqual(self._list_with_paid(0)[0]['paymentStatus'], 'POR_PAGAR')
        created = self._list_with_paid(0)[0]
        self._call(main.mark_estimation_paid, self.budget['id'], created['id'], {}, user=ADMIN)
        self.assertEqual(self._list_with_paid(0)[0]['paymentStatus'], 'PAGADA')

    def test_second_estimation_needs_the_cumulative_amount(self):
        self._approved(pct=50)
        second = self._capture({'captureMode': 'global', 'globalProgressPct': 100})
        self._call(main.submit_estimation, self.budget['id'], second['id'], user=self.capturist)
        self._call(main.approve_estimation, self.budget['id'], second['id'], {}, user=ADMIN)
        rows = self._list_with_paid(4500)
        self.assertEqual([r['paymentStatus'] for r in rows], ['PAGADA', 'POR_PAGAR'])
        rows = self._list_with_paid(9000)
        self.assertEqual([r['paymentStatus'] for r in rows], ['PAGADA', 'PAGADA'])


class EstimationFolioTests(EstimationsWorkflowTests):
    def _closed(self, pct):
        created = self._capture({'captureMode': 'global', 'globalProgressPct': pct})
        self._call(main.submit_estimation, self.budget['id'], created['id'], user=self.capturist)
        return self._call(main.approve_estimation, self.budget['id'], created['id'], {}, user=ADMIN)

    def _set(self, estimation, folio):
        return self._call(main.set_estimation_folio, self.budget['id'], estimation['id'], {'folio': folio}, user=ADMIN)

    def test_manual_folio_and_next_ones_continue_from_it(self):
        first = self._closed(30)
        self.assertEqual(first['folio'], 1)
        self.assertEqual(self._set(first, 32)['folio'], 32)
        second = self._closed(60)
        self.assertEqual(second['folio'], 33)

    def test_folio_must_be_unique_and_keep_order(self):
        first = self._closed(30)
        second = self._closed(60)
        for bad in (2, 3):
            with self.assertRaises(HTTPException) as ctx:
                self._set(first, bad)
            self.assertEqual(ctx.exception.status_code, 409)
        with self.assertRaises(HTTPException) as ctx:
            self._set(second, 1)
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(self._set(second, 11)['folio'], 11)
        self.assertEqual(self._set(first, 10)['folio'], 10)

    def test_folio_rejects_garbage_and_non_admins(self):
        first = self._closed(30)
        for bad in ('abc', 0, -3, None):
            with self.assertRaises(HTTPException) as ctx:
                self._set(first, bad)
            self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException) as ctx:
            main.require_admin_or_superadmin(user=CAPTURIST)
        self.assertEqual(ctx.exception.status_code, 403)


class CapturistBudgetsAccessTests(EstimationsWorkflowTests):
    """MPS (VIEWER con bandera de captura) puede ver, crear y editar presupuestos, sin ver pagos."""

    def _dep(self, fn):
        import inspect
        return inspect.signature(fn).parameters['user'].default.dependency

    def test_budget_endpoints_open_to_capture_flag_users(self):
        for fn in (main.list_estimation_budgets, main.create_estimation_budget, main.update_estimation_budget,
                   main.import_estimation_conceptos):
            self.assertIs(self._dep(fn), main.require_estimation_capture, fn.__name__)

    def test_payments_delete_extras_and_opening_stay_admin_only(self):
        for fn in (main.delete_estimation_budget, main.add_estimation_budget_extras,
                   main.set_estimation_budget_opening_balance, main.replace_estimation_budget_transaction_links):
            self.assertIs(self._dep(fn), main.require_admin_or_superadmin, fn.__name__)

    def test_capturist_creates_budget_and_does_not_see_payments(self):
        payload = self._base_budget_payload()
        with patch.object(main, 'db', self.fake_db):
            created = main.create_estimation_budget(payload, None, user=self.capturist)
            listed = main.list_estimation_budgets(projectId=self.project_id, user=self.capturist)
        self.assertTrue(created['id'])
        self.assertNotIn('paidAmount', created)
        self.assertTrue(listed)
        self.assertTrue(all('paidAmount' not in row for row in listed))


class BudgetAuthorizationTests(EstimationsWorkflowTests):
    """Presupuestos capturados por MPS necesitan autorización antes de estimar."""

    def _create_as(self, user, **overrides):
        with patch.object(main, 'db', self.fake_db):
            return main.create_estimation_budget(
                self._base_budget_payload(**overrides), SimpleNamespace(headers={}, query_params={}), user=user
            )

    def _estimate(self, budget_id, user=None):
        with patch.object(main, 'db', self.fake_db):
            return main.create_estimation(
                budget_id,
                {'periodStart': '2026-02-01', 'periodEnd': '2026-02-07', 'captureMode': 'global', 'globalProgressPct': 10},
                user=user or self.capturist,
            )

    def test_admin_budget_is_authorized_and_capturist_budget_is_pending(self):
        self.assertEqual(self.budget['approvalStatus'], 'AUTORIZADO')
        pending = self._create_as(self.capturist, name='Herrería')
        self.assertEqual(pending['approvalStatus'], 'PENDIENTE')
        self.assertEqual(pending['submittedBy'], 'mps')

    def test_cannot_estimate_on_a_pending_budget(self):
        pending = self._create_as(self.capturist, name='Herrería')
        with self.assertRaises(HTTPException) as ctx:
            self._estimate(pending['id'])
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn('autorización', ctx.exception.detail)

    def test_authorizing_unlocks_estimating(self):
        pending = self._create_as(self.capturist, name='Herrería')
        with patch.object(main, 'db', self.fake_db):
            authorized = main.authorize_estimation_budget(pending['id'], {}, user=ADMIN)
        self.assertEqual(authorized['approvalStatus'], 'AUTORIZADO')
        self.assertEqual(authorized['approvedBy'], 'boss')
        self.assertEqual(self._estimate(pending['id'])['workflowStatus'], 'BORRADOR')
        with self.assertRaises(HTTPException) as ctx, patch.object(main, 'db', self.fake_db):
            main.authorize_estimation_budget(pending['id'], {}, user=ADMIN)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_capturist_changing_prices_or_volumes_requires_reauthorization(self):
        items = [dict(i) for i in self.budget['lineItems']]
        items[0]['unitPrice'] = 70
        with patch.object(main, 'db', self.fake_db):
            updated = main.update_estimation_budget(self.budget['id'], {'lineItems': items}, user=self.capturist)
        self.assertEqual(updated['approvalStatus'], 'PENDIENTE')
        self.assertTrue(updated['reauthRequired'])
        with self.assertRaises(HTTPException):
            self._estimate(self.budget['id'])
        with patch.object(main, 'db', self.fake_db):
            again = main.authorize_estimation_budget(self.budget['id'], {}, user=ADMIN)
        self.assertEqual(again['approvalStatus'], 'AUTORIZADO')
        self.assertFalse(again['reauthRequired'])

    def test_non_material_edits_and_admin_edits_keep_the_authorization(self):
        items = [dict(i) for i in self.budget['lineItems']]
        with patch.object(main, 'db', self.fake_db):
            same = main.update_estimation_budget(self.budget['id'], {'lineItems': items, 'notes': 'ok'}, user=self.capturist)
        self.assertEqual(same['approvalStatus'], 'AUTORIZADO')
        items[0]['quantity'] = 999
        with patch.object(main, 'db', self.fake_db):
            by_admin = main.update_estimation_budget(self.budget['id'], {'lineItems': items}, user=ADMIN)
        self.assertEqual(by_admin['approvalStatus'], 'AUTORIZADO')

    def test_legacy_budgets_without_status_count_as_authorized(self):
        self.assertEqual(main.budget_approval_status({}), 'AUTORIZADO')
        self.assertEqual(main.budget_approval_status({'approvalStatus': 'PENDIENTE'}), 'PENDIENTE')

    def test_pending_budgets_are_counted_for_the_flash(self):
        self._create_as(self.capturist, name='Herrería')
        with patch.object(main, 'db', self.fake_db):
            summary = main.estimations_pending_summary(user=ADMIN)
        self.assertEqual(summary['pendingBudgets'], 1)

    def test_only_admins_can_authorize(self):
        import inspect
        dep = inspect.signature(main.authorize_estimation_budget).parameters['user'].default.dependency
        self.assertIs(dep, main.require_admin_or_superadmin)
