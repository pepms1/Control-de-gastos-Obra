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


class EstimationGroupsTests(phase1.EstimationsPhase1Tests):
    """Presupuestos por grupos (con anticipo propio) y conceptos extra, probados
    con la hoja de estimación real de la obra Calderón de la Barca."""

    GROUPS = [
        ('TUBERIAS MUEBLES, WC, LAV, REG, FREG, LAVA, CALENTA', 425405.00, 20),
        ('BAJADAS', 293276.00, 10),
        ('CUARTO DE BOMBAS', 177150.00, 29),
        ('COLOCACION MUEBLES', 32100.00, 0),
    ]

    def setUp(self):
        super().setUp()
        self.fake_db = self._fake_db()
        line_items = [
            {'description': f'{name} (global)', 'unit': 'pza', 'quantity': 1, 'unitPrice': amount, 'group': name}
            for name, amount, _pct in self.GROUPS
        ]
        self.budget = self._create_budget(
            self.fake_db,
            lineItems=line_items,
            retentionPct=0,
            advanceAmortizationEnabled=False,
            advanceAmount=0,
            groupAdvancePcts={name: pct for name, _a, pct in self.GROUPS if pct},
        )
        self.bid = self.budget['id']

    def _call(self, fn, *args, **kwargs):
        with patch.object(main, 'db', self.fake_db), patch.object(
            main, 'with_legacy_project_filter', side_effect=lambda q, _p: q
        ), patch.object(main, 'build_transactions_query', return_value={}):
            return fn(*args, **kwargs)

    def _add_extras(self, items, **payload):
        return self._call(main.add_estimation_budget_extras, self.bid, {'conceptos': items, **payload}, user=SUPERADMIN)

    def _estimate(self, group_progress, user=SUPERADMIN, **extra):
        body = {
            'periodStart': '2026-10-05', 'periodEnd': '2026-10-05', 'captureMode': 'group',
            'groupProgress': group_progress, **extra,
        }
        return self._call(main.create_estimation, self.bid, body, user=user)

    def _approve(self, estimation):
        self._call(main.submit_estimation, self.bid, estimation['id'], user=SUPERADMIN)
        return self._call(main.approve_estimation, self.bid, estimation['id'], {}, user=SUPERADMIN)

    # ---- presupuesto con grupos ----

    def test_budget_groups_and_planned_advance_come_from_group_percentages(self):
        self.assertEqual(self.budget['totalContractedAmount'], 927931.0)
        self.assertEqual(self.budget['advanceAmount'], 165782.10)  # 85,081.00 + 29,327.60 + 51,373.50
        self.assertTrue(self.budget['advanceAmortizationEnabled'])
        groups = {g['name']: g for g in self.budget['groups']}
        self.assertEqual(groups['BAJADAS']['advanceAmount'], 29327.60)
        self.assertEqual(groups['COLOCACION MUEBLES']['advancePct'], 0)
        self.assertEqual(len(groups), 4)

    def test_concepts_keep_their_group_when_editing_the_budget(self):
        items = [dict(i) for i in self.budget['lineItems']]
        items[1]['description'] = 'BAJADAS (editado)'
        updated = self._call(main.update_estimation_budget, self.bid, {'lineItems': items}, user=SUPERADMIN)
        self.assertEqual(updated['lineItems'][1]['group'], 'BAJADAS')
        self.assertEqual(updated['advanceAmount'], 165782.10)

    # ---- captura por grupo ----

    def test_group_progress_by_percentage_applies_to_every_concept_of_the_group(self):
        two_concepts = self._create_budget(
            self.fake_db, name='Con varios conceptos', supplierCardCode='P777', businessPartner='OTRO', supplierName='Otro',
            retentionPct=0, advanceAmortizationEnabled=False, advanceAmount=0,
            lineItems=[
                {'description': 'A', 'unit': 'm', 'quantity': 10, 'unitPrice': 100, 'group': 'G1'},
                {'description': 'B', 'unit': 'm', 'quantity': 20, 'unitPrice': 50, 'group': 'G1'},
                {'description': 'C', 'unit': 'm', 'quantity': 4, 'unitPrice': 500, 'group': 'G2'},
            ],
        )
        est = self._call(
            main.create_estimation, two_concepts['id'],
            {'periodStart': '2026-10-05', 'periodEnd': '2026-10-05', 'captureMode': 'group', 'groupProgress': [{'group': 'G1', 'progressPct': 50}]},
            user=SUPERADMIN,
        )
        lines = {li['description']: li for li in est['lineItems']}
        self.assertEqual(lines['A']['periodQuantity'], 5)
        self.assertEqual(lines['B']['periodQuantity'], 10)
        self.assertEqual(lines['C']['periodQuantity'], 0)  # grupo omitido: no avanza
        self.assertEqual(est['periodSubtotal'], 1000)  # 5x100 + 10x50
        self.assertEqual(est['captureMode'], 'group')

    def test_group_progress_validation(self):
        with self.assertRaises(HTTPException) as ctx:
            self._estimate([{'group': 'NO EXISTE', 'progressPct': 10}])
        self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException) as ctx:
            self._estimate([{'group': 'BAJADAS', 'progressAmount': 999999999}])
        self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException) as ctx:
            self._estimate([{'group': 'BAJADAS', 'progressPct': 120}])
        self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException) as ctx:
            self._estimate([])
        self.assertEqual(ctx.exception.status_code, 400)

    # ---- la hoja real de la arquitecta ----

    def test_matches_the_architects_estimation_sheet_to_the_cent(self):
        self._add_extras(
            [
                {'description': 'TUBERIAS EN EXCAVACION', 'unit': 'pza', 'quantity': 1, 'unitPrice': 6000},
                {'description': 'MPV. BAÑO OBRA', 'unit': 'pza', 'quantity': 1, 'unitPrice': 4200},
            ],
            kind='extra',
        )
        # pagado a la fecha 535,200.00 = anticipo 165,782.10 + pagos a cuenta 369,417.90
        self._call(
            main.set_estimation_budget_opening_balance, self.bid,
            {'manualAdvanceAmount': 165782.10, 'manualPriorPaidAmount': 369417.90}, user=SUPERADMIN,
        )
        est = self._estimate([
            {'group': self.GROUPS[0][0], 'progressAmount': 164602.00},
            {'group': 'BAJADAS', 'progressAmount': 191533.90},
            {'group': 'CUARTO DE BOMBAS', 'progressAmount': 118620.00},
            {'group': 'Extras', 'progressPct': 100},
        ])

        self.assertEqual(est['periodSubtotal'], 484955.90)  # avance acumulado
        self.assertEqual(est['advanceAmortizationAmount'], 86473.59)  # amortización de anticipos
        self.assertEqual(est['netBeforePriorPayments'], 398482.31)  # saldo acumulado
        self.assertEqual(est['priorPaidApplied'], 369417.90)  # pagado a la fecha - anticipo
        self.assertEqual(est['totalToPay'], 29064.41)  # saldo total de la hoja

        rows = {r['group']: r for r in est['groupBreakdown']}
        first = rows[self.GROUPS[0][0]]
        self.assertEqual((first['budgetAmount'], first['advanceAmount'], first['advancePct']), (425405.00, 85081.00, 20))
        self.assertEqual((first['cumulativeAmount'], first['amortizationAmount'], first['netAmount']), (164602.00, 32920.40, 131681.60))
        self.assertEqual(round(first['cumulativePct'] / 100, 2), 0.39)
        bajadas = rows['BAJADAS']
        self.assertEqual((bajadas['advanceAmount'], bajadas['amortizationAmount'], bajadas['netAmount']), (29327.60, 19153.39, 172380.51))
        bombas = rows['CUARTO DE BOMBAS']
        self.assertEqual((bombas['advanceAmount'], bombas['amortizationAmount'], bombas['netAmount']), (51373.50, 34399.80, 84220.20))
        extras = rows['Extras']
        self.assertEqual((extras['budgetAmount'], extras['advanceAmount'], extras['amortizationAmount'], extras['netAmount']), (10200.00, 0, 0, 10200.00))
        self.assertTrue(extras['isExtra'])
        self.assertEqual(rows['COLOCACION MUEBLES']['cumulativeAmount'], 0)

    def test_total_budget_with_extras_matches_the_sheet(self):
        budget = self._add_extras(
            [{'description': 'TUBERIAS EN EXCAVACION', 'unit': 'pza', 'quantity': 1, 'unitPrice': 6000},
             {'description': 'MPV. BAÑO OBRA', 'unit': 'pza', 'quantity': 1, 'unitPrice': 4200}],
        )
        self.assertEqual(budget['totalContractedAmount'], 938131.00)  # 927,931 + 10,200
        self.assertEqual(budget['extraAmount'], 10200.00)

    # ---- extras / adicionales ----

    def test_extras_go_to_their_own_group_without_advance(self):
        budget = self._add_extras([{'description': 'Extra 1', 'unit': 'pza', 'quantity': 2, 'unitPrice': 500}])
        extras = [i for i in budget['lineItems'] if i.get('isExtra')]
        self.assertEqual(len(extras), 1)
        self.assertEqual((extras[0]['group'], extras[0]['extraKind']), ('Extras', 'extra'))
        self.assertEqual(extras[0]['addedBy'], 'admin')
        groups = {g['name']: g for g in budget['groups']}
        self.assertEqual(groups['Extras']['advancePct'], 0)
        self.assertTrue(groups['Extras']['isExtra'])
        self.assertEqual(budget['advanceAmount'], 165782.10)  # el anticipo no cambia

    def test_more_extras_join_the_same_group_and_additional_budgets_are_numbered(self):
        self._add_extras([{'description': 'Extra 1', 'unit': 'pza', 'quantity': 1, 'unitPrice': 100}])
        budget = self._add_extras([{'description': 'Extra 2', 'unit': 'pza', 'quantity': 1, 'unitPrice': 200}])
        self.assertEqual([g['name'] for g in budget['groups']].count('Extras'), 1)
        self.assertEqual(budget['extraAmount'], 300)

        budget = self._add_extras([{'description': 'Adicional A', 'unit': 'pza', 'quantity': 1, 'unitPrice': 1000}], kind='adicional')
        names = [g['name'] for g in budget['groups']]
        self.assertIn('Adicional 1', names)
        budget = self._add_extras([{'description': 'Adicional B', 'unit': 'pza', 'quantity': 1, 'unitPrice': 2000}], kind='adicional')
        self.assertIn('Adicional 2', [g['name'] for g in budget['groups']])
        custom = self._add_extras([{'description': 'Otro', 'unit': 'pza', 'quantity': 1, 'unitPrice': 5}], kind='adicional', groupName='Adicional baños')
        self.assertIn('Adicional baños', [g['name'] for g in custom['groups']])
        self.assertEqual(len(custom['extrasLog']), 5)

    def test_extras_can_be_added_after_estimations_exist_without_losing_history(self):
        first = self._estimate([{'group': 'BAJADAS', 'progressPct': 50}])
        self._approve(first)
        budget = self._add_extras([{'description': 'Extra tardío', 'unit': 'pza', 'quantity': 1, 'unitPrice': 8000}])
        self.assertEqual(budget['totalContractedAmount'], 935931.0)
        second = self._estimate([{'group': 'BAJADAS', 'progressPct': 60}, {'group': 'Extras', 'progressPct': 100}])
        rows = {r['group']: r for r in second['groupBreakdown']}
        self.assertEqual(rows['BAJADAS']['previousAmount'], 146638.00)  # 50% de 293,276
        self.assertEqual(rows['BAJADAS']['periodAmount'], 29327.60)  # 10% más
        self.assertEqual(rows['Extras']['periodAmount'], 8000)
        self.assertEqual(rows['BAJADAS']['cumulativeAmortization'], round(14663.80 + 2932.76, 2))

    def test_open_draft_shows_extras_added_afterwards(self):
        draft = self._estimate([{'group': 'BAJADAS', 'progressPct': 10}])
        self._add_extras([{'description': 'Extra', 'unit': 'pza', 'quantity': 1, 'unitPrice': 500}])
        listed = self._call(main.list_estimations, self.bid, user=SUPERADMIN)
        self.assertIn('Extras', [r['group'] for r in listed[0]['groupBreakdown']])

    def test_extras_require_admin_and_at_least_one_concept(self):
        with self.assertRaises(HTTPException) as ctx:
            main.require_admin_or_superadmin(user=CAPTURIST)
        self.assertEqual(ctx.exception.status_code, 403)
        with self.assertRaises(HTTPException) as ctx:
            self._add_extras([])
        self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException) as ctx:
            self._add_extras([{'description': 'Mal', 'unit': 'pza', 'quantity': 0, 'unitPrice': 10}])
        self.assertEqual(ctx.exception.status_code, 400)

    def test_extra_flag_survives_a_budget_edit_that_omits_it(self):
        budget = self._add_extras([{'description': 'Extra', 'unit': 'pza', 'quantity': 1, 'unitPrice': 500}])
        stripped = [
            {k: v for k, v in item.items() if k not in ('isExtra', 'extraKind', 'addedAt', 'addedBy')}
            for item in budget['lineItems']
        ]
        updated = self._call(main.update_estimation_budget, self.bid, {'lineItems': stripped}, user=SUPERADMIN)
        extra = [i for i in updated['lineItems'] if i['description'] == 'Extra'][0]
        self.assertTrue(extra['isExtra'])
        self.assertEqual(extra['addedBy'], 'admin')

    # ---- amortización ----

    def test_amortization_is_capped_by_the_remaining_advance_and_spread_by_group(self):
        # Se avanza el 100% de todo: lo amortizable (22.5% promedio) excede el anticipo; se topa.
        est = self._estimate([{'group': name, 'progressPct': 100} for name, _a, _p in self.GROUPS])
        self.assertLessEqual(est['advanceAmortizationAmount'], 165782.10)
        self.assertEqual(est['advanceAmortizationAmount'], 165782.10)  # 85,081 + 29,327.60 + 51,373.50
        total_by_group = sum(r['amortizationAmount'] for r in est['groupBreakdown'])
        self.assertAlmostEqual(total_by_group, est['advanceAmortizationAmount'], places=1)

    def test_budget_without_groups_keeps_the_uniform_advance_rate(self):
        plain = self._create_budget(
            self.fake_db, name='Sin grupos', supplierCardCode='P888', businessPartner='SIN GRUPOS', supplierName='Sin grupos',
            retentionPct=0, advanceAmortizationEnabled=True, advanceAmount=1000,
        )  # total 10,000 -> 10% uniforme
        est = self._call(
            main.create_estimation, plain['id'],
            {'periodStart': '2026-10-05', 'periodEnd': '2026-10-05', 'captureMode': 'global', 'globalProgressPct': 50}, user=SUPERADMIN,
        )
        self.assertEqual(est['periodSubtotal'], 5000)
        self.assertEqual(est['advanceAmortizationAmount'], 500)
        self.assertEqual(len(est['groupBreakdown']), 1)
        self.assertEqual(est['groupBreakdown'][0]['amortizationAmount'], 500)

    # ---- precisión de cantidades ----

    def test_fractional_quantities_are_not_rounded_to_two_decimals(self):
        est = self._estimate([{'group': 'BAJADAS', 'progressAmount': 191533.90}])
        line = [li for li in est['lineItems'] if li['group'] == 'BAJADAS'][0]
        self.assertGreater(len(str(line['periodQuantity']).split('.')[1]), 2)
        self.assertEqual(line['periodAmount'], 191533.90)


# Las pruebas heredadas de phase 1 ya corren en su propio módulo.
for _name in [n for n in dir(phase1.EstimationsPhase1Tests) if n.startswith('test_')]:
    setattr(EstimationGroupsTests, _name, None)


if __name__ == '__main__':
    unittest.main()
