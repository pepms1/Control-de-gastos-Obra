import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import HTTPException

import main  # noqa: E402

WHATSAPP = """[10:32, 5/10/2026] Mármoles Pérez: Buenas tardes arquitecto, le mando el presupuesto
*COLOCACIÓN*
1. Colocación de mármol 45 m2 x $350 = $15,750
2. Boquillas y resanes 45 m² @ 40
- Sellado: $2,500
SALIDAS PARA MUEBLES:
Salida sanitaria 6 pza $450 c/u
12 ml zoclo de mármol $180
Subtotal $20,000
IVA 16%
Total $24,910
Quedo al pendiente"""


class PastedTextImportTests(unittest.TestCase):
    def test_whatsapp_message_is_parsed_with_groups_units_and_lump_sums(self):
        items, warnings = main.parse_concepto_text(WHATSAPP)
        by_desc = {i['description']: i for i in items}
        self.assertEqual(len(items), 5)
        marble = by_desc['Colocación de mármol']
        self.assertEqual((marble['quantity'], marble['unit'], marble['unitPrice'], marble['group']), (45.0, 'm2', 350.0, 'Colocación'))
        self.assertEqual(by_desc['Boquillas y resanes']['unitPrice'], 40.0)
        sealed = by_desc['Sellado']
        self.assertEqual((sealed['quantity'], sealed['unit'], sealed['unitPrice']), (1.0, 'lote', 2500.0))
        self.assertEqual(by_desc['Salida sanitaria']['group'], 'Salidas para muebles')
        self.assertEqual(by_desc['Salida sanitaria']['quantity'], 6.0)
        # "12 ml zoclo de mármol $180": cantidad y unidad al inicio
        zoclo = by_desc['zoclo de mármol']
        self.assertEqual((zoclo['quantity'], zoclo['unit'], zoclo['unitPrice']), (12.0, 'ml', 180.0))
        self.assertTrue(any('omitieron' in w for w in warnings))
        self.assertFalse(any('no coincide' in w and 'texto' in w for w in warnings))

    def test_total_that_does_not_match_is_warned(self):
        _, warnings = main.parse_concepto_text("Tubería 10 ml x 100\nTotal $5,000")
        self.assertTrue(any('no coincide con la suma' in w for w in warnings))

    def test_amount_without_marker_is_flagged_as_assumed_unit_price(self):
        items, warnings = main.parse_concepto_text("Pintura 20 m2 250")
        self.assertEqual(items[0]['unitPrice'], 250.0)
        self.assertTrue(any('precio unitario' in w for w in warnings))

    def test_table_pasted_from_excel_is_read_as_table(self):
        text = "Concepto\tUnidad\tCantidad\tPrecio Unitario\nTubería\tml\t100\t60\nConexiones\tpza\t20\t200"
        items, _ = main.parse_concepto_text(text)
        self.assertEqual([(i['description'], i['quantity'], i['unitPrice']) for i in items], [('Tubería', 100.0, 60.0), ('Conexiones', 20.0, 200.0)])

    def test_endpoint_rejects_empty_and_unparseable_text(self):
        for bad in ('', '   ', 'Hola, buen día'):
            with self.assertRaises(HTTPException) as ctx:
                main.import_estimation_conceptos_text({'text': bad}, user={'role': 'ADMIN'})
            self.assertIn(ctx.exception.status_code, (400, 422))
        ok = main.import_estimation_conceptos_text({'text': WHATSAPP}, user={'role': 'ADMIN'})
        self.assertEqual(ok['sourceType'], 'texto')
        self.assertEqual(len(ok['items']), 5)

    def test_endpoint_is_open_to_capture_users(self):
        import inspect
        dep = inspect.signature(main.import_estimation_conceptos_text).parameters['user'].default.dependency
        self.assertIs(dep, main.require_estimation_capture)
