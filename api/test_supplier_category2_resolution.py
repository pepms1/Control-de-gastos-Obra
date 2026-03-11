import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class SupplierCategory2ResolutionTests(unittest.TestCase):
    def _tx(self, *, card_code='P-001', business_partner='Proveedor Grande', supplier_name='Proveedor Grande SA'):
        return {
            'categoryEffectiveName': 'Trabajos Especiales / Obra Civil',
            'supplierName': supplier_name,
            'sap': {
                'cardCode': card_code,
                'businessPartner': business_partner,
            },
        }

    def test_resolve_category2_falls_back_to_card_code_when_supplier_key_changed(self):
        rule = {
            'supplierKey': 'cardcode:p-001',
            'supplierCardCode': 'P-001',
            'businessPartner': '',
            'supplierName': 'Proveedor Grande SA',
            'category2Id': 'cat-2',
            'category2Name': 'Acabados Especiales',
        }
        indexes = main.build_supplier_rule_indexes([rule])

        resolved = main.resolve_transaction_category2(
            self._tx(card_code='P-001', business_partner='Proveedor Grande Renovado'),
            supplier_rules_by_key=indexes['by_key'],
            supplier_rule_indexes=indexes,
        )

        self.assertEqual(resolved['resolvedCategory2Source'], 'supplier_rule')
        self.assertEqual(resolved['resolvedCategory2Name'], 'Acabados Especiales')

    def test_resolve_category2_does_not_use_ambiguous_card_code_fallback(self):
        rule_a = {
            'supplierKey': 'cardcode:p-001',
            'supplierCardCode': 'P-001',
            'businessPartner': '',
            'supplierName': 'Proveedor Grande SA',
            'category2Id': 'cat-2a',
            'category2Name': 'Acabados Especiales',
        }
        rule_b = {
            'supplierKey': 'bpcc:otro|p-001',
            'supplierCardCode': 'P-001',
            'businessPartner': 'Otro Proveedor',
            'supplierName': 'Otro Proveedor',
            'category2Id': 'cat-2b',
            'category2Name': 'Herrería',
        }
        indexes = main.build_supplier_rule_indexes([rule_a, rule_b])

        resolved = main.resolve_transaction_category2(
            self._tx(card_code='P-001', business_partner='Proveedor Nuevo', supplier_name='Proveedor Distinto'),
            supplier_rules_by_key=indexes['by_key'],
            supplier_rule_indexes=indexes,
        )

        self.assertEqual(resolved['resolvedCategory2Source'], 'trabajos_especiales_unclassified')
        self.assertEqual(resolved['resolvedCategory2Name'], main.TRABAJOS_ESPECIALES_UNCLASSIFIED_NAME)


if __name__ == '__main__':
    unittest.main()
