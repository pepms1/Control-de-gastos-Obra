import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class TransactionsSearchCategory2Tests(unittest.TestCase):
    def test_search_query_includes_supplier_rule_condition_for_trabajos_especiales_category2_name(self):
        class FakeCategories:
            def find(self, *_args, **_kwargs):
                return []

        class FakeSupplierCategory2Rules:
            def find(self, _query, _projection):
                return [
                    {
                        'supplierKey': 'bpcc:proveedor demo|p-001',
                        'supplierCardCode': 'P-001',
                        'businessPartner': 'Proveedor Demo',
                        'supplierName': 'Proveedor Demo SA',
                    }
                ]

        fake_db = type(
            'FakeDb',
            (),
            {
                'categories': FakeCategories(),
                'supplierCategory2Rules': FakeSupplierCategory2Rules(),
            },
        )()

        with patch.object(main, 'db', fake_db):
            query = main.build_transactions_query(search_query='Acabados Especiales')

        conditions = query.get('$or')
        self.assertIsInstance(conditions, list)

        derived_condition = next(
            (
                condition
                for condition in conditions
                if isinstance(condition, dict)
                and '$and' in condition
                and len(condition['$and']) == 2
                and isinstance(condition['$and'][0], dict)
                and '$or' in condition['$and'][0]
            ),
            None,
        )

        self.assertIsNotNone(derived_condition)

        trabajos_matchers = derived_condition['$and'][0]['$or']
        self.assertTrue(
            any(matcher.get('categoryEffectiveName', {}).get('$regex') == '^trabajos\\ especiales' for matcher in trabajos_matchers)
        )

        supplier_matchers = derived_condition['$and'][1]['$or']
        self.assertTrue(any('sap.cardCode' in matcher for matcher in supplier_matchers))
        self.assertTrue(any('sap.businessPartner' in matcher for matcher in supplier_matchers))
        self.assertTrue(any('supplierName' in matcher for matcher in supplier_matchers))


if __name__ == '__main__':
    unittest.main()
