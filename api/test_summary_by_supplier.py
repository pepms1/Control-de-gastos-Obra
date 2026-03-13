import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class SupplierSummaryGroupingTests(unittest.TestCase):
    def test_trusted_id_map_links_legacy_id_to_single_cardcode_identity(self):
        movements = [
            {
                '_id': 'tx1',
                'supplierId': 'legacy-1',
                'sap': {'cardCode': 'C100', 'businessPartner': 'OMAR SALAS ALDANA'},
                'supplierName': 'OMAR SALAS ALDANA',
            },
            {
                '_id': 'tx2',
                'supplierId': 'legacy-1',
                'sap': {'cardCode': 'C100', 'businessPartner': 'OMAR SALAS ALDANA'},
            },
            {
                '_id': 'tx3',
                'supplierId': 'legacy-2',
                'supplierName': 'Proveedor sin SAP',
            },
        ]

        trusted = main._build_trusted_id_supplier_key_map(movements)

        self.assertEqual(trusted.get('legacy-1'), 'bpcc:omar salas aldana|c100')
        self.assertNotIn('legacy-2', trusted)

    def test_trusted_id_map_ignores_ambiguous_legacy_id(self):
        movements = [
            {'_id': 'tx1', 'supplierId': 'legacy-1', 'sap': {'cardCode': 'C100'}},
            {'_id': 'tx2', 'supplierId': 'legacy-1', 'sap': {'cardCode': 'C200'}},
        ]

        trusted = main._build_trusted_id_supplier_key_map(movements)

        self.assertEqual(trusted, {})

    def test_bucket_key_uses_trusted_bridge_for_missing_canonical_data(self):
        trusted = {'legacy-1': 'bpcc:omar salas aldana|c100'}
        tx = {'_id': 'tx-no-sap', 'supplierId': 'legacy-1', 'supplierName': ''}

        bucket_key = main._build_supplier_summary_bucket_key(tx, trusted)

        self.assertEqual(bucket_key, 'bpcc:omar salas aldana|c100')

    def test_bucket_key_falls_back_to_supplier_id_when_no_bridge(self):
        tx = {'_id': 'tx-no-sap', 'supplierId': 'legacy-2', 'supplierName': ''}

        bucket_key = main._build_supplier_summary_bucket_key(tx, {})

        self.assertEqual(bucket_key, 'supplier:legacy-2')

    def test_bucket_key_uses_composite_legacy_ids_to_avoid_vendor_collisions(self):
        tx = {'_id': 'tx-no-sap', 'supplierId': 'legacy-shared', 'vendor_id': 'vendor-a', 'supplierName': ''}

        bucket_key = main._build_supplier_summary_bucket_key(tx, {})

        self.assertEqual(bucket_key, 'supplier:legacy-shared|vendor:vendor-a')


class SupplierIdentityKeyTests(unittest.TestCase):
    def test_build_supplier_key_uses_business_partner_and_cardcode_composite_when_both_exist(self):
        key = main.build_supplier_key('P00071', 'OMAR SALAS ALDANA', None)

        self.assertEqual(key, 'bpcc:omar salas aldana|p00071')

    def test_build_supplier_key_avoids_collision_same_cardcode_different_business_partner(self):
        omar_key = main.build_supplier_key('P00071', 'OMAR SALAS ALDANA', 'OMAR SALAS ALDANA')
        camargo_key = main.build_supplier_key('P00071', 'MATERIALES CAMARGO, S.A. DE C.V.', 'MATERIALES CAMARGO, S.A. DE C.V.')

        self.assertNotEqual(omar_key, camargo_key)
        self.assertEqual(omar_key, 'bpcc:omar salas aldana|p00071')
        self.assertEqual(camargo_key, 'bpcc:materiales camargo, s.a. de c.v.|p00071')

    def test_summary_bucket_key_keeps_distinct_suppliers_same_cardcode(self):
        tx_omar = {
            '_id': 'tx1',
            'sap': {'cardCode': 'P00071', 'businessPartner': 'OMAR SALAS ALDANA'},
            'supplierName': 'OMAR SALAS ALDANA',
        }
        tx_camargo = {
            '_id': 'tx2',
            'sap': {'cardCode': 'P00071', 'businessPartner': 'MATERIALES CAMARGO, S.A. DE C.V.'},
            'supplierName': 'MATERIALES CAMARGO, S.A. DE C.V.',
        }

        key_omar = main._build_supplier_summary_bucket_key(tx_omar, {})
        key_camargo = main._build_supplier_summary_bucket_key(tx_camargo, {})

        self.assertNotEqual(key_omar, key_camargo)



class SupplierSummaryRegressionTests(unittest.TestCase):
    def test_summary_by_supplier_keeps_distinct_vendors_when_legacy_supplier_id_is_shared(self):
        class FakeTransactions:
            def find(self, _query, _projection):
                return [
                    {'_id': 'tx1', 'supplierId': 'legacy-shared', 'vendor_id': 'vendor-1', 'amount': 100},
                    {'_id': 'tx2', 'supplierId': 'legacy-shared', 'vendor_id': 'vendor-2', 'amount': 200},
                    {'_id': 'tx3', 'supplierId': 'legacy-shared', 'vendor_id': 'vendor-3', 'amount': 300},
                ]

        class EmptyFind:
            def find(self, *_args, **_kwargs):
                return []

        fake_db = type(
            'FakeDb',
            (),
            {
                'transactions': FakeTransactions(),
                'suppliers': EmptyFind(),
                'vendors': EmptyFind(),
            },
        )()

        with patch.object(main, 'db', fake_db), patch.object(main, 'resolve_project_id', return_value='project-1'), patch.object(
            main, 'can_access_project', return_value=True
        ), patch.object(main, 'build_transactions_query', return_value={}), patch.object(
            main, 'with_legacy_project_filter', side_effect=lambda q, _project_id: q
        ):
            result = main.summary_expenses_by_supplier(
                projectId='project-1',
                include_iva=True,
                user={'id': 'u1', 'role': 'ADMIN'},
            )

        self.assertEqual(len(result), 3)

    def test_summary_by_supplier_prefers_catalog_name_for_display_label(self):
        supplier_oid = '507f1f77bcf86cd799439011'

        class FakeTransactions:
            def find(self, _query, _projection):
                return [
                    {
                        '_id': 'tx1',
                        'supplierId': supplier_oid,
                        'supplierName': 'Alias poco reconocible',
                        'sap': {'cardCode': 'C100', 'businessPartner': 'OMAR SALAS ALDANA'},
                        'amount': 100,
                    }
                ]

        class FakeSuppliers:
            def find(self, _query, _projection):
                return [{'_id': main.ObjectId(supplier_oid), 'name': 'OMAR SALAS ALDANA'}]

        class EmptyFind:
            def find(self, *_args, **_kwargs):
                return []

        fake_db = type(
            'FakeDb',
            (),
            {
                'transactions': FakeTransactions(),
                'suppliers': FakeSuppliers(),
                'vendors': EmptyFind(),
            },
        )()

        with patch.object(main, 'db', fake_db), patch.object(main, 'resolve_project_id', return_value='project-1'), patch.object(
            main, 'can_access_project', return_value=True
        ), patch.object(main, 'build_transactions_query', return_value={}), patch.object(
            main, 'with_legacy_project_filter', side_effect=lambda q, _project_id: q
        ):
            result = main.summary_expenses_by_supplier(
                projectId='project-1',
                include_iva=True,
                user={'id': 'u1', 'role': 'ADMIN'},
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['supplierName'], 'OMAR SALAS ALDANA')


class SupplierSummaryFilterParityTests(unittest.TestCase):
    def test_summary_by_supplier_forwards_date_and_source_filters_to_transactions_query(self):
        class FakeTransactions:
            def find(self, _query, _projection):
                return []

        fake_db = type('FakeDb', (), {'transactions': FakeTransactions()})()

        captured = {}

        def fake_build_transactions_query(**kwargs):
            captured.update(kwargs)
            return {}

        with patch.object(main, 'db', fake_db), patch.object(main, 'resolve_project_id', return_value='project-1'), patch.object(
            main, 'can_access_project', return_value=True
        ), patch.object(main, 'build_transactions_query', side_effect=fake_build_transactions_query), patch.object(
            main, 'with_legacy_project_filter', side_effect=lambda q, _project_id: q
        ):
            result = main.summary_expenses_by_supplier(
                projectId='project-1',
                include_iva=False,
                date_from='2026-01-01',
                date_to='2026-01-31',
                source='sap-sbo',
                sourceDb='SBO_CDB',
                user={'id': 'u1', 'role': 'ADMIN'},
            )

        self.assertEqual(result, [])
        self.assertEqual(captured.get('type_value'), 'EXPENSE')
        self.assertEqual(captured.get('date_from'), '2026-01-01')
        self.assertEqual(captured.get('date_to'), '2026-01-31')
        self.assertEqual(captured.get('source'), 'sap-sbo')
        self.assertEqual(captured.get('source_db'), 'SBO_CDB')


if __name__ == '__main__':
    unittest.main()
