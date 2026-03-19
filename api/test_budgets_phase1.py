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


def _get_path(document, dotted_key):
    current = document
    for part in dotted_key.split('.'):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current.get(part)
    return current, True


def _matches(document, query):
    if not query:
        return True
    for key, value in query.items():
        if key == '$and':
            return all(_matches(document, item) for item in value)
        if key == '$or':
            return any(_matches(document, item) for item in value)

        field_value, field_exists = _get_path(document, key)
        if isinstance(value, dict):
            if '$in' in value:
                if field_value not in value['$in']:
                    return False
                continue
            if '$ne' in value:
                if field_value == value['$ne']:
                    return False
                continue
            if '$exists' in value:
                if bool(field_exists) != bool(value['$exists']):
                    return False
                continue

        if field_value != value:
            return False
    return True


class _InsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class _DeleteResult:
    def __init__(self, deleted_count):
        self.deleted_count = deleted_count


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def find(self, query=None, projection=None):
        query = query or {}
        rows = [dict(doc) for doc in self.docs if _matches(doc, query)]
        if projection:
            trimmed = []
            for row in rows:
                out = {}
                for key in projection.keys():
                    if key == '_id':
                        out['_id'] = row.get('_id')
                        continue
                    value, exists = _get_path(row, key)
                    if exists:
                        cursor = out
                        parts = key.split('.')
                        for part in parts[:-1]:
                            cursor.setdefault(part, {})
                            cursor = cursor[part]
                        cursor[parts[-1]] = value
                if '_id' in row and '_id' not in out:
                    out['_id'] = row['_id']
                trimmed.append(out)
            return trimmed
        return rows

    def find_one(self, query=None, projection=None):
        rows = self.find(query, projection)
        return rows[0] if rows else None

    def insert_one(self, doc):
        payload = dict(doc)
        payload.setdefault('_id', ObjectId())
        self.docs.append(payload)
        return _InsertResult(payload['_id'])

    def update_one(self, query, update):
        for idx, doc in enumerate(self.docs):
            if not _matches(doc, query):
                continue
            next_doc = dict(doc)
            if '$set' in update:
                next_doc.update(update['$set'])
            self.docs[idx] = next_doc
            return

    def delete_one(self, query):
        for idx, doc in enumerate(self.docs):
            if _matches(doc, query):
                del self.docs[idx]
                return _DeleteResult(1)
        return _DeleteResult(0)


class BudgetsPhase1Tests(unittest.TestCase):
    def setUp(self):
        self.project_id = str(ObjectId())
        self.supplier_key = 'bpcc:acero sa|p001'

    def _fake_db(self, transactions=None, budgets=None, projects=None):
        return SimpleNamespace(
            transactions=FakeCollection(transactions or []),
            budgets=FakeCollection(budgets or []),
            projects=FakeCollection(projects or [{'_id': ObjectId(self.project_id)}]),
        )

    def test_paid_amount_matches_canonical_grouping_and_avoids_wrong_supplier(self):
        tx = [
            {'_id': '1', 'projectId': self.project_id, 'type': 'EXPENSE', 'amount': 100, 'sap': {'cardCode': 'P001', 'businessPartner': 'ACERO SA'}},
            {'_id': '2', 'projectId': self.project_id, 'type': 'EXPENSE', 'amount': 75, 'supplierId': 'legacy-1', 'sap': {'cardCode': 'P001', 'businessPartner': 'ACERO SA'}},
            {'_id': '3', 'projectId': self.project_id, 'type': 'EXPENSE', 'amount': 50, 'supplierId': 'legacy-1'},
            {'_id': '4', 'projectId': self.project_id, 'type': 'EXPENSE', 'amount': 70, 'sap': {'cardCode': 'P001', 'businessPartner': 'OTRO PROVEEDOR'}},
        ]
        fake_db = self._fake_db(transactions=tx)

        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ):
            metrics = main.compute_budget_metrics(self.project_id, self.supplier_key, 500, budget_includes_tax=True)

        self.assertEqual(metrics['paidAmount'], 225.0)
        self.assertEqual(metrics['remainingAmount'], 275.0)

    def test_duplicate_active_budget_fails(self):
        existing = [{'_id': ObjectId(), 'projectId': self.project_id, 'supplierKey': self.supplier_key, 'isActive': True}]
        fake_db = self._fake_db(budgets=existing)

        with patch.object(main, 'db', fake_db), patch.object(main, 'resolve_project_id', return_value=self.project_id):
            with self.assertRaises(HTTPException) as ctx:
                main.create_budget(
                    {
                        'projectId': self.project_id,
                        'supplierKey': self.supplier_key,
                        'supplierName': 'ACERO SA',
                        'supplierCardCode': 'P001',
                        'businessPartner': 'ACERO SA',
                        'budgetAmount': 1000,
                    },
                    request=SimpleNamespace(headers={}, query_params={}),
                    user={'role': 'SUPERADMIN', 'username': 'admin'},
                )
        self.assertEqual(ctx.exception.status_code, 409)

    def test_deactivate_and_create_new_budget_works(self):
        budget_id = ObjectId()
        existing = [{'_id': budget_id, 'projectId': self.project_id, 'supplierKey': self.supplier_key, 'isActive': True, 'budgetAmount': 100, 'notes': ''}]
        fake_db = self._fake_db(budgets=existing)

        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ), patch.object(main, 'resolve_project_id', return_value=self.project_id):
            main.update_budget(str(budget_id), {'isActive': False}, user={'role': 'SUPERADMIN'})
            created = main.create_budget(
                {
                    'projectId': self.project_id,
                    'supplierKey': self.supplier_key,
                    'supplierName': 'ACERO SA',
                    'supplierCardCode': 'P001',
                    'businessPartner': 'ACERO SA',
                    'budgetAmount': 200,
                },
                request=SimpleNamespace(headers={}, query_params={}),
                user={'role': 'SUPERADMIN', 'username': 'admin'},
            )

        self.assertEqual(created['budgetAmount'], 200)

    def test_delete_budget_removes_document(self):
        budget_id = ObjectId()
        existing = [{'_id': budget_id, 'projectId': self.project_id, 'supplierKey': self.supplier_key, 'isActive': True, 'budgetAmount': 100, 'notes': ''}]
        fake_db = self._fake_db(budgets=existing)

        with patch.object(main, 'db', fake_db):
            result = main.delete_budget(str(budget_id), user={'role': 'SUPERADMIN'})

        self.assertEqual(result, {'ok': True})
        self.assertEqual(fake_db.budgets.find_one({'_id': budget_id}), None)

    def test_supplier_without_movements_returns_paid_zero(self):
        fake_db = self._fake_db(transactions=[])
        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ):
            metrics = main.compute_budget_metrics(self.project_id, self.supplier_key, 100, budget_includes_tax=True)
        self.assertEqual(metrics['paidAmount'], 0.0)

    def test_budget_zero_does_not_break_progress(self):
        fake_db = self._fake_db(transactions=[])
        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ):
            metrics = main.compute_budget_metrics(self.project_id, self.supplier_key, 0, budget_includes_tax=True)
        self.assertEqual(metrics['progressPct'], 0.0)

    def test_negative_remaining_marks_exceeded(self):
        tx = [{'_id': '1', 'projectId': self.project_id, 'type': 'EXPENSE', 'amount': 250, 'sap': {'cardCode': 'P001', 'businessPartner': 'ACERO SA'}}]
        fake_db = self._fake_db(transactions=tx)
        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ):
            metrics = main.compute_budget_metrics(self.project_id, self.supplier_key, 100, budget_includes_tax=True)
        self.assertEqual(metrics['status'], 'EXCEEDED')
        self.assertLess(metrics['remainingAmount'], 0)


    def test_budget_without_tax_uses_compute_monto_sin_iva_logic(self):
        tx = [
            {
                '_id': '1',
                'projectId': self.project_id,
                'type': 'EXPENSE',
                'amount': 116,
                'tax': {'subtotal': 100, 'iva': 16, 'totalFactura': 116},
                'sap': {'cardCode': 'P001', 'businessPartner': 'ACERO SA'},
            }
        ]
        fake_db = self._fake_db(transactions=tx)

        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ):
            metrics_with_tax = main.compute_budget_metrics(self.project_id, self.supplier_key, 200, budget_includes_tax=True)
            metrics_without_tax = main.compute_budget_metrics(self.project_id, self.supplier_key, 200, budget_includes_tax=False)

        self.assertEqual(metrics_with_tax['paidAmount'], 116.0)
        self.assertEqual(metrics_without_tax['paidAmount'], 100.0)

    def test_serialize_budget_defaults_budget_includes_tax_for_legacy_docs(self):
        tx = [{'_id': '1', 'projectId': self.project_id, 'type': 'EXPENSE', 'amount': 116, 'tax': {'subtotal': 100, 'iva': 16, 'totalFactura': 116}, 'sap': {'cardCode': 'P001', 'businessPartner': 'ACERO SA'}}]
        fake_db = self._fake_db(transactions=tx)
        legacy_doc = {'_id': ObjectId(), 'projectId': self.project_id, 'supplierKey': self.supplier_key, 'budgetAmount': 200}

        with patch.object(main, 'db', fake_db), patch.object(main, 'with_legacy_project_filter', side_effect=lambda q, _p: q), patch.object(
            main, 'build_transactions_query', return_value={}
        ):
            serialized = main.serialize_budget_with_metrics(legacy_doc)

        self.assertTrue(serialized['budgetIncludesTax'])
        self.assertEqual(serialized['paidAmount'], 116.0)

    def test_create_budget_persists_budget_includes_tax(self):
        fake_db = self._fake_db(budgets=[])

        with patch.object(main, 'db', fake_db), patch.object(main, 'resolve_project_id', return_value=self.project_id):
            created = main.create_budget(
                {
                    'projectId': self.project_id,
                    'supplierKey': self.supplier_key,
                    'supplierName': 'ACERO SA',
                    'supplierCardCode': 'P001',
                    'businessPartner': 'ACERO SA',
                    'budgetAmount': 1000,
                    'budgetIncludesTax': False,
                },
                request=SimpleNamespace(headers={}, query_params={}),
                user={'role': 'SUPERADMIN', 'username': 'admin'},
            )

        self.assertFalse(created['budgetIncludesTax'])

    def test_roles_access_for_budget_module(self):
        with self.assertRaises(HTTPException):
            main.require_admin_or_superadmin({'role': 'VIEWER'})
        self.assertEqual(main.require_admin_or_superadmin({'role': 'ADMIN'})['role'], 'ADMIN')
        self.assertEqual(main.require_admin_or_superadmin({'role': 'SUPERADMIN'})['role'], 'SUPERADMIN')

    def test_validate_budget_amount_rejects_invalid_values(self):
        self.assertEqual(main.validate_budget_amount(0), 0)
        with self.assertRaises(HTTPException):
            main.validate_budget_amount(-1)
        with self.assertRaises(HTTPException):
            main.validate_budget_amount('1000000000001')

    def test_supplier_key_mismatch_with_identity_fields_fails(self):
        with self.assertRaises(HTTPException):
            main.resolve_budget_supplier_key(
                {
                    'supplierKey': 'cardcode:p001',
                    'supplierCardCode': 'P001',
                    'businessPartner': 'ACERO SA',
                    'supplierName': 'ACERO SA',
                }
            )


if __name__ == '__main__':
    unittest.main()
