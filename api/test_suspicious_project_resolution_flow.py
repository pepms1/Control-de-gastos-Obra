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


class FakeTransactions:
    def __init__(self, docs):
        self.docs = {str(doc['_id']): doc for doc in docs}

    def find_one(self, query, projection=None):
        target_id = str(query.get('_id')) if query.get('_id') is not None else None
        doc = self.docs.get(target_id)
        if not doc:
            return None
        source = query.get('source')
        if source and doc.get('source') != source:
            return None
        return doc

    def update_one(self, query, update):
        target_id = str(query.get('_id')) if query.get('_id') is not None else None
        doc = self.docs.get(target_id)
        if not doc:
            return None
        sets = update.get('$set', {})
        for key, value in sets.items():
            if '.' in key:
                first, second = key.split('.', 1)
                root = doc.setdefault(first, {})
                root[second] = value
            else:
                doc[key] = value
        return None


class FakeProjects:
    def __init__(self, docs):
        self.docs = {str(doc['_id']): doc for doc in docs}

    def find_one(self, query, projection=None):
        target_id = query.get('_id')
        if target_id is None:
            return None
        return self.docs.get(str(target_id))


class SuspiciousProjectResolutionFlowTests(unittest.TestCase):
    def setUp(self):
        self.tx_id = ObjectId()
        self.project_id = ObjectId()
        self.base_tx = {
            '_id': self.tx_id,
            'source': 'sap-sbo',
            'sap': {
                'movementType': 'egreso',
                'isProjectResolutionSuspicious': True,
                'documentProjectCode': 'DOC-CODE',
                'documentProjectName': 'Proyecto Documento',
                'paymentProjectCode': 'PAY-CODE',
                'paymentProjectName': 'Proyecto Pago',
            },
        }

    def _fake_db(self, tx_doc=None, project_doc=None):
        tx = tx_doc or self.base_tx
        project = project_doc or {
            '_id': self.project_id,
            'name': 'Proyecto Custom',
            'sap': {'projectCode': 'CUS-CODE'},
        }
        return type(
            'FakeDb',
            (),
            {
                'transactions': FakeTransactions([tx]),
                'projects': FakeProjects([project]),
            },
        )()

    def test_resolve_to_document_and_payment(self):
        fake_db = self._fake_db()
        with patch.object(main, 'db', fake_db):
            out_document = main.resolve_admin_suspicious_project_resolution(
                str(self.tx_id),
                {'resolve_to': 'document', 'resolution_reason': 'doc path'},
                user={'username': 'admin'},
            )
            out_payment = main.resolve_admin_suspicious_project_resolution(
                str(self.tx_id),
                {'resolve_to': 'payment', 'resolution_reason': 'payment path'},
                user={'username': 'admin'},
            )

        self.assertTrue(out_document['ok'])
        self.assertEqual(out_document['manualResolvedProjectCode'], 'DOC-CODE')
        self.assertEqual(out_payment['manualResolvedProjectCode'], 'PAY-CODE')

    def test_resolve_with_custom_project_and_camel_case_aliases(self):
        fake_db = self._fake_db()
        with patch.object(main, 'db', fake_db):
            out = main.resolve_admin_suspicious_project_resolution(
                str(self.tx_id),
                {
                    'resolveTo': 'custom',
                    'projectId': str(self.project_id),
                    'resolutionReason': 'manual choice',
                },
                user={'displayName': 'Super Admin'},
            )

        self.assertTrue(out['ok'])
        self.assertEqual(out['manualResolvedProjectId'], str(self.project_id))
        self.assertEqual(out['manualResolvedProjectCode'], 'CUS-CODE')
        self.assertEqual(out['manualResolvedProjectName'], 'Proyecto Custom')

    def test_reject_invalid_transaction_id(self):
        fake_db = self._fake_db()
        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as exc:
                main.resolve_admin_suspicious_project_resolution(
                    'not-an-oid',
                    {'resolve_to': 'document'},
                    user={'username': 'admin'},
                )

        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn('transactionId', str(exc.exception.detail))

    def test_reject_when_target_project_cannot_be_derived(self):
        broken_tx = {
            **self.base_tx,
            'sap': {
                'movementType': 'egreso',
                'isProjectResolutionSuspicious': True,
                'documentProjectCode': None,
                'documentProjectName': None,
                'paymentProjectCode': None,
                'paymentProjectName': None,
            },
        }
        fake_db = self._fake_db(tx_doc=broken_tx)
        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as exc:
                main.resolve_admin_suspicious_project_resolution(
                    str(self.tx_id),
                    {'resolve_to': 'document'},
                    user={'username': 'admin'},
                )

        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn('Could not derive resolved project', str(exc.exception.detail))

    def test_pending_and_resolved_filter_supports_objectid_or_string(self):
        pending_query = main.build_suspicious_project_resolutions_query(status='pending')
        resolved_query = main.build_suspicious_project_resolutions_query(status='resolved')

        pending_expr = pending_query['$expr']['$eq']
        resolved_expr = resolved_query['$expr']['$gt']
        self.assertEqual(pending_expr[1], 0)
        self.assertEqual(resolved_expr[1], 0)


if __name__ == '__main__':
    unittest.main()
