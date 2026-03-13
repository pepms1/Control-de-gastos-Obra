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


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, _spec):
        return self

    def skip(self, count):
        self.docs = self.docs[count:]
        return self

    def limit(self, count):
        self.docs = self.docs[:count]
        return self

    def __iter__(self):
        return iter(self.docs)


class FakeTransactions:
    def __init__(self, docs):
        self.docs = {str(doc['_id']): doc for doc in docs}
        self.last_update = None

    def _matches_query(self, doc, query):
        target_id = str(query.get('_id')) if query.get('_id') is not None else None
        if target_id and str(doc.get('_id')) != target_id:
            return False

        source = query.get('source')
        if source and doc.get('source') != source:
            return False

        sap_doc = doc.get('sap') if isinstance(doc.get('sap'), dict) else {}
        if query.get('sap.isProjectResolutionSuspicious') is True and not sap_doc.get('isProjectResolutionSuspicious'):
            return False

        manual_id = str(sap_doc.get('manualResolvedProjectId') or '').strip()
        if '$expr' in query and '$eq' in query['$expr'] and manual_id:
            return False
        if '$expr' in query and '$gt' in query['$expr'] and not manual_id:
            return False

        return True

    def find_one(self, query, projection=None):
        for doc in self.docs.values():
            if self._matches_query(doc, query):
                return doc
        return None

    def find(self, query, projection=None):
        out = [doc for doc in self.docs.values() if self._matches_query(doc, query)]
        return FakeCursor(out)

    def count_documents(self, query):
        return len([doc for doc in self.docs.values() if self._matches_query(doc, query)])

    def update_one(self, query, update):
        target_id = str(query.get('_id')) if query.get('_id') is not None else None
        doc = self.docs.get(target_id)
        if not doc:
            return None
        self.last_update = {'query': query, 'update': update}
        sets = update.get('$set', {})
        for key, value in sets.items():
            if '.' in key:
                first, second = key.split('.', 1)
                root = doc.setdefault(first, {})
                root[second] = value
            else:
                doc[key] = value

        unsets = update.get('$unset', {})
        for key in unsets.keys():
            if '.' in key:
                first, second = key.split('.', 1)
                root = doc.get(first)
                if isinstance(root, dict):
                    root.pop(second, None)
            else:
                doc.pop(key, None)
        return None


class FakeProjects:
    def __init__(self, docs):
        self.docs = {str(doc['_id']): doc for doc in docs}

    def find_one(self, query, projection=None):
        target_id = query.get('_id')
        if target_id is None:
            return None
        return self.docs.get(str(target_id))

    def find(self, query=None, projection=None):
        return list(self.docs.values())


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

    def _fake_db(self, tx_doc=None, projects=None):
        tx = tx_doc or self.base_tx
        project_docs = projects or [
            {
                '_id': self.project_id,
                'name': 'Proyecto Custom',
                'sap': {'projectCode': 'CUS-CODE'},
            },
            {
                '_id': ObjectId(),
                'name': 'Proyecto Documento',
                'sap': {'projectCode': 'DOC-CODE'},
            },
            {
                '_id': ObjectId(),
                'name': 'Proyecto Pago',
                'sap': {'projectCode': 'PAY-CODE'},
            },
        ]
        return type(
            'FakeDb',
            (),
            {
                'transactions': FakeTransactions([tx]),
                'projects': FakeProjects(project_docs),
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
        self.assertTrue(out_document['manualResolvedProjectId'])
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
        self.assertIn('Could not derive resolved project id', str(exc.exception.detail))

    def test_reject_partial_manual_resolution_payload_and_persist_nothing(self):
        fake_db = self._fake_db()
        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as exc:
                main.resolve_admin_suspicious_project_resolution(
                    str(self.tx_id),
                    {'resolve_to': 'custom', 'project_name': 'Proyecto Inexistente'},
                    user={'username': 'admin'},
                )

        self.assertEqual(exc.exception.status_code, 400)
        self.assertIsNone(fake_db.transactions.last_update)

    def test_pending_and_resolved_filter_supports_manual_project_id_only(self):
        pending_query = main.build_suspicious_project_resolutions_query(status='pending')
        resolved_query = main.build_suspicious_project_resolutions_query(status='resolved')

        self.assertNotIn('source', pending_query)
        self.assertNotIn('source', resolved_query)
        pending_expr = pending_query['$expr']['$eq']
        resolved_expr = resolved_query['$expr']['$gt']
        self.assertEqual(pending_expr[1], 0)
        self.assertEqual(resolved_expr[1], 0)

    def test_pending_list_includes_suspicious_sap_source_without_manual_resolution(self):
        tx_pending = {
            '_id': ObjectId(),
            'source': 'sap',
            'sourceDb': 'SBO_GMDI',
            'sourceSbo': 'SBO_GMDI',
            'sap': {
                'sourceDb': 'SBO_GMDI',
                'sourceSbo': 'SBO_GMDI',
                'movementType': 'ingreso',
                'isProjectResolutionSuspicious': True,
            },
        }
        tx_resolved = {
            '_id': ObjectId(),
            'source': 'sap',
            'sap': {
                'movementType': 'egreso',
                'isProjectResolutionSuspicious': True,
                'manualResolvedProjectId': str(ObjectId()),
            },
        }

        fake_db = type('FakeDb', (), {'transactions': FakeTransactions([tx_pending, tx_resolved]), 'projects': FakeProjects([])})()

        with patch.object(main, 'db', fake_db):
            pending = main.list_admin_suspicious_project_resolutions(status='pending', _={'username': 'admin'})

        pending_ids = {item['id'] for item in pending['items']}
        self.assertIn(str(tx_pending['_id']), pending_ids)
        self.assertNotIn(str(tx_resolved['_id']), pending_ids)
        first_row = pending['items'][0]
        self.assertEqual(first_row['id'], str(tx_pending['_id']))
        self.assertEqual(first_row['transactionId'], str(tx_pending['_id']))
        self.assertEqual(first_row['_id'], str(tx_pending['_id']))

    def test_resolve_with_non_matching_valid_object_id_returns_404_with_received_id(self):
        payment_num_like_oid = str(ObjectId())
        fake_db = self._fake_db(
            tx_doc={
                **self.base_tx,
                'sap': {
                    **self.base_tx['sap'],
                    'paymentNum': payment_num_like_oid,
                },
            }
        )

        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as exc:
                main.resolve_admin_suspicious_project_resolution(
                    payment_num_like_oid,
                    {'resolve_to': 'document'},
                    user={'username': 'admin'},
                )

        self.assertEqual(exc.exception.status_code, 404)
        self.assertEqual(exc.exception.detail['message'], 'Transaction not found')
        self.assertEqual(exc.exception.detail['transactionId'], payment_num_like_oid)

    def test_resolved_list_includes_only_rows_with_non_empty_manual_resolved_project_id(self):
        tx_pending = {
            '_id': ObjectId(),
            'source': 'sap',
            'sap': {
                'movementType': 'egreso',
                'isProjectResolutionSuspicious': True,
                'manualResolvedProjectId': '',
            },
        }
        tx_resolved = {
            '_id': ObjectId(),
            'source': 'sap',
            'sap': {
                'movementType': 'egreso',
                'isProjectResolutionSuspicious': True,
                'manualResolvedProjectId': str(ObjectId()),
            },
        }

        fake_db = type('FakeDb', (), {'transactions': FakeTransactions([tx_pending, tx_resolved]), 'projects': FakeProjects([])})()

        with patch.object(main, 'db', fake_db):
            resolved = main.list_admin_suspicious_project_resolutions(status='resolved', _={'username': 'admin'})

        resolved_ids = {item['id'] for item in resolved['items']}

        self.assertIn(str(tx_resolved['_id']), resolved_ids)
        self.assertNotIn(str(tx_pending['_id']), resolved_ids)

    def test_repair_backfills_unique_mapping_and_clears_unmapped_partial_fields(self):
        tx_fix_id = ObjectId()
        tx_clear_id = ObjectId()
        tx_fix = {
            '_id': tx_fix_id,
            'source': 'sap-sbo',
            'sap': {
                'isProjectResolutionSuspicious': True,
                'manualResolvedProjectId': None,
                'manualResolvedProjectCode': 'DOC-CODE',
                'manualResolvedProjectName': 'Proyecto Documento',
                'manualResolvedAt': '2025-01-01T00:00:00+00:00',
            },
        }
        tx_clear = {
            '_id': tx_clear_id,
            'source': 'sap-sbo',
            'sap': {
                'isProjectResolutionSuspicious': True,
                'manualResolvedProjectId': '',
                'manualResolvedProjectName': 'Proyecto Ambiguo',
                'manualResolvedAt': '2025-01-01T00:00:00+00:00',
                'manualResolvedBy': 'admin',
                'manualResolutionReason': 'legacy',
            },
        }

        ambiguous_name = 'Proyecto Ambiguo'
        project_docs = [
            {'_id': ObjectId(), 'name': 'Proyecto Documento', 'sap': {'projectCode': 'DOC-CODE'}},
            {'_id': ObjectId(), 'name': ambiguous_name, 'sap': {'projectCode': 'AMB-1'}},
            {'_id': ObjectId(), 'name': ambiguous_name, 'sap': {'projectCode': 'AMB-2'}},
        ]
        fake_db = type('FakeDb', (), {'transactions': FakeTransactions([tx_fix, tx_clear]), 'projects': FakeProjects(project_docs)})()

        with patch.object(main, 'db', fake_db):
            result = main.repair_partial_suspicious_project_resolutions()

        self.assertEqual(result['scanned'], 2)
        self.assertEqual(result['fixed'], 1)
        self.assertEqual(result['cleared'], 1)
        self.assertTrue(str(fake_db.transactions.docs[str(tx_fix_id)]['sap'].get('manualResolvedProjectId')).strip())
        self.assertIsNone(fake_db.transactions.docs[str(tx_clear_id)]['sap'].get('manualResolvedProjectId'))
        self.assertIsNone(fake_db.transactions.docs[str(tx_clear_id)]['sap'].get('manualResolvedProjectName'))


if __name__ == '__main__':
    unittest.main()
