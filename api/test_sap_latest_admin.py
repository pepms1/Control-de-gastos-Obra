import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class SapLatestImportCoreTests(unittest.TestCase):
    def test_handle_sap_latest_import_calls_run_s3_latest(self):
        expected = {'iva': {'already_imported': True}, 'efectivo': {'already_imported': False}}
        with patch.object(main, 'run_s3_latest_sap_import', return_value=expected) as run_mock, patch.object(
            main, 'notify_sap_latest_import_success'
        ) as notify_ok:
            result = main.handle_sap_latest_import(project='Demo', source='sap-latest-cron')

        self.assertEqual(result, expected)
        run_mock.assert_called_once_with(project='Demo', force=0, mode='upsert', source='sap-latest-cron')
        notify_ok.assert_called_once_with(project='Demo', result=expected)


class SapLatestAdminEndpointTests(unittest.TestCase):
    def test_admin_endpoint_calls_core(self):
        class FakeInsertResult:
            inserted_id = 'audit-1'

        class FakeAdminActions:
            def insert_one(self, _doc):
                return FakeInsertResult()

            def update_one(self, *_args, **_kwargs):
                return None

        class FakeProjects:
            def find_one(self, query, _projection):
                if str(query.get('_id')):
                    return {'name': 'Proyecto Demo'}
                return None

        fake_db = type('FakeDb', (), {'projects': FakeProjects(), 'adminActions': FakeAdminActions()})()
        project_id = '507f1f77bcf86cd799439011'

        with patch.object(main, 'db', fake_db), patch.object(
            main,
            'handle_sap_latest_import',
            return_value={'iva': {'already_imported': True}, 'efectivo': {'already_imported': True}},
        ) as core_mock:
            main.sap_latest_admin_locks.clear()
            main.sap_latest_admin_last_request_at.clear()
            result = main.admin_import_sap_latest(
                {'projectId': project_id, 'sources': ['IVA']},
                user={'username': 'admin', 'displayName': 'Admin'},
            )

        self.assertIn('iva', result)
        self.assertIn('efectivo', result)
        core_mock.assert_called_once()


if __name__ == '__main__':
    unittest.main()


class SapManualImportGuardrailTests(unittest.TestCase):
    def test_guardrail_blocks_filename_mismatch(self):
        class FakeProjects:
            def find_one(self, query, _projection):
                if str(query.get('_id')):
                    return {'name': 'CALDERON', 'slug': 'calderon', 'sap': {'projectCode': 'CALDERON'}}
                return None

            def find(self, _query, _projection):
                return [{'slug': 'calderon'}, {'slug': 'horacio1027'}]

        fake_db = type('FakeDb', (), {'projects': FakeProjects()})()
        with patch.object(main, 'db', fake_db):
            result = main.evaluate_manual_import_project_guardrail(
                project_id='507f1f77bcf86cd799439011',
                file_name='horacio1027_2026-01.csv',
                file_bytes=b'PagoNum,FechaPago,CardCode,Beneficiario,Moneda,TotalPago,ConceptoPago,FacturaProveedorNum,FechaFactura,MontoAplicado\n',
            )

        self.assertTrue(result.get('shouldBlock'))
        self.assertEqual(result.get('mismatchLevel'), 'moderate')
        self.assertEqual(result.get('detected', {}).get('byFilename'), 'horacio1027')

    def test_guardrail_strong_content_mismatch(self):
        class FakeProjects:
            def find_one(self, query, _projection):
                if str(query.get('_id')):
                    return {'name': 'CALDERON', 'slug': 'calderon', 'sap': {'projectCode': 'CALDERON'}}
                return None

            def find(self, _query, _projection):
                return [{'slug': 'calderon'}, {'slug': 'horacio1027'}]

        fake_db = type('FakeDb', (), {'projects': FakeProjects()})()
        rows = ['Pago_PrjCode,Other'] + [f'HORACIO 1027,{i}' for i in range(15)]
        content = ('\n'.join(rows)).encode('utf-8')

        with patch.object(main, 'db', fake_db):
            result = main.evaluate_manual_import_project_guardrail(
                project_id='507f1f77bcf86cd799439011',
                file_name='sap_export.csv',
                file_bytes=content,
            )

        self.assertTrue(result.get('shouldBlock'))
        self.assertEqual(result.get('mismatchLevel'), 'strong')
        self.assertEqual(result.get('detected', {}).get('byContentTopCode'), 'HORACIO 1027')

    def test_guardrail_no_block_on_insufficient_evidence(self):
        class FakeProjects:
            def find_one(self, query, _projection):
                if str(query.get('_id')):
                    return {'name': 'CALDERON', 'slug': 'calderon', 'sap': {'projectCode': 'CALDERON'}}
                return None

            def find(self, _query, _projection):
                return [{'slug': 'calderon'}, {'slug': 'horacio1027'}]

        fake_db = type('FakeDb', (), {'projects': FakeProjects()})()
        rows = ['Pago_PrjCode,Other'] + [f'HORACIO 1027,{i}' for i in range(5)]
        content = ('\n'.join(rows)).encode('utf-8')

        with patch.object(main, 'db', fake_db):
            result = main.evaluate_manual_import_project_guardrail(
                project_id='507f1f77bcf86cd799439011',
                file_name='export_neutro.csv',
                file_bytes=content,
            )

        self.assertFalse(result.get('shouldBlock'))
        self.assertTrue(result.get('detected', {}).get('insufficientEvidence'))


class SapMovementsBySboIdempotencyTests(unittest.TestCase):
    class _FakeImportRuns:
        def __init__(self, docs):
            self.docs = docs
            self._next_id = 100

        def _matches(self, doc, query):
            return all(doc.get(k) == v for k, v in query.items())

        def find_one(self, query):
            for doc in self.docs:
                if self._matches(doc, query):
                    return doc
            return None

        def update_one(self, query, update):
            doc = self.find_one(query)
            if not doc:
                return None
            doc.update(update.get('$set', {}))
            return None

        def insert_one(self, doc):
            new_doc = dict(doc)
            new_doc['_id'] = f"run-{self._next_id}"
            self._next_id += 1
            self.docs.append(new_doc)

            class _Res:
                inserted_id = new_doc['_id']

            return _Res()

    class _FakeProjects:
        def find(self, *_args, **_kwargs):
            return []

    class _FakeDb:
        def __init__(self, import_runs):
            self.importRuns = import_runs
            self.projects = SapMovementsBySboIdempotencyTests._FakeProjects()
            self.unmatched_projects = type('X', (), {'update_one': lambda *args, **kwargs: None})()
            self.vendors = type('X', (), {'bulk_write': lambda *args, **kwargs: None})()
            self.transactions = type('X', (), {'update_one': lambda *args, **kwargs: None})()

    def test_latest_uses_sha_and_not_only_import_key_for_already_imported(self):
        old_hash = 'a' * 64
        import_runs = self._FakeImportRuns(
            [
                {
                    '_id': 'run-1',
                    'source': 'sap-movements-by-sbo',
                    'sourceSbo': 'SBO_TEST',
                    'mode': 'latest',
                    'importKey': 'sap-movements-by-sbo:SBO_TEST:latest:exports-v2/SBO_TEST/latest_movements.csv',
                    'sha256': old_hash,
                    'status': 'ok',
                }
            ]
        )
        fake_db = self._FakeDb(import_runs)

        with patch.object(main, 'db', fake_db), patch.object(
            main, 'downloadFromS3Object', return_value=b'movement_type,source_type\n'
        ):
            result = main.import_sap_movements_by_sbo(sbo='SBO_TEST', mode='latest', force=0)

        self.assertNotIn('already_imported', result)
        self.assertEqual(result.get('status'), 'ok')

    def test_latest_returns_already_imported_when_sha_matches(self):
        content = b'movement_type,source_type\n'
        same_hash = main.sha256(content).hexdigest()
        import_runs = self._FakeImportRuns(
            [
                {
                    '_id': 'run-2',
                    'source': 'sap-movements-by-sbo',
                    'sourceSbo': 'SBO_TEST',
                    'mode': 'latest',
                    'importKey': 'sap-movements-by-sbo:SBO_TEST:latest:exports-v2/SBO_TEST/latest_movements.csv',
                    'sha256': same_hash,
                    'status': 'ok',
                }
            ]
        )
        fake_db = self._FakeDb(import_runs)

        with patch.object(main, 'db', fake_db), patch.object(main, 'downloadFromS3Object', return_value=content):
            result = main.import_sap_movements_by_sbo(sbo='SBO_TEST', mode='latest', force=0)

        self.assertTrue(result.get('already_imported'))
        self.assertEqual(result.get('importRunId'), 'run-2')
