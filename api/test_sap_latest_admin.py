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


class LatestImportsEndpointTests(unittest.TestCase):
    class _FakeCursor:
        def __init__(self, docs):
            self.docs = docs

        def sort(self, *_args, **_kwargs):
            return self

        def limit(self, *_args, **_kwargs):
            return self

        def __iter__(self):
            return iter(self.docs)

    class _FakeTransactions:
        def __init__(self, docs):
            self.docs = docs

        def find(self, *_args, **_kwargs):
            return LatestImportsEndpointTests._FakeCursor(self.docs)

    class _FakeDb:
        def __init__(self, docs):
            self.transactions = LatestImportsEndpointTests._FakeTransactions(docs)

    def test_latest_imports_includes_concept_and_monto(self):
        fake_docs = [
            {
                '_id': 'tx-1',
                'created_at': '2026-01-01T12:00:00+00:00',
                'date': '2026-01-01',
                'description': 'Pago de proveedor',
                'amount': 1250.5,
                'supplierName': 'Proveedor Demo',
                'sap': {'documentProjectName': 'Obra Norte', 'sourceSbo': 'SBO_A'},
            }
        ]
        with patch.object(main, 'db', self._FakeDb(fake_docs)):
            result = main.list_admin_latest_imports(days=7, limit=100, _={'role': 'SUPERADMIN'})

        self.assertEqual(result['total'], 1)
        row = result['items'][0]
        self.assertEqual(row['concept'], 'Pago de proveedor')
        self.assertEqual(row['monto'], 1250.5)

    def test_latest_imports_filters_and_sorts_by_document_date(self):
        class CapturingCursor(self._FakeCursor):
            def __init__(self, docs):
                super().__init__(docs)
                self.sort_args = None

            def sort(self, args, **_kwargs):
                self.sort_args = args
                return self

        class CapturingTransactions(self._FakeTransactions):
            def __init__(self, docs):
                super().__init__(docs)
                self.last_query = None
                self.last_projection = None
                self.last_cursor = None

            def find(self, query, projection):
                self.last_query = query
                self.last_projection = projection
                self.last_cursor = CapturingCursor(self.docs)
                return self.last_cursor

        tx = CapturingTransactions([])
        fake_db = type('FakeDb', (), {'transactions': tx})()

        with patch.object(main, 'db', fake_db):
            main.list_admin_latest_imports(days=7, limit=100, _={'role': 'SUPERADMIN'})

        self.assertIn('$or', tx.last_query)
        self.assertEqual(tx.last_cursor.sort_args[0][0], 'date')
        self.assertEqual(tx.last_cursor.sort_args[0][1], -1)



class TelegramAdminBootstrapTests(unittest.TestCase):
    class _FakeTelegramUsers:
        def __init__(self):
            self.calls = []

        def update_one(self, query, update, upsert=False):
            self.calls.append({'query': query, 'update': update, 'upsert': upsert})
            return None

    def test_ensure_telegram_admin_user_upserts_approved_admin_chat(self):
        fake_telegram_users = self._FakeTelegramUsers()
        fake_db = type('FakeDb', (), {'telegram_users': fake_telegram_users})()

        with patch.object(main, 'db', fake_db), patch.object(main, 'get_telegram_admin_chat_id', return_value='13875693'):
            main.ensure_telegram_admin_user()

        self.assertEqual(len(fake_telegram_users.calls), 1)
        call = fake_telegram_users.calls[0]
        self.assertEqual(call['query'], {'chat_id': '13875693'})
        self.assertTrue(call['upsert'])
        update_set = call['update']['$set']
        self.assertEqual(update_set['status'], 'approved')
        self.assertTrue(update_set['approved'])
        self.assertEqual(update_set['chat_id'], '13875693')
        self.assertTrue(update_set['is_admin'])
        self.assertEqual(call['update']['$setOnInsert'], {'requested_at': update_set['updated_at']})

    def test_ensure_telegram_admin_user_skips_when_chat_id_missing(self):
        fake_telegram_users = self._FakeTelegramUsers()
        fake_db = type('FakeDb', (), {'telegram_users': fake_telegram_users})()

        with patch.object(main, 'db', fake_db), patch.object(main, 'get_telegram_admin_chat_id', return_value=''):
            main.ensure_telegram_admin_user()

        self.assertEqual(fake_telegram_users.calls, [])


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


class SapMovementsTelegramNotificationTests(unittest.TestCase):
    def test_success_message_format_contains_required_fields(self):
        message = main._build_sap_movements_success_message(
            sbo='SBO_GMDI',
            mode='latest',
            trigger_source='cron',
            actor='scheduler',
            result={
                'rowsTotal': 12,
                'rowsOk': 11,
                'imported': 7,
                'updated': 4,
                'unmatched': 1,
                'importRunId': 'run-123',
            },
        )

        self.assertIn('✅ SAP import', message)
        self.assertIn('SBO: SBO_GMDI', message)
        self.assertIn('Modo: latest', message)
        self.assertIn('Origen: cron', message)
        self.assertIn('Actor: scheduler', message)
        self.assertIn('Rows total: 12', message)
        self.assertIn('Rows ok: 11', message)
        self.assertIn('Imported: 7', message)
        self.assertIn('Updated: 4', message)
        self.assertIn('Unmatched: 1', message)
        self.assertIn('ImportRunId: run-123', message)

    def test_cron_endpoint_notifies_result_with_trigger_source(self):
        with patch.object(main, 'import_sap_movements_by_sbo', return_value={'already_imported': True, 'importRunId': 'run-9'}) as import_mock, patch.object(
            main, 'notify_sap_movements_by_sbo_result'
        ) as notify_mock:
            result = main.cron_import_sap_movements_by_sbo(
                sbo='SBO_TEST',
                mode='latest',
                force=0,
                x_trigger_source='frontend',
                user={'username': 'admin-user'},
            )

        self.assertTrue(result.get('already_imported'))
        import_mock.assert_called_once()
        notify_mock.assert_called_once_with(
            sbo='SBO_TEST',
            mode='latest',
            trigger_source='frontend',
            actor='admin-user',
            result={'already_imported': True, 'importRunId': 'run-9'},
        )

    def test_cron_endpoint_notifies_error_and_reraises(self):
        error = RuntimeError('sap failed')
        with patch.object(main, 'import_sap_movements_by_sbo', side_effect=error), patch.object(
            main, 'notify_sap_movements_by_sbo_error'
        ) as notify_error_mock:
            with self.assertRaises(RuntimeError):
                main.cron_import_sap_movements_by_sbo(
                    sbo='SBO_TEST',
                    mode='latest',
                    force=0,
                    x_trigger_source='cron',
                    user={'displayName': 'Render Cron'},
                )

        notify_error_mock.assert_called_once()
        kwargs = notify_error_mock.call_args.kwargs
        self.assertEqual(kwargs.get('trigger_source'), 'cron')
        self.assertEqual(kwargs.get('actor'), 'Render Cron')


class TelegramAdminTestEndpointTests(unittest.TestCase):
    def test_admin_test_endpoint_broadcasts(self):
        with patch.object(main, 'send_telegram_broadcast', return_value={'total': 2, 'sent': 2, 'failed': 0}) as broadcast_mock:
            result = main.admin_test_telegram(message='hola', user={'username': 'root'})

        broadcast_mock.assert_called_once_with('hola')
        self.assertTrue(result.get('ok'))
        self.assertEqual(result.get('sent'), 2)


class SapSboV2ProjectResolutionTests(unittest.TestCase):
    def test_egreso_uses_document_project_as_raw(self):
        row = {
            'document_project_code': 'PBPC',
            'document_project_name': 'PB Y PC INTERIORES',
            'payment_project_code': 'CALD',
            'payment_project_name': 'CALDERON DE LA BARCA',
            'raw_project_code': 'CALD',
            'raw_project_name': 'CALDERON DE LA BARCA',
            'project_resolution_source': 'payment_jdt1',
        }
        fields = main.resolve_sbo_project_fields_from_row(row, 'egreso')
        self.assertEqual(fields['raw_project_code'], 'PBPC')
        self.assertEqual(fields['raw_project_name'], 'PB Y PC INTERIORES')
        self.assertEqual(fields['project_resolution_source'], 'document')

    def test_egreso_diff_document_payment_is_suspicious(self):
        suspicion = main.build_project_resolution_suspicion_fields(
            'egreso',
            {
                'document_project_code': 'PBPC',
                'document_project_name': 'PB Y PC INTERIORES',
                'payment_project_code': 'ROKA',
                'payment_project_name': 'CALDERON DE LA BARCA',
            },
        )
        self.assertTrue(suspicion['isProjectResolutionSuspicious'])
        self.assertEqual(
            suspicion['projectResolutionSuspicionReasons'],
            ['document_project_differs_from_payment_project'],
        )
