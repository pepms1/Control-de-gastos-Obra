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
