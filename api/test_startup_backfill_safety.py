import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from bson import ObjectId

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class FakeTransactions:
    def __init__(self, docs):
        self.docs = docs
        self.bulk_write_calls = 0

    def find(self, _query, _projection=None):
        return list(self.docs)

    def find_one(self, query, projection=None):
        ne_id = ((query.get('_id') or {}).get('$ne'))
        for doc in self.docs:
            if ne_id is not None and doc.get('_id') == ne_id:
                continue
            if doc.get('projectId') != query.get('projectId'):
                continue
            if doc.get('source') != query.get('source'):
                continue
            if doc.get('sourceDb') != query.get('sourceDb'):
                continue
            sap_doc = doc.get('sap') if isinstance(doc.get('sap'), dict) else {}
            if sap_doc.get('pagoNum', '') != query.get('sap.pagoNum'):
                continue
            if sap_doc.get('facturaNum', '') != query.get('sap.facturaNum'):
                continue
            if sap_doc.get('montoAplicadoCents') != query.get('sap.montoAplicadoCents'):
                continue
            return {'_id': doc.get('_id')}
        return None

    def bulk_write(self, _ops, ordered=False):
        self.bulk_write_calls += 1
        class Result:
            modified_count = 0
            upserted_count = 0
        return Result()


class FakeDb:
    def __init__(self, transactions):
        self.transactions = transactions


class StartupBackfillSafetyTests(unittest.TestCase):
    def test_backfill_skips_collision_for_legacy_blank_pago_factura_rows(self):
        project_id = ObjectId()
        amount = 123.45
        cents = 12345

        doc_to_update = {
            '_id': ObjectId(),
            'projectId': project_id,
            'source': 'sap-sbo',
            'sourceDb': 'SBO_RAFAEL',
            'amount': amount,
            'sap': {
                'pagoNum': '',
                'facturaNum': '',
                'montoAplicado': amount,
                'montoAplicadoCents': cents,
            },
        }
        conflicting_doc = {
            '_id': ObjectId(),
            'projectId': project_id,
            'source': 'sap',
            'sourceDb': 'SBO_RAFAEL',
            'amount': amount,
            'sap': {
                'pagoNum': '',
                'facturaNum': '',
                'montoAplicado': amount,
                'montoAplicadoCents': cents,
            },
        }

        fake_transactions = FakeTransactions([doc_to_update, conflicting_doc])
        fake_db = FakeDb(fake_transactions)

        with patch.object(main, 'db', fake_db):
            result = main.backfill_sap_transactions_metadata()

        self.assertEqual(result.get('skippedCollisions'), 1)
        self.assertEqual(result.get('updated'), 0)
        self.assertEqual(fake_transactions.bulk_write_calls, 0)


if __name__ == '__main__':
    unittest.main()
