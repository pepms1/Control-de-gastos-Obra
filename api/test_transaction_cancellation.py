import os
import sys
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from bson import ObjectId

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or []

    def _get_nested(self, doc, key):
        value = doc
        for part in key.split('.'):
            if not isinstance(value, dict) or part not in value:
                return None
            value = value.get(part)
        return value

    def _match(self, doc, query):
        for key, value in query.items():
            if key == '$or':
                if not any(self._match(doc, clause) for clause in value):
                    return False
                continue
            if isinstance(value, dict) and '$ne' in value:
                if self._get_nested(doc, key) == value['$ne']:
                    return False
                continue
            if self._get_nested(doc, key) != value:
                return False
        return True

    def _apply_set(self, doc, set_doc):
        for key, value in set_doc.items():
            if '.' not in key:
                doc[key] = value
                continue
            target = doc
            parts = key.split('.')
            for part in parts[:-1]:
                if part not in target or not isinstance(target.get(part), dict):
                    target[part] = {}
                target = target[part]
            target[parts[-1]] = value

    def find_one(self, query, projection=None):
        for doc in self.docs:
            if self._match(doc, query):
                result = deepcopy(doc)
                if not projection:
                    return result
                projected = {'_id': result.get('_id')}
                for key, include in projection.items():
                    if include and key != '_id':
                        projected[key] = self._get_nested(result, key)
                return projected
        return None

    def update_one(self, query, update, upsert=False):
        for idx, doc in enumerate(self.docs):
            if self._match(doc, query):
                if '$set' in update:
                    self._apply_set(self.docs[idx], update['$set'])
                return
        if upsert:
            new_doc = {}
            if '$setOnInsert' in update:
                self._apply_set(new_doc, update['$setOnInsert'])
            if '$set' in update:
                self._apply_set(new_doc, update['$set'])
            if '_id' not in new_doc:
                new_doc['_id'] = ObjectId()
            self.docs.append(new_doc)

    def update_many(self, query, update):
        for idx, doc in enumerate(self.docs):
            if self._match(doc, query) and '$set' in update:
                self._apply_set(self.docs[idx], update['$set'])


class _FakeDb:
    def __init__(self, tx_docs):
        self.transactions = _FakeCollection(tx_docs)
        self.transaction_cancellation_overrides = _FakeCollection([])

    def __getitem__(self, name):
        if name == main.TRANSACTION_CANCELLATION_OVERRIDES_COLLECTION:
            return self.transaction_cancellation_overrides
        raise KeyError(name)


class TransactionCancellationTests(unittest.TestCase):
    def test_build_transactions_query_excludes_cancelled_by_default(self):
        query = main.build_transactions_query(type_value='EXPENSE')
        self.assertIn('$and', query)
        self.assertIn('isCancelled', str(query))

    def test_build_transactions_query_allows_include_cancelled(self):
        query = main.build_transactions_query(type_value='EXPENSE', include_cancelled=True)
        self.assertIsNone(query.get('isCancelled'))

    def test_resolve_cancellation_state_precedence(self):
        evaluated_at = '2026-01-01T00:00:00+00:00'
        state_override = main.resolve_cancellation_state(
            existing_transaction={'isCancelled': False},
            override={'isActive': True, 'reason': 'Manual'},
            source_cancelled_flag=False,
            evaluated_at=evaluated_at,
        )
        self.assertTrue(state_override['isCancelled'])
        self.assertEqual(state_override['cancellation']['source'], 'manual')

        state_backfill = main.resolve_cancellation_state(
            existing_transaction={'isCancelled': True, 'cancellation': {'source': 'manual'}},
            override=None,
            source_cancelled_flag=None,
            evaluated_at=evaluated_at,
        )
        self.assertTrue(state_backfill['isCancelled'])

        state_upstream = main.resolve_cancellation_state(
            existing_transaction={'isCancelled': False},
            override=None,
            source_cancelled_flag=True,
            evaluated_at=evaluated_at,
        )
        self.assertTrue(state_upstream['isCancelled'])
        self.assertEqual(state_upstream['cancellation']['source'], 'sap')

    def test_cancel_and_restore_admin_transaction(self):
        tx_id = ObjectId()
        tx_doc = {
            '_id': tx_id,
            'projectId': 'p1',
            'source': 'sap-sbo',
            'sourceDb': 'IVA',
            'sourceSbo': 'SBO_GMDI',
            'type': 'EXPENSE',
            'dedupeKey': 'dk-1',
            'sap': {
                'paymentDocEntry': '11',
                'paymentNum': '22',
                'invoiceDocEntry': '33',
                'invoiceNum': '44',
                'externalDocNum': '55',
            },
            'isCancelled': False,
        }
        fake_db = _FakeDb([tx_doc])
        admin = {'role': 'ADMIN', 'username': 'admin', 'displayName': 'Admin'}

        with patch.object(main, 'db', fake_db):
            cancelled = main.cancel_transaction_admin(str(tx_id), {'reason': 'Cancelado en SAP'}, user=admin)
            self.assertTrue(cancelled['isCancelled'])
            override = fake_db.transaction_cancellation_overrides.find_one({'dedupeKey': 'dk-1'})
            self.assertTrue(override['isActive'])

            restored = main.restore_transaction_admin(str(tx_id), {'notes': 'restaurada'}, user=admin)
            self.assertFalse(restored['isCancelled'])
            override = fake_db.transaction_cancellation_overrides.find_one({'dedupeKey': 'dk-1'})
            self.assertFalse(override['isActive'])


if __name__ == '__main__':
    unittest.main()
