import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bson import ObjectId

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def _matches(self, doc, query):
        if not query:
            return True
        for key, condition in query.items():
            value = doc.get(key)
            if isinstance(condition, dict):
                if '$ne' in condition and value == condition['$ne']:
                    return False
                if '$exists' in condition:
                    exists = key in doc
                    if bool(condition['$exists']) != exists:
                        return False
                if '$nin' in condition and value in condition['$nin']:
                    return False
            else:
                if value != condition:
                    return False
        return True

    def find(self, query=None, projection=None):
        rows = [doc for doc in self.docs if self._matches(doc, query or {})]
        if projection:
            projected = []
            for row in rows:
                item = {}
                for field, include in projection.items():
                    if include and field in row:
                        item[field] = row[field]
                if '_id' in row:
                    item['_id'] = row['_id']
                projected.append(item)
            return projected
        return rows

    def update_one(self, query, update):
        target_id = query.get('_id')
        for doc in self.docs:
            if doc.get('_id') == target_id:
                if '$set' in update:
                    doc.update(update['$set'])
                return

    def insert_one(self, payload):
        doc = dict(payload)
        doc['_id'] = doc.get('_id') or ObjectId()
        self.docs.append(doc)
        return SimpleNamespace(inserted_id=doc['_id'])

    def aggregate(self, _pipeline):
        grouped = set()
        for doc in self.docs:
            for key in ('categoryEffectiveName', 'categoryManualName', 'categoryHintName'):
                value = doc.get(key)
                if isinstance(value, str) and value.strip():
                    grouped.add(value)
        return [{'_id': value} for value in grouped]


class AdminGlobalCategoriesTests(unittest.TestCase):
    def test_catalog_merges_existing_and_global_sources_without_case_duplicates(self):
        categories = FakeCollection([
            {'_id': ObjectId(), 'name': 'Pintura', 'active': False},
            {'_id': ObjectId(), 'name': '  ELECTRICO  ', 'active': True},
        ])
        transactions = FakeCollection([
            {'_id': ObjectId(), 'categoryHintName': 'Plomería'},
            {'_id': ObjectId(), 'categoryManualName': 'pintura'},
            {'_id': ObjectId(), 'categoryEffectiveName': 'Impermeabilización'},
        ])
        rules = FakeCollection([
            {'_id': ObjectId(), 'category2Name': 'Cancelería / Vidrio'},
        ])

        fake_db = SimpleNamespace(
            categories=categories,
            transactions=transactions,
            supplierCategory2Rules=rules,
        )

        with patch.object(main, 'db', fake_db):
            response = main.list_admin_global_categories({})

        names = [row['name'] for row in response]
        normalized = {main.normalize_text_for_matching(name) for name in names}

        self.assertIn('pintura', normalized)
        self.assertIn('electrico', normalized)
        self.assertIn('plomeria', normalized)
        self.assertIn('impermeabilizacion', normalized)
        self.assertIn('canceleria / vidrio', normalized)
        self.assertEqual(len(normalized), len(names))


if __name__ == '__main__':
    unittest.main()
