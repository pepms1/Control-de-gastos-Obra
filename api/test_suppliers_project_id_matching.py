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


class _Cursor(list):
    def sort(self, *_args, **_kwargs):
        return self


class _SuppliersCollection:
    def __init__(self, docs):
        self.docs = docs

    def find(self, query):
        bucket = query.get('$and', [{}])[0].get('$or', [])
        candidates = []
        for clause in bucket:
            value = clause.get('projectId', {}).get('$in') or clause.get('projectIds', {}).get('$in')
            if value:
                candidates.extend(value)

        matched = []
        for doc in self.docs:
            doc_project = doc.get('projectId')
            doc_projects = doc.get('projectIds') or []
            if doc_project in candidates or any(item in candidates for item in (doc_projects if isinstance(doc_projects, list) else [doc_projects])):
                matched.append(doc)
        return _Cursor(matched)


class SuppliersProjectIdMatchingTests(unittest.TestCase):
    def test_list_suppliers_matches_objectid_project_records(self):
        project_id = str(ObjectId())
        fake_db = type(
            'FakeDb',
            (),
            {
                'suppliers': _SuppliersCollection([
                    {'_id': ObjectId(), 'name': 'Proveedor A', 'projectId': ObjectId(project_id)},
                    {'_id': ObjectId(), 'name': 'Proveedor B', 'projectId': 'other-project'},
                ])
            },
        )()

        with patch.object(main, 'db', fake_db), patch.object(main, 'get_active_project_id', return_value=project_id), patch.object(
            main, 'can_access_project', return_value=True
        ):
            rows = main.list_suppliers(user={'role': 'ADMIN'}, request=object())

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get('name'), 'Proveedor A')


if __name__ == '__main__':
    unittest.main()
