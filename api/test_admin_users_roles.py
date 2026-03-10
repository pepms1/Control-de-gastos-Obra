import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from bson import ObjectId

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class FakeUsersCollection:
    def __init__(self, docs):
        self.docs = {str(doc['_id']): dict(doc) for doc in docs}

    def find(self, _query, _projection):
        return [dict(doc) for doc in self.docs.values()]

    def find_one(self, query):
        key = str(query.get('_id'))
        doc = self.docs.get(key)
        return dict(doc) if doc else None

    def find_one_and_update(self, query, update, return_document=None):
        key = str(query.get('_id'))
        doc = self.docs.get(key)
        if not doc:
            return None
        for k, v in update.get('$set', {}).items():
            doc[k] = v
        self.docs[key] = doc
        return dict(doc)


class AdminUserRoleUpdateTests(unittest.TestCase):
    def test_blocks_degrading_last_superadmin(self):
        user_id = ObjectId()
        fake_users = FakeUsersCollection([
            {'_id': user_id, 'username': 'root', 'role': 'SUPERADMIN', 'roleVersion': 2, 'allowedProjectIds': []}
        ])
        fake_db = type('FakeDb', (), {'users': fake_users})()

        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as ctx:
                main.update_admin_user(str(user_id), {'role': 'ADMIN'}, _={'role': 'SUPERADMIN'})

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, 'No se puede degradar al último SUPERADMIN')

    def test_allows_degrading_when_another_superadmin_exists(self):
        user_id = ObjectId()
        backup_id = ObjectId()
        fake_users = FakeUsersCollection([
            {'_id': user_id, 'username': 'root', 'role': 'SUPERADMIN', 'roleVersion': 2, 'allowedProjectIds': []},
            {'_id': backup_id, 'username': 'root2', 'role': 'SUPERADMIN', 'roleVersion': 2, 'allowedProjectIds': []},
        ])
        fake_db = type('FakeDb', (), {'users': fake_users})()

        with patch.object(main, 'db', fake_db):
            updated = main.update_admin_user(str(user_id), {'role': 'ADMIN'}, _={'role': 'SUPERADMIN'})

        self.assertEqual(updated.get('role'), 'ADMIN')


if __name__ == '__main__':
    unittest.main()
