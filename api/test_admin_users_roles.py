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


class InsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class FakePwdContext:
    def hash(self, value):
        return f"hashed::{value}"

    def verify(self, plain, hashed):
        return hashed == f"hashed::{plain}"


class FakeUsersCollection:
    def __init__(self, docs):
        self.docs = {str(doc['_id']): dict(doc) for doc in docs}

    def find(self, _query, _projection):
        return [dict(doc) for doc in self.docs.values()]

    def find_one(self, query):
        if '_id' in query:
            key = str(query.get('_id'))
            doc = self.docs.get(key)
            return dict(doc) if doc else None
        if 'username' in query:
            username = query.get('username')
            for doc in self.docs.values():
                if doc.get('username') == username:
                    return dict(doc)
        return None

    def find_one_and_update(self, query, update, return_document=None):
        key = str(query.get('_id'))
        doc = self.docs.get(key)
        if not doc:
            return None
        for k, v in update.get('$set', {}).items():
            doc[k] = v
        self.docs[key] = doc
        return dict(doc)

    def insert_one(self, doc):
        _id = ObjectId()
        stored = dict(doc)
        stored['_id'] = _id
        self.docs[str(_id)] = stored
        return InsertResult(_id)


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

    def test_create_user_with_display_name_and_viewer_projects(self):
        fake_users = FakeUsersCollection([])
        fake_db = type('FakeDb', (), {'users': fake_users})()

        payload = {
            'username': 'viewer01',
            'password': 'secret123',
            'displayName': 'Viewer Uno',
            'email': 'viewer@obra.com',
            'role': 'VIEWER',
            'allowedProjectIds': [str(ObjectId()), str(ObjectId())],
        }

        with patch.object(main, 'db', fake_db):
            created = main.create_user(payload, _={'role': 'SUPERADMIN'})

        self.assertEqual(created.get('username'), 'viewer01')
        self.assertEqual(created.get('displayName'), 'Viewer Uno')
        self.assertEqual(created.get('email'), 'viewer@obra.com')
        self.assertEqual(created.get('role'), 'VIEWER')
        self.assertEqual(len(created.get('allowedProjectIds', [])), 2)

    def test_update_admin_user_display_name(self):
        user_id = ObjectId()
        fake_users = FakeUsersCollection([
            {
                '_id': user_id,
                'username': 'admin01',
                'displayName': 'Nombre Viejo',
                'role': 'ADMIN',
                'roleVersion': 2,
                'allowedProjectIds': [],
            }
        ])
        fake_db = type('FakeDb', (), {'users': fake_users})()

        with patch.object(main, 'db', fake_db):
            updated = main.update_admin_user(str(user_id), {'displayName': 'Nombre Nuevo'}, _={'role': 'SUPERADMIN'})

        self.assertEqual(updated.get('displayName'), 'Nombre Nuevo')


class AdminUserPasswordResetTests(unittest.TestCase):
    def test_superadmin_can_reset_password_and_stores_hash(self):
        user_id = ObjectId()
        target_doc = {
            '_id': user_id,
            'username': 'viewer01',
            'password_hash': 'hashed::oldpass123',
            'role': 'VIEWER',
            'roleVersion': 2,
            'allowedProjectIds': [],
        }
        fake_users = FakeUsersCollection([target_doc])
        fake_db = type('FakeDb', (), {'users': fake_users})()
        fake_pwd = FakePwdContext()

        with patch.object(main, 'db', fake_db), patch.object(main, 'pwd_context', fake_pwd), patch.object(main, 'get_env_auth_users', return_value={}):
            response = main.reset_admin_user_password(
                str(user_id),
                {'new_password': 'newpass123'},
                _={'role': 'SUPERADMIN', 'username': 'root'},
            )
            login_response = main.login(main.LoginRequest(username='viewer01', password='newpass123'))

        stored = fake_users.find_one({'_id': user_id})
        self.assertTrue(response.get('ok'))
        self.assertEqual(response.get('userId'), str(user_id))
        self.assertEqual(response.get('username'), 'viewer01')
        self.assertEqual(stored.get('password_hash'), 'hashed::newpass123')
        self.assertNotEqual(stored.get('password_hash'), 'newpass123')
        self.assertEqual(stored.get('passwordResetBy'), 'root')
        self.assertIn('access_token', login_response)

    def test_non_superadmin_cannot_reset_password(self):
        user_id = ObjectId()
        fake_users = FakeUsersCollection([
            {
                '_id': user_id,
                'username': 'viewer01',
                'password_hash': 'hashed::oldpass123',
                'role': 'VIEWER',
                'roleVersion': 2,
                'allowedProjectIds': [],
            }
        ])
        fake_db = type('FakeDb', (), {'users': fake_users})()

        with patch.object(main, 'db', fake_db):
            with self.assertRaises(HTTPException) as ctx:
                main.reset_admin_user_password(
                    str(user_id),
                    {'new_password': 'newpass123'},
                    _={'role': 'ADMIN', 'username': 'admin01'},
                )

        self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(ctx.exception.detail, 'SUPERADMIN role required')


if __name__ == '__main__':
    unittest.main()
