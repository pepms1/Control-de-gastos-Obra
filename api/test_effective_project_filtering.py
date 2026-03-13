import os
import sys
import unittest
from pathlib import Path

from bson import ObjectId

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


def _get_path(document, dotted_key):
    current = document
    for part in dotted_key.split('.'):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current.get(part)
    return current, True


def _matches(document, query):
    if not query:
        return True

    for key, value in query.items():
        if key == '$and':
            return all(_matches(document, item) for item in value)
        if key == '$or':
            return any(_matches(document, item) for item in value)

        field_value, field_exists = _get_path(document, key)

        if isinstance(value, dict):
            if '$in' in value:
                if field_value not in value['$in']:
                    return False
                continue
            if '$exists' in value:
                if bool(field_exists) != bool(value['$exists']):
                    return False
                continue

        if field_value != value:
            return False

    return True


class EffectiveProjectFilteringTests(unittest.TestCase):
    def test_manual_resolved_project_takes_precedence_for_filters(self):
        pb_project_id = str(ObjectId())
        calderon_project_id = str(ObjectId())

        tx = {
            '_id': ObjectId(),
            'projectId': pb_project_id,
            'sap': {'manualResolvedProjectId': calderon_project_id},
        }

        calderon_query = main.with_legacy_project_filter({}, calderon_project_id)
        pb_query = main.with_legacy_project_filter({}, pb_project_id)

        self.assertTrue(_matches(tx, calderon_query))
        self.assertFalse(_matches(tx, pb_query))

    def test_fallback_to_project_id_when_manual_resolution_is_missing(self):
        pb_project_id = str(ObjectId())

        tx = {
            '_id': ObjectId(),
            'projectId': pb_project_id,
            'sap': {},
        }

        pb_query = main.with_legacy_project_filter({}, pb_project_id)

        self.assertTrue(_matches(tx, pb_query))


if __name__ == '__main__':
    unittest.main()
