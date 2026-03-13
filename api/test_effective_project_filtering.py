import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


def _matches(document, query):
    if not query:
        return True

    for key, value in query.items():
        if key == "$and":
            return all(_matches(document, subquery) for subquery in value)
        if key == "$or":
            return any(_matches(document, subquery) for subquery in value)

        sentinel = object()
        current = document
        for part in key.split('.'):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                current = sentinel
                break

        if isinstance(value, dict) and "$exists" in value:
            should_exist = bool(value["$exists"])
            exists = current is not sentinel
            if exists != should_exist:
                return False
        elif current is sentinel or current != value:
            return False

    return True


class EffectiveProjectFilteringTests(unittest.TestCase):
    def test_manual_resolution_takes_precedence_over_project_id(self):
        tx = {
            "projectId": "PB Y PC INTERIORES",
            "sap": {"manualResolvedProjectId": "CALDERON DE LA BARCA"},
        }

        calderon_filter = main.build_effective_project_filter("CALDERON DE LA BARCA")
        pb_filter = main.build_effective_project_filter("PB Y PC INTERIORES")

        self.assertTrue(_matches(tx, calderon_filter))
        self.assertFalse(_matches(tx, pb_filter))

    def test_project_id_is_used_when_manual_resolution_is_missing(self):
        tx = {
            "projectId": "PB Y PC INTERIORES",
            "sap": {},
        }

        pb_filter = main.build_effective_project_filter("PB Y PC INTERIORES")
        calderon_filter = main.build_effective_project_filter("CALDERON DE LA BARCA")

        self.assertTrue(_matches(tx, pb_filter))
        self.assertFalse(_matches(tx, calderon_filter))


if __name__ == '__main__':
    unittest.main()
