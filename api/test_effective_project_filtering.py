import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017')
os.environ.setdefault('SKIP_STARTUP_INIT', '1')
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402
from bson import ObjectId  # noqa: E402
from types import SimpleNamespace
from unittest.mock import patch


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
        elif isinstance(value, dict) and "$in" in value:
            if current is sentinel or current not in value["$in"]:
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


    def test_project_id_string_matches_effective_filter(self):
        tx = {"projectId": "507f1f77bcf86cd799439011", "sap": {}}

        project_filter = main.build_effective_project_filter("507f1f77bcf86cd799439011")

        self.assertTrue(_matches(tx, project_filter))

    def test_project_id_object_id_matches_effective_filter(self):
        oid = ObjectId("507f1f77bcf86cd799439011")
        tx = {"projectId": oid, "sap": {}}

        project_filter = main.build_effective_project_filter("507f1f77bcf86cd799439011")

        self.assertTrue(_matches(tx, project_filter))

    def test_manual_resolved_project_id_string_matches_effective_filter(self):
        tx = {
            "projectId": "some-other-project",
            "sap": {"manualResolvedProjectId": "507f1f77bcf86cd799439011"},
        }

        project_filter = main.build_effective_project_filter("507f1f77bcf86cd799439011")

        self.assertTrue(_matches(tx, project_filter))

    def test_manual_resolved_project_id_object_id_matches_effective_filter(self):
        oid = ObjectId("507f1f77bcf86cd799439011")
        tx = {
            "projectId": "some-other-project",
            "sap": {"manualResolvedProjectId": oid},
        }

        project_filter = main.build_effective_project_filter("507f1f77bcf86cd799439011")

        self.assertTrue(_matches(tx, project_filter))

    def test_transactions_query_prioritizes_manual_resolution_for_project_views(self):
        tx = {
            "projectId": "PB Y PC INTERIORES",
            "supplierName": "PROVEEDOR UNO",
            "sap": {
                "manualResolvedProjectId": "CALDERON DE LA BARCA",
                "pagoNum": "PAY-12345",
            },
        }

        query = main.build_transactions_query(project_id="CALDERON DE LA BARCA")

        self.assertTrue(_matches(tx, query))

    def test_transactions_query_searches_by_sap_payment_number(self):
        fake_db = SimpleNamespace(categories=SimpleNamespace(find=lambda *_args, **_kwargs: []))
        with patch.object(main, "db", fake_db):
            query = main.build_transactions_query(project_id="CALDERON DE LA BARCA", search_query="PAY-12345")

        def collect_or_fields(node):
            fields = set()
            if isinstance(node, dict):
                for key, value in node.items():
                    if key == "$or" and isinstance(value, list):
                        for clause in value:
                            if isinstance(clause, dict) and len(clause) == 1:
                                fields.add(next(iter(clause.keys())))
                            fields.update(collect_or_fields(clause))
                    else:
                        fields.update(collect_or_fields(value))
            elif isinstance(node, list):
                for item in node:
                    fields.update(collect_or_fields(item))
            return fields

        search_fields = collect_or_fields(query)

        self.assertIn("sap.pagoNum", search_fields)

    def test_list_transactions_includes_manual_resolved_project_in_project_view(self):
        project_id = "69acf7a4988149905ed1a3f9"
        tx = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "projectId": "PB Y PC INTERIORES",
            "type": "EXPENSE",
            "amount": 1200,
            "date": "2025-01-15",
            "sap": {
                "manualResolvedProjectId": ObjectId(project_id),
                "manualResolvedProjectName": "CALDERON DE LA BARCA",
            },
        }

        class FakeCursor:
            def __init__(self, docs):
                self.docs = list(docs)

            def sort(self, *_args, **_kwargs):
                return self

            def skip(self, value):
                self.docs = self.docs[value:]
                return self

            def limit(self, value):
                self.docs = self.docs[:value]
                return self

            def __iter__(self):
                return iter(self.docs)

        class FakeTransactions:
            def __init__(self, docs):
                self.docs = docs

            def count_documents(self, query):
                return sum(1 for doc in self.docs if _matches(doc, query))

            def find(self, query, *_args, **_kwargs):
                return FakeCursor([doc for doc in self.docs if _matches(doc, query)])

            def aggregate(self, *_args, **_kwargs):
                return []

        fake_db = SimpleNamespace(
            transactions=FakeTransactions([tx]),
            suppliers=SimpleNamespace(find=lambda *_args, **_kwargs: []),
            categories=SimpleNamespace(find=lambda *_args, **_kwargs: []),
            projects=SimpleNamespace(find_one=lambda *_args, **_kwargs: {'_id': ObjectId(project_id)}),
        )

        with patch.object(main, "db", fake_db):
            response = main.list_transactions(project_id=project_id, from_date=None, to_date=None, page=1, limit=50, _={"role": "ADMIN"})

        self.assertEqual(response["totalCount"], 1)
        self.assertEqual(len(response["items"]), 1)


if __name__ == '__main__':
    unittest.main()
