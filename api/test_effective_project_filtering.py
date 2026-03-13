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


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    def sort(self, *_args, **_kwargs):
        return self

    def skip(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return list(self._docs)


class _FakeTransactionsCollection:
    def __init__(self, docs):
        self.docs = docs
        self.last_count_query = None
        self.last_find_query = None

    def count_documents(self, query):
        self.last_count_query = query
        return sum(1 for doc in self.docs if _matches(doc, query))

    def find(self, query, *_args, **_kwargs):
        self.last_find_query = query
        return _FakeCursor([doc for doc in self.docs if _matches(doc, query)])


class ProjectTransactionsEndpointWiringTests(unittest.TestCase):
    def test_list_transactions_endpoint_returns_manual_resolved_transaction_id(self):
        project_calderon = "69acf7a4988149905ed1a3f9"
        trace_tx_oid = ObjectId("69ae6065aae96a6a5bd0529f")
        tx_doc = {
            "_id": trace_tx_oid,
            "projectId": "PB Y PC INTERIORES",
            "type": "EXPENSE",
            "amount": 500.0,
            "date": "2025-01-20",
            "sap": {
                "manualResolvedProjectId": ObjectId(project_calderon),
                "manualResolvedProjectName": "CALDERON DE LA BARCA",
            },
        }

        fake_transactions = _FakeTransactionsCollection([tx_doc])
        fake_db = SimpleNamespace(
            transactions=fake_transactions,
            suppliers=SimpleNamespace(find=lambda *_args, **_kwargs: []),
            projects=SimpleNamespace(find_one=lambda *_args, **_kwargs: {"_id": ObjectId(project_calderon)}),
        )

        with patch.object(main, "db", fake_db), patch.object(main, "build_transaction_totals", return_value={}):
            response = main.list_transactions(project_id=project_calderon, page=1, limit=50, from_date=None, to_date=None, _={"role": "ADMIN"})

        self.assertEqual(response["totalCount"], 1)
        self.assertEqual(len(response["items"]), 1)
        self.assertEqual(response["items"][0]["id"], str(trace_tx_oid))
        self.assertTrue(_matches(tx_doc, fake_transactions.last_find_query))

    def test_list_transactions_uses_effective_project_filter_for_count_and_find(self):
        project_calderon = "507f1f77bcf86cd799439011"
        project_pb = "507f191e810c19729de860ea"
        tx_doc = {
            "_id": ObjectId("507f191e810c19729de860eb"),
            "projectId": project_pb,
            "type": "EXPENSE",
            "amount": 100.0,
            "sap": {
                "manualResolvedProjectId": project_calderon,
                "manualResolvedProjectName": "CALDERON DE LA BARCA",
            },
        }

        fake_transactions = _FakeTransactionsCollection([tx_doc])
        fake_db = SimpleNamespace(
            transactions=fake_transactions,
            suppliers=SimpleNamespace(find=lambda *_args, **_kwargs: []),
            projects=SimpleNamespace(find_one=lambda *_args, **_kwargs: {"_id": ObjectId(project_calderon)}),
        )

        with patch.object(main, "db", fake_db), patch.object(main, "build_transaction_totals", return_value={}):
            calderon_response = main.list_transactions(project_id=project_calderon, page=1, limit=50, from_date=None, to_date=None, _={"role": "ADMIN"})
            calderon_query = fake_transactions.last_find_query

            pb_response = main.list_transactions(project_id=project_pb, page=1, limit=50, from_date=None, to_date=None, _={"role": "ADMIN"})
            pb_query = fake_transactions.last_find_query

        self.assertEqual(calderon_response["totalCount"], 1)
        self.assertEqual(len(calderon_response["items"]), 1)
        self.assertEqual(pb_response["totalCount"], 0)
        self.assertEqual(len(pb_response["items"]), 0)

        self.assertTrue(_matches(tx_doc, calderon_query))
        self.assertFalse(_matches(tx_doc, pb_query))

    def test_build_project_transactions_query_matches_manual_resolution_precedence(self):
        tx = {
            "projectId": "PB Y PC INTERIORES",
            "sap": {"manualResolvedProjectId": "CALDERON DE LA BARCA"},
        }

        calderon_query = main.build_project_transactions_query("CALDERON DE LA BARCA")
        pb_query = main.build_project_transactions_query("PB Y PC INTERIORES")

        self.assertTrue(_matches(tx, calderon_query))
        self.assertFalse(_matches(tx, pb_query))


if __name__ == '__main__':
    unittest.main()
