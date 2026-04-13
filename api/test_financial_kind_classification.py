import os
import sys
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bson import ObjectId

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("SKIP_STARTUP_INIT", "1")
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main  # noqa: E402


class FinancialKindClassificationTests(unittest.TestCase):
    def test_classify_by_category_hint_code(self):
        result = main.classify_financial_kind({"categoryHintCode": "3700-01-001", "type": "EXPENSE"})
        self.assertEqual(result["financialKind"], "contribution_withdrawal")
        self.assertTrue(result["excludeFromExpenseViews"])
        self.assertEqual(result["classificationSource"], "rule")

    def test_classify_by_description(self):
        result = main.classify_financial_kind({"description": "RETIRO DE APORTACION", "type": "EXPENSE"})
        self.assertEqual(result["financialKind"], "contribution_withdrawal")
        self.assertTrue(result["excludeFromExpenseViews"])

    def test_classify_investment_withdrawal_by_description(self):
        result = main.classify_financial_kind({"description": "RETIRO DE INVERSION", "type": "EXPENSE"})
        self.assertEqual(result["financialKind"], "contribution_withdrawal")
        self.assertTrue(result["excludeFromExpenseViews"])
        self.assertEqual(result["classificationSource"], "rule")

    def test_classify_investor_withdrawal_by_hint_name(self):
        result = main.classify_financial_kind(
            {
                "categoryHintName": "Retiro de Inversionistas José Hamui",
                "sap": {"movementType": "egreso"},
            }
        )
        self.assertEqual(result["financialKind"], "contribution_withdrawal")
        self.assertTrue(result["excludeFromExpenseViews"])
        self.assertEqual(result["classificationSource"], "rule")

    def test_classify_default_expense(self):
        result = main.classify_financial_kind({"description": "Pago de proveedor", "type": "EXPENSE"})
        self.assertEqual(result["financialKind"], "expense")
        self.assertFalse(result["excludeFromExpenseViews"])
        self.assertEqual(result["classificationSource"], "default")

    def test_does_not_classify_when_not_expense_like(self):
        result = main.classify_financial_kind({"description": "RETIRO DE INVERSION", "type": "INCOME"})
        self.assertEqual(result["financialKind"], "expense")
        self.assertFalse(result["excludeFromExpenseViews"])
        self.assertEqual(result["classificationSource"], "default")

    def test_expense_query_applies_exclusion_filter(self):
        query = main.build_transactions_query(type_value="EXPENSE")
        query_text = str(query)
        self.assertIn("excludeFromExpenseViews", query_text)
        self.assertIn("contribution_withdrawal", query_text)


class _FakeImportRuns:
    def __init__(self):
        self.docs = []

    def find_one(self, *_args, **_kwargs):
        return None

    def insert_one(self, doc):
        stored = deepcopy(doc)
        stored["_id"] = ObjectId()
        self.docs.append(stored)
        return SimpleNamespace(inserted_id=stored["_id"])

    def update_one(self, *_args, **_kwargs):
        return None


class _FakeCollection:
    def __init__(self):
        self.docs = {}

    def find_one(self, query, projection=None):
        dedupe_key = query.get("dedupeKey")
        doc = self.docs.get(dedupe_key)
        if doc is None:
            return None
        if not projection:
            return deepcopy(doc)
        output = {"_id": doc.get("_id")}
        for key in projection:
            if projection[key]:
                output[key] = doc.get(key)
        return output

    def update_one(self, query, update, upsert=False):
        dedupe_key = query.get("dedupeKey")
        existing = deepcopy(self.docs.get(dedupe_key) or {"_id": ObjectId(), "dedupeKey": dedupe_key})
        set_doc = deepcopy(update.get("$set", {}))
        existing.update(set_doc)
        self.docs[dedupe_key] = existing
        return SimpleNamespace(upserted_id=ObjectId() if upsert else None)

    def find(self, query, projection=None):
        for doc in self.docs.values():
            yield deepcopy(doc)

    def bulk_write(self, ops, ordered=False):
        modified = 0
        for op in ops:
            filter_doc = op._filter
            tx_id = filter_doc.get("_id")
            for key, doc in self.docs.items():
                if doc.get("_id") == tx_id:
                    doc.update(op._doc.get("$set", {}))
                    self.docs[key] = doc
                    modified += 1
        return SimpleNamespace(modified_count=modified, upserted_count=0)


class _FakeVendors:
    def bulk_write(self, *_args, **_kwargs):
        return None


class _FakeUnmatched:
    def update_one(self, *_args, **_kwargs):
        return None


class _FakeOverrides:
    def find_one(self, *_args, **_kwargs):
        return None


class _FakeDb:
    def __init__(self):
        self.importRuns = _FakeImportRuns()
        self.unmatched_projects = _FakeUnmatched()
        self.transactions = _FakeCollection()
        self.vendors = _FakeVendors()
        self.transaction_cancellation_overrides = _FakeOverrides()

    def __getitem__(self, name):
        if name == main.TRANSACTION_CANCELLATION_OVERRIDES_COLLECTION:
            return self.transaction_cancellation_overrides
        raise KeyError(name)


class FinancialKindImportAndReclassifyTests(unittest.TestCase):
    def test_import_persists_financial_kind_fields(self):
        csv_content = (
            "movement_type,source_type,payment_docentry,invoice_docentry,amount_applied,movement_date,invoice_date,"
            "business_partner,card_code,payment_comments,CategoryHintCode,CategoryHintName,raw_project_name,source_db\n"
            "egreso,PAGO,10,20,100.00,2026-01-01,2026-01-01,Proveedor X,CX1,RETIRO DE APORTACION,3700-01-001,"
            "Aportaciones Calderon de la Barca,Proyecto Demo,SBO\n"
        ).encode("utf-8")

        fake_db = _FakeDb()
        fake_transactions = fake_db.transactions

        with patch.object(main, "db", fake_db), patch.object(main, "downloadFromS3Object", return_value=csv_content), patch.object(
            main, "resolve_projects_by_sap_names", return_value={"PROYECTO DEMO": "p1"}
        ):
            result = main.import_sap_movements_by_sbo("SBO_GMDI", "latest")
        self.assertEqual(result.get("rowsError"), 0)

        saved = next(iter(fake_transactions.docs.values()))
        self.assertEqual(saved.get("financialKind"), "contribution_withdrawal")
        self.assertTrue(saved.get("excludeFromExpenseViews"))
        self.assertEqual(saved.get("classificationSource"), "rule")

    def test_reclassify_updates_existing_docs(self):
        tx_id = ObjectId()
        fake_transactions = _FakeCollection()
        fake_transactions.docs["k1"] = {
            "_id": tx_id,
            "dedupeKey": "k1",
            "projectId": "p1",
            "description": "RETIRO DE APORTACION",
            "type": "EXPENSE",
            "categoryHintCode": "3700-01-001",
            "financialKind": "expense",
            "excludeFromExpenseViews": False,
            "classificationSource": "default",
        }
        fake_db = SimpleNamespace(transactions=fake_transactions)

        with patch.object(main, "db", fake_db):
            result = main.reclassify_transactions_financial_kind(project_id="p1")

        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["modified"], 1)
        updated = fake_transactions.docs["k1"]
        self.assertEqual(updated.get("financialKind"), "contribution_withdrawal")
        self.assertTrue(updated.get("excludeFromExpenseViews"))


if __name__ == "__main__":
    unittest.main()
