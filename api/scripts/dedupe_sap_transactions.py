#!/usr/bin/env python3
"""One-off SAP dedupe + backfill migration script."""

from decimal import Decimal, InvalidOperation
import os
from pymongo import MongoClient, UpdateOne


def infer_source_db(doc: dict) -> str:
    source_db = str(doc.get("sourceDb") or "").strip().upper()
    if source_db:
        return source_db
    iva_value = ((doc.get("tax") or {}).get("iva"))
    if iva_value is None:
        return "UNKNOWN"
    try:
        return "IVA" if Decimal(str(iva_value)) > 0 else "EFECTIVO"
    except (InvalidOperation, ValueError, TypeError):
        return "UNKNOWN"


def infer_source_db_for_backfill(doc: dict) -> str:
    iva_value = ((doc.get("tax") or {}).get("iva"))
    try:
        return "IVA" if Decimal(str(iva_value)) > 0 else "EFECTIVO"
    except (InvalidOperation, ValueError, TypeError):
        return "EFECTIVO"


def to_cents(amount) -> int:
    return int(round(float(amount or 0) * 100))


def normalize_sap_fields(tx: dict) -> dict:
    sap_doc = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
    amount = round(float(sap_doc.get("montoAplicado") or tx.get("amount") or 0), 2)
    return {
        "pagoNum": str(sap_doc.get("pagoNum") or "").strip(),
        "facturaNum": str(sap_doc.get("facturaNum") or "").strip(),
        "montoAplicado": amount,
        "montoAplicadoCents": to_cents(amount),
    }


def is_missing(value):
    return value in (None, "", [], {})


def pick_winner(candidates):
    def sort_key(doc):
        has_category = doc.get("categoryId") or doc.get("category_id")
        created_at = str(doc.get("created_at") or "")
        return (1 if has_category else 0, created_at, doc.get("_id"))

    return max(candidates, key=sort_key)


def main():
    mongo_url = os.getenv("MONGO_URL")
    db_name = os.getenv("DB_NAME", "obra")
    if not mongo_url:
        raise RuntimeError("MONGO_URL env var is required")

    db = MongoClient(mongo_url)[db_name]
    txs = db.transactions

    backfill_ops = []
    for tx in txs.find({"$or": [{"source": "sap"}, {"sap": {"$exists": True}}]}, {"source": 1, "sourceDb": 1, "sourceDbInferred": 1, "tax": 1, "amount": 1, "sap": 1}):
        if not isinstance(tx.get("sap"), dict):
            continue
        normalized = normalize_sap_fields(tx)
        current_source_db = str(tx.get("sourceDb") or "").strip().upper()
        source_db_was_missing = not current_source_db
        source_db = current_source_db or infer_source_db_for_backfill(tx)
        set_payload = {
            "source": "sap",
            "sourceDb": source_db,
            "sap.pagoNum": normalized["pagoNum"],
            "sap.facturaNum": normalized["facturaNum"],
            "sap.montoAplicado": normalized["montoAplicado"],
            "sap.montoAplicadoCents": normalized["montoAplicadoCents"],
        }
        if source_db_was_missing:
            set_payload["sourceDbInferred"] = True
        backfill_ops.append(
            UpdateOne(
                {"_id": tx["_id"]},
                {"$set": set_payload},
            )
        )

    if backfill_ops:
        txs.bulk_write(backfill_ops, ordered=False)

    projection = {
        "projectId": 1,
        "sourceDb": 1,
        "sap": 1,
        "amount": 1,
        "categoryId": 1,
        "category_id": 1,
        "supplierId": 1,
        "supplierName": 1,
        "supplierCardCode": 1,
        "vendor_id": 1,
        "tax": 1,
        "created_at": 1,
    }

    groups = {}
    for tx in txs.find({"source": "sap", "sap": {"$exists": True}}, projection):
        normalized = normalize_sap_fields(tx)
        key = (
            tx.get("projectId"),
            infer_source_db(tx),
            normalized["pagoNum"],
            normalized["facturaNum"],
            normalized["montoAplicadoCents"],
        )
        groups.setdefault(key, []).append(tx)

    duplicate_groups = [g for g in groups.values() if len(g) > 1]
    merge_fields = ["categoryId", "category_id", "supplierId", "supplierName", "supplierCardCode", "vendor_id", "tax"]
    winner_updates = []
    delete_ids = []

    for group in duplicate_groups:
        winner = pick_winner(group)
        merged = {}
        for doc in group:
            if doc["_id"] == winner["_id"]:
                continue
            delete_ids.append(doc["_id"])
            for field in merge_fields:
                if is_missing(winner.get(field)) and not is_missing(doc.get(field)):
                    winner[field] = doc.get(field)
                    merged[field] = doc.get(field)

        if merged:
            winner_updates.append(UpdateOne({"_id": winner["_id"]}, {"$set": merged}))

    if winner_updates:
        txs.bulk_write(winner_updates, ordered=False)
    if delete_ids:
        txs.delete_many({"_id": {"$in": delete_ids}})

    print({
        "backfillUpdated": len(backfill_ops),
        "duplicateGroups": len(duplicate_groups),
        "deletedDocs": len(delete_ids),
    })


if __name__ == "__main__":
    main()
