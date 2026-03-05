"""Backfill manual/effective category fields in transactions."""

from datetime import datetime, timezone
import os
from pymongo import MongoClient, UpdateOne


def normalize(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"N/A", "NA"}:
        return None
    return text


def main():
    mongo_uri = (os.getenv("MONGO_URI") or "mongodb://localhost:27017").strip()
    db_name = (os.getenv("DB_NAME") or "control_gastos").strip()
    client = MongoClient(mongo_uri)
    db = client[db_name]

    ops = []
    scanned = 0
    now = datetime.now(timezone.utc).isoformat()

    for tx in db.transactions.find({}, {
        "category_id": 1,
        "categoryId": 1,
        "category_name": 1,
        "categoryHintCode": 1,
        "categoryHintName": 1,
        "categoryManualCode": 1,
        "categoryManualName": 1,
    }):
        scanned += 1
        hint_code = normalize(tx.get("categoryHintCode"))
        hint_name = normalize(tx.get("categoryHintName"))
        manual_code = normalize(tx.get("categoryManualCode"))
        manual_name = normalize(tx.get("categoryManualName"))

        # Conservative migration: preserve existing category assignment as manual if no manual exists yet.
        legacy_category = normalize(tx.get("category_id") or tx.get("categoryId"))
        legacy_name = normalize(tx.get("category_name"))
        if not manual_code and not manual_name and (legacy_category or legacy_name):
            manual_code = legacy_category
            manual_name = legacy_name

        updates = {
            "categoryManualCode": manual_code,
            "categoryManualName": manual_name,
            "categoryManualUpdatedAt": now,
            "categoryEffectiveCode": manual_code or hint_code,
            "categoryEffectiveName": manual_name or hint_name,
        }

        if hint_code:
            updates["categorySapCode"] = hint_code
        if hint_name:
            updates["categorySapName"] = hint_name

        ops.append(UpdateOne({"_id": tx["_id"]}, {"$set": updates}))
        if len(ops) >= 500:
            db.transactions.bulk_write(ops, ordered=False)
            ops = []

    if ops:
        db.transactions.bulk_write(ops, ordered=False)

    print({"scanned": scanned})


if __name__ == "__main__":
    main()
