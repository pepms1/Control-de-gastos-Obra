"""Backfill canonical effective project fields in transactions."""

import os
from pymongo import MongoClient, UpdateOne
from bson import ObjectId


def normalize(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def project_identity(db, raw_project_id):
    project_id = normalize(raw_project_id)
    if not project_id:
        return {"id": None, "code": None, "name": None}

    query = {"_id": ObjectId(project_id)} if ObjectId.is_valid(project_id) else {"_id": project_id}
    project = db.projects.find_one(query, {"name": 1, "slug": 1, "code": 1}) or {}
    return {
        "id": query["_id"],
        "code": normalize(project.get("code") or project.get("slug")),
        "name": normalize(project.get("name")),
    }


def resolve_effective(db, tx):
    sap = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
    manual_id = normalize(sap.get("manualResolvedProjectId"))
    if manual_id:
        ident = project_identity(db, manual_id)
        return {
            "effectiveProjectId": ident["id"] or manual_id,
            "effectiveProjectCode": normalize(sap.get("manualResolvedProjectCode") or ident["code"]),
            "effectiveProjectName": normalize(sap.get("manualResolvedProjectName") or ident["name"]),
        }

    automatic_id = normalize(tx.get("projectId")) or normalize(sap.get("projectId"))
    ident = project_identity(db, automatic_id)
    return {
        "effectiveProjectId": ident["id"] or automatic_id,
        "effectiveProjectCode": ident["code"],
        "effectiveProjectName": ident["name"],
    }


def main():
    mongo_uri = (os.getenv("MONGO_URI") or "mongodb://localhost:27017").strip()
    db_name = (os.getenv("DB_NAME") or "control_gastos").strip()
    client = MongoClient(mongo_uri)
    db = client[db_name]

    scanned = 0
    updated = 0
    ops = []

    projection = {
        "projectId": 1,
        "sap.projectId": 1,
        "sap.manualResolvedProjectId": 1,
        "sap.manualResolvedProjectCode": 1,
        "sap.manualResolvedProjectName": 1,
        "effectiveProjectId": 1,
        "effectiveProjectCode": 1,
        "effectiveProjectName": 1,
    }

    for tx in db.transactions.find({}, projection):
        scanned += 1
        next_values = resolve_effective(db, tx)
        current_values = {
            "effectiveProjectId": tx.get("effectiveProjectId"),
            "effectiveProjectCode": tx.get("effectiveProjectCode"),
            "effectiveProjectName": tx.get("effectiveProjectName"),
        }
        if current_values == next_values:
            continue

        ops.append(UpdateOne({"_id": tx["_id"]}, {"$set": next_values}))
        if len(ops) >= 500:
            result = db.transactions.bulk_write(ops, ordered=False)
            updated += (result.modified_count or 0)
            ops = []

    if ops:
        result = db.transactions.bulk_write(ops, ordered=False)
        updated += (result.modified_count or 0)

    print({"scanned": scanned, "updated": updated})


if __name__ == "__main__":
    main()
