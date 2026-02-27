#!/usr/bin/env python3
"""Regression check: SAP re-import must preserve manually assigned categories."""

from datetime import datetime, timezone
import os
import uuid

if not os.getenv("MONGO_URL"):
    raise RuntimeError("MONGO_URL env var is required")

os.environ.setdefault("DB_NAME", f"obra_regression_{uuid.uuid4().hex[:8]}")

from main import db, run_sap_import  # noqa: E402


def make_csv_bytes() -> bytes:
    header = (
        "PagoNum,FechaPago,CardCode,Beneficiario,Moneda,TotalPago,ConceptoPago,"
        "FacturaProveedorNum,FechaFactura,MontoAplicado\n"
    )
    row = "9001,2024-12-20,C100,Proveedor Demo,MXN,1000,Compra material,F001,2024-12-19,1000\n"
    return (header + row).encode("utf-8")


def main() -> None:
    project_name = f"REGRESSION-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    csv_bytes = make_csv_bytes()

    first = run_sap_import("sap.csv", csv_bytes, project_name, force=1)
    assert not first.get("already_imported"), "First import should process rows"

    project_doc = db.projects.find_one({"name": project_name})
    assert project_doc is not None, "Project should exist"
    project_id = str(project_doc["_id"])

    tx = db.transactions.find_one({"projectId": project_id, "source": "sap"})
    assert tx is not None, "Expected imported SAP transaction"

    vendor = db.vendors.find_one({"source": "sap", "externalIds.sapCardCode": "C100"})
    assert vendor is not None, "Expected synced SAP vendor"

    tx_category = f"cat-tx-{uuid.uuid4().hex[:6]}"
    vendor_category = f"cat-vendor-{uuid.uuid4().hex[:6]}"

    db.transactions.update_one({"_id": tx["_id"]}, {"$set": {"categoryId": tx_category, "category_id": tx_category}})
    db.vendors.update_one({"_id": vendor["_id"]}, {"$set": {"categoryId": vendor_category}})

    second = run_sap_import("sap.csv", csv_bytes, project_name, force=1)
    assert not second.get("already_imported"), "Second import should process rows with force=1"

    tx_after = db.transactions.find_one({"_id": tx["_id"]})
    vendor_after = db.vendors.find_one({"_id": vendor["_id"]})

    assert tx_after is not None and vendor_after is not None
    assert tx_after.get("categoryId") == tx_category, "Transaction categoryId was overwritten"
    assert tx_after.get("category_id") == tx_category, "Transaction category_id was overwritten"
    assert vendor_after.get("categoryId") == vendor_category, "Vendor categoryId was overwritten"

    print("OK: SAP re-import preserved transaction and vendor categories")
    print(
        {
            "updatedCount": second.get("updatedCount"),
            "insertedCount": second.get("insertedCount"),
            "categoryPreservedCount": second.get("categoryPreservedCount"),
            "categoryWouldHaveChangedCount": second.get("categoryWouldHaveChangedCount"),
        }
    )


if __name__ == "__main__":
    main()
