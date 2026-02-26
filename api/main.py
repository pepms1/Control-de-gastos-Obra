from fastapi import FastAPI, HTTPException, Response, Depends, UploadFile, File
from fastapi import Header
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
from datetime import date, datetime, timedelta, timezone
from jose import JWTError, jwt
from passlib.context import CryptContext
from hashlib import sha256
from decimal import Decimal, InvalidOperation
from io import BytesIO
from urllib.parse import urlparse
from urllib.request import urlopen
import re
import csv
import openpyxl
import os

app = FastAPI(title="Control de Obra API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URL = os.getenv("MONGO_URL")
if not MONGO_URL:
    raise RuntimeError("MONGO_URL env var is required")

DB_NAME = os.getenv("DB_NAME", "obra")
JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "12"))

client = MongoClient(MONGO_URL)
db = client[DB_NAME]
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ---------- helpers ----------
def oid(s: str) -> ObjectId:
    try:
        return ObjectId(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid id: {s}")


def serialize(doc):
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


def serialize_user(user):
    user_doc = serialize(user)
    user_doc.pop("password_hash", None)
    return user_doc


def role_from_token(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    username = payload.get("sub")
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    user = db.users.find_one({"username": username})
    if not user or not user.get("active", True):
        raise HTTPException(status_code=401, detail="User inactive or not found")

    role = user.get("role")
    if role not in ("ADMIN", "VIEWER"):
        raise HTTPException(status_code=401, detail="Invalid role")
    return {"username": user["username"], "role": role, "active": user.get("active", True)}


def require_admin(user=Depends(role_from_token)):
    if user["role"] != "ADMIN":
        raise HTTPException(status_code=403, detail="ADMIN role required")
    return user


def require_authenticated(user=Depends(role_from_token)):
    return user


def ensure_default_users():
    users = db.users
    users.create_index("username", unique=True)

    default_admin_user = os.getenv("DEFAULT_ADMIN_USERNAME", "admin")
    default_admin_pass = os.getenv("DEFAULT_ADMIN_PASSWORD", "admin123")
    default_viewer_user = os.getenv("DEFAULT_VIEWER_USERNAME", "viewer")
    default_viewer_pass = os.getenv("DEFAULT_VIEWER_PASSWORD", "viewer123")

    defaults = [
        (default_admin_user, default_admin_pass, "ADMIN"),
        (default_viewer_user, default_viewer_pass, "VIEWER"),
    ]

    for username, plain_password, role in defaults:
        existing = users.find_one({"username": username})
        if existing:
            continue
        users.insert_one(
            {
                "username": username,
                "password_hash": pwd_context.hash(plain_password),
                "role": role,
                "active": True,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    users.update_many({"active": {"$exists": False}}, {"$set": {"active": True}})


def ensure_indexes():
    db.users.create_index("username", unique=True)
    db.suppliers.create_index("cardCode", unique=True)
    db.supplierCategories.create_index("name", unique=True)
    db.projects.create_index("name", unique=True)
    db.payments.create_index([("projectId", 1), ("sapPaymentNum", 1)], unique=True)
    db.apInvoices.create_index([("projectId", 1), ("sapInvoiceNum", 1)], unique=True)
    db.paymentLines.create_index([("paymentId", 1), ("apInvoiceId", 1), ("appliedAmount", 1)], unique=True)
    db.importRuns.create_index("sha256", unique=True)
    db.transactions.create_index(
        [("projectId", 1), ("sap.pagoNum", 1), ("sap.facturaNum", 1), ("sap.montoAplicado", 1)],
        unique=True,
        partialFilterExpression={"source": "sap"},
    )


def create_token(username: str, role: str):
    exp = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS)
    payload = {"sub": username, "role": role, "exp": exp}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


ensure_indexes()
ensure_default_users()


def normalizeDate(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    cleaned = str(value).strip().replace("\ufeff", "")
    if not cleaned:
        return None

    yyyy_mm_dd_match = re.match(r"^(\d{4}-\d{2}-\d{2})", cleaned)
    if yyyy_mm_dd_match:
        try:
            return datetime.strptime(yyyy_mm_dd_match.group(1), "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid date value: {value}") from exc

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise ValueError(f"Invalid date value: {value}") from exc


def parse_excel_date(value):
    normalized = normalizeDate(value)
    if normalized is None:
        return None
    return normalized.isoformat()


def parse_decimal(value):
    if value is None:
        raise ValueError("Amount is required")
    if isinstance(value, (int, float, Decimal)):
        return round(float(value), 2)
    text = str(value).strip()
    if not text:
        raise ValueError("Amount is required")
    normalized = text.replace(",", "")
    try:
        return round(float(Decimal(normalized)), 2)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid number: {value}") from exc


def parse_sap_file(file_name: str, file_bytes: bytes):
    expected_headers = [
        "PagoNum",
        "FechaPago",
        "CardCode",
        "Beneficiario",
        "Moneda",
        "TotalPago",
        "ConceptoPago",
        "FacturaProveedorNum",
        "FechaFactura",
        "MontoAplicado",
    ]

    def normalize_header(header_value):
        if header_value is None:
            return ""
        return str(header_value).replace("\ufeff", "").strip()

    if file_name.lower().endswith(".csv"):
        decoded = file_bytes.decode("utf-8-sig")
        reader = csv.reader(decoded.splitlines())
        rows = list(reader)
        if not rows:
            return []

        headers = [normalize_header(h) for h in rows[0]]
        if headers != expected_headers:
            raise HTTPException(status_code=400, detail=f"Invalid headers. Expected: {expected_headers}")

        parsed = []
        for values in rows[1:]:
            row_values = values
            if len(values) > len(expected_headers):
                repaired_values = values[:6] + [",".join(values[6:-3])] + values[-3:]
                row_values = repaired_values

            row_dict = {}
            for idx, h in enumerate(expected_headers):
                row_dict[h] = row_values[idx] if idx < len(row_values) else None

            if len(values) != len(expected_headers):
                row_dict["__csvRepairApplied"] = len(values) > len(expected_headers)
                row_dict["__csvOriginalFieldCount"] = len(values)

            parsed.append(row_dict)

        return parsed

    if file_name.lower().endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [normalize_header(h) for h in rows[0]]
        if headers != expected_headers:
            raise HTTPException(status_code=400, detail=f"Invalid headers. Expected: {expected_headers}")

        parsed = []
        for values in rows[1:]:
            row_dict = {}
            for idx, h in enumerate(expected_headers):
                row_dict[h] = values[idx] if idx < len(values) else None
            parsed.append(row_dict)
        return parsed

    raise HTTPException(status_code=400, detail="Only CSV and XLSX files are supported")

# ---------- health ----------
@app.get("/")
def root():
    return {"status": "API running"}


@app.head("/")
def root_head():
    return Response(status_code=200)


@app.get("/health")
def health():
    return {"ok": True}


@app.head("/health")
def health_head():
    return Response(status_code=200)


# ---------- auth ----------
@app.post("/auth/login")
def login(payload: dict):
    username = (payload.get("username") or "").strip()
    password = payload.get("password") or ""
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password are required")

    user = db.users.find_one({"username": username})
    if not user or not pwd_context.verify(password, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not user.get("active", True):
        raise HTTPException(status_code=403, detail="User is inactive")

    role = user.get("role", "VIEWER")
    token = create_token(username, role)
    return {"access_token": token, "token_type": "bearer", "role": role, "username": username}


@app.get("/auth/me")
def me(user=Depends(require_authenticated)):
    return user


@app.post("/users")
def create_user(payload: dict, _: dict = Depends(require_admin)):
    username = (payload.get("username") or "").strip()
    password = payload.get("password") or ""
    role = (payload.get("role") or "").strip().upper()
    active = bool(payload.get("active", True))

    if len(username) < 3:
        raise HTTPException(status_code=400, detail="username must have at least 3 characters")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="password must have at least 6 characters")
    if role not in ("ADMIN", "VIEWER"):
        raise HTTPException(status_code=400, detail="role must be ADMIN or VIEWER")
    if db.users.find_one({"username": username}):
        raise HTTPException(status_code=409, detail="User already exists")

    doc = {
        "username": username,
        "password_hash": pwd_context.hash(password),
        "role": role,
        "active": active,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _id = db.users.insert_one(doc).inserted_id
    return serialize_user(db.users.find_one({"_id": _id}))


@app.get("/users")
def list_users(_: dict = Depends(require_admin)):
    users = db.users.find({}, {"password_hash": 0}).sort("created_at", -1)
    return [serialize(u) for u in users]


@app.get("/api/supplier-categories")
def list_supplier_categories(_: dict = Depends(require_authenticated)):
    return [serialize(c) for c in db.supplierCategories.find({}).sort("name", 1)]


@app.post("/api/supplier-categories")
def create_supplier_category(payload: dict, _: dict = Depends(require_admin)):
    name = (payload.get("name") or "").strip()
    if len(name) < 2:
        raise HTTPException(status_code=400, detail="name is required")
    if db.supplierCategories.find_one({"name": name}):
        raise HTTPException(status_code=409, detail="Supplier category already exists")
    _id = db.supplierCategories.insert_one({"name": name}).inserted_id
    return serialize(db.supplierCategories.find_one({"_id": _id}))


@app.get("/api/suppliers")
def list_suppliers(uncategorized: int = 0, _: dict = Depends(require_authenticated)):
    query = {}
    if uncategorized == 1:
        query = {"$or": [{"categoryId": None}, {"categoryId": {"$exists": False}}]}
    return [serialize(s) for s in db.suppliers.find(query).sort("name", 1)]


@app.patch("/api/suppliers/{supplier_id}")
def update_supplier(supplier_id: str, payload: dict, _: dict = Depends(require_admin)):
    supplier = db.suppliers.find_one({"_id": oid(supplier_id)})
    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    category_id = payload.get("categoryId")
    if category_id is not None:
        if not db.supplierCategories.find_one({"_id": oid(category_id)}):
            raise HTTPException(status_code=400, detail="Invalid categoryId")
        db.suppliers.update_one({"_id": oid(supplier_id)}, {"$set": {"categoryId": category_id}})
    else:
        db.suppliers.update_one({"_id": oid(supplier_id)}, {"$set": {"categoryId": None}})

    return serialize(db.suppliers.find_one({"_id": oid(supplier_id)}))


def run_sap_import(file_name: str, file_bytes: bytes, project: str, force: int, source: str = "sap-payments"):
    file_hash = sha256(file_bytes).hexdigest()

    existing_run = db.importRuns.find_one({"sha256": file_hash})
    existing_ok_run = existing_run and existing_run.get("status") == "ok"

    if existing_ok_run and force != 1:
        return {"already_imported": True, "importRunId": str(existing_run["_id"])}

    now = datetime.now(timezone.utc).isoformat()
    project_name = project.strip() or "CALDERON DE LA BARCA"
    project_doc = db.projects.find_one_and_update(
        {"name": project_name},
        {"$setOnInsert": {"name": project_name}},
        upsert=True,
    )
    if not project_doc:
        project_doc = db.projects.find_one({"name": project_name})
    project_id = str(project_doc["_id"])

    import_run_doc = {
        "sha256": file_hash,
        "fileName": file_name,
        "source": source,
        "projectId": project_id,
        "rowsTotal": 0,
        "rowsOk": 0,
        "rowsSkipped": 0,
        "rowsError": 0,
        "status": "processing",
        "startedAt": now,
        "finishedAt": None,
        "errorsSample": [],
    }

    should_reuse_existing_run = existing_run and (force == 1 or not existing_ok_run or (existing_run.get("rowsOk") or 0) == 0)

    if should_reuse_existing_run:
        db.importRuns.update_one({"_id": existing_run["_id"]}, {"$set": import_run_doc})
        import_run_id = existing_run["_id"]
    else:
        import_run_id = db.importRuns.insert_one(import_run_doc).inserted_id

    rows = parse_sap_file(file_name, file_bytes)

    suppliers_created = 0
    payments_upserted = 0
    invoices_upserted = 0
    lines_inserted = 0
    sap_expenses_upserted = 0
    duplicates_skipped = 0
    rows_ok = 0
    rows_error = 0
    errors_sample = []

    suppliers_ops = []
    line_records = []
    existing_cardcodes = {s["cardCode"] for s in db.suppliers.find({}, {"cardCode": 1})}
    created_cardcodes = set()

    for idx, row in enumerate(rows, start=2):
        try:
            if row.get("__csvRepairApplied") and len(errors_sample) < 50:
                errors_sample.append(
                    {
                        "row": idx,
                        "warning": f"CSV row repaired from {row.get('__csvOriginalFieldCount')} columns to 10",
                    }
                )

            payment_num = str(row.get("PagoNum") or "").strip()
            card_code = str(row.get("CardCode") or "").strip()
            beneficiary = str(row.get("Beneficiario") or "").strip()
            currency = str(row.get("Moneda") or "").strip()
            concept = str(row.get("ConceptoPago") or "").strip()
            invoice_num = str(row.get("FacturaProveedorNum") or "").strip()

            if not payment_num or not card_code or not invoice_num:
                raise ValueError("PagoNum, CardCode y FacturaProveedorNum son obligatorios")

            payment_date = parse_excel_date(row.get("FechaPago"))
            invoice_date = parse_excel_date(row.get("FechaFactura"))
            total_payment = parse_decimal(row.get("TotalPago"))
            applied_amount = parse_decimal(row.get("MontoAplicado"))

            if card_code not in existing_cardcodes and card_code not in created_cardcodes:
                suppliers_created += 1
                created_cardcodes.add(card_code)

            suppliers_ops.append(
                UpdateOne(
                    {"cardCode": card_code},
                    {
                        "$setOnInsert": {"cardCode": card_code, "categoryId": None},
                        "$set": {"name": beneficiary or card_code},
                    },
                    upsert=True,
                )
            )

            payment_key = f"{payment_num}|{card_code}|{payment_date}|{currency}|{total_payment}|{concept}"
            invoice_key = f"{invoice_num}|{card_code}|{invoice_date}"
            line_records.append(
                {
                    "rowNumber": idx,
                    "paymentNum": payment_num,
                    "invoiceNum": invoice_num,
                    "cardCode": card_code,
                    "paymentKey": payment_key,
                    "invoiceKey": invoice_key,
                    "appliedAmount": applied_amount,
                    "paymentDate": payment_date,
                    "invoiceDate": invoice_date,
                    "currency": currency,
                    "totalPayment": total_payment,
                    "concept": concept,
                    "beneficiary": beneficiary,
                }
            )
            rows_ok += 1
        except Exception as exc:
            rows_error += 1
            if len(errors_sample) < 50:
                errors_sample.append({"row": idx, "error": str(exc)})

    if suppliers_ops:
        db.suppliers.bulk_write(suppliers_ops, ordered=False)

    suppliers_map = {s["cardCode"]: str(s["_id"]) for s in db.suppliers.find({}, {"cardCode": 1})}

    payments_unique = {}
    invoices_unique = {}
    for record in line_records:
        supplier_id = suppliers_map.get(record["cardCode"])
        if not supplier_id:
            continue
        payments_unique[(record["paymentNum"], record["cardCode"])] = UpdateOne(
            {"projectId": project_id, "sapPaymentNum": record["paymentNum"]},
            {
                "$set": {
                    "paymentDate": record["paymentDate"],
                    "supplierId": supplier_id,
                    "currency": record["currency"],
                    "totalPayment": record["totalPayment"],
                    "concept": record["concept"],
                }
            },
            upsert=True,
        )
        invoices_unique[(record["invoiceNum"], record["cardCode"])] = UpdateOne(
            {"projectId": project_id, "sapInvoiceNum": record["invoiceNum"]},
            {
                "$set": {
                    "invoiceDate": record["invoiceDate"],
                    "supplierId": supplier_id,
                }
            },
            upsert=True,
        )

    if payments_unique:
        result = db.payments.bulk_write(list(payments_unique.values()), ordered=False)
        payments_upserted = (result.upserted_count or 0) + (result.modified_count or 0)
    if invoices_unique:
        result = db.apInvoices.bulk_write(list(invoices_unique.values()), ordered=False)
        invoices_upserted = (result.upserted_count or 0) + (result.modified_count or 0)

    payments_map = {
        p["sapPaymentNum"]: str(p["_id"])
        for p in db.payments.find({"projectId": project_id}, {"sapPaymentNum": 1})
    }
    invoices_map = {
        i["sapInvoiceNum"]: str(i["_id"])
        for i in db.apInvoices.find({"projectId": project_id}, {"sapInvoiceNum": 1})
    }

    lines_ops = []
    sap_expense_ops = []
    for record in line_records:
        payment_id = payments_map.get(record["paymentNum"])
        invoice_id = invoices_map.get(record["invoiceNum"])
        supplier_id = suppliers_map.get(record["cardCode"])
        if not payment_id or not invoice_id:
            rows_error += 1
            if len(errors_sample) < 50:
                errors_sample.append({"row": record["rowNumber"], "error": "Payment or invoice missing for line"})
            continue

        lines_ops.append(
            UpdateOne(
                {"paymentId": payment_id, "apInvoiceId": invoice_id, "appliedAmount": record["appliedAmount"]},
                {"$setOnInsert": {"paymentId": payment_id, "apInvoiceId": invoice_id, "appliedAmount": record["appliedAmount"]}},
                upsert=True,
            )
        )

        if supplier_id:
            sap_doc = {
                "type": "EXPENSE",
                "projectId": project_id,
                "date": record["paymentDate"] or record["invoiceDate"],
                "amount": record["appliedAmount"],
                "currency": record["currency"],
                "concept": record["concept"],
                "description": record["concept"],
                "supplierId": supplier_id,
                "categoryId": None,
                "category_id": None,
                "vendor_id": None,
                "source": "sap",
                "sap": {
                    "pagoNum": record["paymentNum"],
                    "facturaNum": record["invoiceNum"],
                    "montoAplicado": record["appliedAmount"],
                },
            }
            sap_expense_ops.append(
                UpdateOne(
                    {
                        "projectId": project_id,
                        "sap.pagoNum": record["paymentNum"],
                        "sap.facturaNum": record["invoiceNum"],
                        "sap.montoAplicado": record["appliedAmount"],
                        "source": "sap",
                    },
                    {"$set": sap_doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc).isoformat()}},
                    upsert=True,
                )
            )

    if lines_ops:
        result = db.paymentLines.bulk_write(lines_ops, ordered=False)
        lines_inserted = result.upserted_count or 0
        duplicates_skipped = len(lines_ops) - lines_inserted

    if sap_expense_ops:
        result = db.transactions.bulk_write(sap_expense_ops, ordered=False)
        sap_expenses_upserted = (result.upserted_count or 0) + (result.modified_count or 0)

    rows_total = len(rows)
    db.importRuns.update_one(
        {"_id": import_run_id},
        {
            "$set": {
                "rowsTotal": rows_total,
                "rowsOk": rows_ok,
                "rowsSkipped": duplicates_skipped,
                "rowsError": rows_error,
                "status": "ok" if rows_error == 0 else "completed_with_errors",
                "finishedAt": datetime.now(timezone.utc).isoformat(),
                "errorsSample": errors_sample[:50],
            }
        },
    )

    return {
        "already_imported": False,
        "rowsTotal": rows_total,
        "rowsOk": rows_ok,
        "suppliersCreated": suppliers_created,
        "paymentsUpserted": payments_upserted,
        "invoicesUpserted": invoices_upserted,
        "linesInserted": lines_inserted,
        "sapExpensesUpserted": sap_expenses_upserted,
        "duplicatesSkipped": duplicates_skipped,
        "errorsSample": errors_sample[:50],
        "importRunId": str(import_run_id),
    }


@app.post("/api/import/sap-payments")
async def import_sap_payments(
    file: UploadFile = File(...),
    project: str = "CALDERON DE LA BARCA",
    force: int = 0,
    _: dict = Depends(require_admin),
):
    file_bytes = await file.read()
    return run_sap_import(file.filename or "", file_bytes, project, force, source="sap-payments")


@app.post("/api/cron/import/sap-payments")
def cron_import_sap_payments(project: str = "CALDERON DE LA BARCA", force: int = 0):
    sap_import_url = (os.getenv("SAP_IMPORT_URL") or "").strip()
    now = datetime.now(timezone.utc).isoformat()

    if not sap_import_url:
        skipped_hash = sha256(f"skipped_no_source:{now}".encode("utf-8")).hexdigest()
        import_run_id = db.importRuns.insert_one(
            {
                "sha256": skipped_hash,
                "fileName": None,
                "source": "sap-payments-cron",
                "projectId": None,
                "rowsTotal": 0,
                "rowsOk": 0,
                "rowsSkipped": 0,
                "rowsError": 0,
                "status": "skipped_no_source",
                "startedAt": now,
                "finishedAt": now,
                "errorsSample": [],
            }
        ).inserted_id
        print("sap_payments_cron skipped_no_source")
        return {"status": "skipped_no_source", "importRunId": str(import_run_id)}

    file_name = os.path.basename(urlparse(sap_import_url).path) or "sap_payments_import.csv"
    try:
        with urlopen(sap_import_url, timeout=60) as response:
            file_bytes = response.read()
    except Exception as exc:
        error_hash = sha256(f"download_error:{sap_import_url}:{now}".encode("utf-8")).hexdigest()
        import_run_id = db.importRuns.insert_one(
            {
                "sha256": error_hash,
                "fileName": file_name,
                "source": "sap-payments-cron",
                "projectId": None,
                "rowsTotal": 0,
                "rowsOk": 0,
                "rowsSkipped": 0,
                "rowsError": 1,
                "status": "failed_download",
                "startedAt": now,
                "finishedAt": datetime.now(timezone.utc).isoformat(),
                "errorsSample": [{"error": str(exc)}],
            }
        ).inserted_id
        raise HTTPException(
            status_code=502,
            detail={"status": "failed_download", "importRunId": str(import_run_id), "error": str(exc)},
        ) from exc

    return run_sap_import(file_name, file_bytes, project, force, source="sap-payments-cron")


# ---------- seed categories ----------
DEFAULT_CATEGORIES = [
    "Albañilería",
    "Cimentación / Estructura",
    "Plomería / Hidrosanitario",
    "Eléctrico",
    "Tablaroca / Plafones",
    "Cancelería / Vidrio",
    "Carpintería",
    "Herrería",
    "Impermeabilización",
    "Pisos / Azulejos",
    "Yesos / Aplanados",
    "Pintura",
    "Acabados / Detalles",
    "Materiales (Generales)",
    "Renta de maquinaria",
    "Fletes / Acarreos",
    "Permisos / Gestoría",
    "Mano de obra (General)",
    "Seguridad / Limpieza",
    "Imprevistos",
]


@app.post("/seed")
def seed(_: dict = Depends(require_admin)):
    cats = db.categories
    existing = {c["name"] for c in cats.find({}, {"name": 1})}
    to_insert = [{"name": n, "active": True} for n in DEFAULT_CATEGORIES if n not in existing]
    if to_insert:
        cats.insert_many(to_insert)
    return {"created_categories": len(to_insert)}


# ---------- categories ----------
@app.get("/categories")
def list_categories(active_only: bool = True, _: dict = Depends(require_authenticated)):
    q = {"active": True} if active_only else {}
    return [serialize(c) for c in db.categories.find(q).sort("name", 1)]


@app.post("/categories")
def create_category(payload: dict, _: dict = Depends(require_admin)):
    name = (payload.get("name") or "").strip()
    if len(name) < 2:
        raise HTTPException(status_code=400, detail="name is required")
    if db.categories.find_one({"name": name}):
        raise HTTPException(status_code=409, detail="Category already exists")
    _id = db.categories.insert_one({"name": name, "active": True}).inserted_id
    return serialize(db.categories.find_one({"_id": _id}))


@app.patch("/categories/{category_id}")
def update_category(category_id: str, payload: dict, _: dict = Depends(require_admin)):
    cat = db.categories.find_one({"_id": oid(category_id)})
    if not cat:
        raise HTTPException(status_code=404, detail="Category not found")

    updates = {}
    if "name" in payload:
        name = (payload.get("name") or "").strip()
        if len(name) < 2:
            raise HTTPException(status_code=400, detail="name is required")
        dup = db.categories.find_one({"name": name, "_id": {"$ne": oid(category_id)}})
        if dup:
            raise HTTPException(status_code=409, detail="Category already exists")
        updates["name"] = name
    if "active" in payload:
        updates["active"] = bool(payload.get("active"))
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    db.categories.update_one({"_id": oid(category_id)}, {"$set": updates})
    return serialize(db.categories.find_one({"_id": oid(category_id)}))


@app.delete("/categories/{category_id}")
def delete_category(category_id: str, _: dict = Depends(require_admin)):
    result = db.categories.delete_one({"_id": oid(category_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Category not found")
    db.vendors.update_many({}, {"$pull": {"category_ids": category_id}})
    db.transactions.update_many({"category_id": category_id}, {"$set": {"category_id": None}})
    return {"ok": True}


# ---------- vendors ----------
@app.get("/vendors")
def list_vendors(active_only: bool = True, category_id: str | None = None, _: dict = Depends(require_authenticated)):
    q = {"active": True} if active_only else {}
    if category_id:
        q["category_ids"] = category_id
    return [serialize(v) for v in db.vendors.find(q).sort("name", 1)]


@app.post("/vendors")
def create_vendor(payload: dict, _: dict = Depends(require_admin)):
    name = (payload.get("name") or "").strip()
    if len(name) < 2:
        raise HTTPException(status_code=400, detail="name is required")
    if db.vendors.find_one({"name": name}):
        raise HTTPException(status_code=409, detail="Vendor already exists")

    category_ids = payload.get("category_ids") or []
    for cid in category_ids:
        if not db.categories.find_one({"_id": oid(cid), "active": True}):
            raise HTTPException(status_code=400, detail=f"Invalid category_id: {cid}")

    doc = {
        "name": name,
        "phone": payload.get("phone"),
        "email": payload.get("email"),
        "notes": payload.get("notes"),
        "category_ids": category_ids,
        "active": True,
    }
    _id = db.vendors.insert_one(doc).inserted_id
    return serialize(db.vendors.find_one({"_id": _id}))


@app.patch("/vendors/{vendor_id}")
def update_vendor(vendor_id: str, payload: dict, _: dict = Depends(require_admin)):
    vendor = db.vendors.find_one({"_id": oid(vendor_id)})
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")

    updates = {}
    if "name" in payload:
        name = (payload.get("name") or "").strip()
        if len(name) < 2:
            raise HTTPException(status_code=400, detail="name is required")
        dup = db.vendors.find_one({"name": name, "_id": {"$ne": oid(vendor_id)}})
        if dup:
            raise HTTPException(status_code=409, detail="Vendor already exists")
        updates["name"] = name

    for field in ("phone", "email", "notes"):
        if field in payload:
            updates[field] = payload.get(field)

    if "category_ids" in payload:
        category_ids = payload.get("category_ids") or []
        for cid in category_ids:
            if not db.categories.find_one({"_id": oid(cid), "active": True}):
                raise HTTPException(status_code=400, detail=f"Invalid category_id: {cid}")
        updates["category_ids"] = category_ids

    if "active" in payload:
        updates["active"] = bool(payload.get("active"))

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    db.vendors.update_one({"_id": oid(vendor_id)}, {"$set": updates})
    return serialize(db.vendors.find_one({"_id": oid(vendor_id)}))


@app.delete("/vendors/{vendor_id}")
def delete_vendor(vendor_id: str, _: dict = Depends(require_admin)):
    result = db.vendors.delete_one({"_id": oid(vendor_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Vendor not found")
    db.transactions.update_many({"vendor_id": vendor_id}, {"$set": {"vendor_id": None}})
    return {"ok": True}


@app.post("/vendors/{vendor_id}/categories")
def set_vendor_categories(vendor_id: str, category_ids: list[str], _: dict = Depends(require_admin)):
    if not db.vendors.find_one({"_id": oid(vendor_id)}):
        raise HTTPException(status_code=404, detail="Vendor not found")
    for cid in category_ids:
        if not db.categories.find_one({"_id": oid(cid), "active": True}):
            raise HTTPException(status_code=400, detail=f"Invalid category_id: {cid}")
    db.vendors.update_one({"_id": oid(vendor_id)}, {"$set": {"category_ids": category_ids}})
    return serialize(db.vendors.find_one({"_id": oid(vendor_id)}))


# ---------- transactions ----------
@app.post("/transactions")
def create_transaction(payload: dict, _: dict = Depends(require_admin)):
    ttype = payload.get("type")
    if ttype not in ("INCOME", "EXPENSE"):
        raise HTTPException(status_code=400, detail="type must be INCOME or EXPENSE")

    d = payload.get("date")
    if not d:
        raise HTTPException(status_code=400, detail="date is required")
    if isinstance(d, str):
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
    else:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    try:
        amount = float(payload.get("amount"))
    except Exception:
        raise HTTPException(status_code=400, detail="amount is required")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")

    category_id = payload.get("category_id")
    vendor_id = payload.get("vendor_id")

    if ttype == "EXPENSE":
        if not category_id or not vendor_id:
            raise HTTPException(status_code=400, detail="EXPENSE requires category_id and vendor_id")
        if not db.categories.find_one({"_id": oid(category_id), "active": True}):
            raise HTTPException(status_code=400, detail="Invalid category_id")
        if not db.vendors.find_one({"_id": oid(vendor_id), "active": True}):
            raise HTTPException(status_code=400, detail="Invalid vendor_id")

    doc = {
        "type": ttype,
        "date": d,
        "amount": amount,
        "category_id": category_id,
        "vendor_id": vendor_id,
        "description": payload.get("description"),
        "reference": payload.get("reference"),
        "created_at": datetime.utcnow().isoformat(),
    }
    _id = db.transactions.insert_one(doc).inserted_id
    return serialize(db.transactions.find_one({"_id": _id}))


@app.patch("/transactions/{transaction_id}")
def update_transaction(transaction_id: str, payload: dict, _: dict = Depends(require_admin)):
    tx = db.transactions.find_one({"_id": oid(transaction_id)})
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")

    updates = {}
    if "date" in payload:
        d = payload.get("date")
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
        updates["date"] = d

    if "amount" in payload:
        try:
            amount = float(payload.get("amount"))
        except Exception:
            raise HTTPException(status_code=400, detail="amount is required")
        if amount <= 0:
            raise HTTPException(status_code=400, detail="amount must be > 0")
        updates["amount"] = amount

    if "type" in payload:
        ttype = payload.get("type")
        if ttype not in ("INCOME", "EXPENSE"):
            raise HTTPException(status_code=400, detail="type must be INCOME or EXPENSE")
        updates["type"] = ttype

    if "category_id" in payload:
        cid = payload.get("category_id")
        if cid and not db.categories.find_one({"_id": oid(cid), "active": True}):
            raise HTTPException(status_code=400, detail="Invalid category_id")
        updates["category_id"] = cid

    if "vendor_id" in payload:
        vid = payload.get("vendor_id")
        if vid and not db.vendors.find_one({"_id": oid(vid), "active": True}):
            raise HTTPException(status_code=400, detail="Invalid vendor_id")
        updates["vendor_id"] = vid

    for field in ("description", "reference"):
        if field in payload:
            updates[field] = payload.get(field)

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    merged = dict(tx)
    merged.update(updates)
    if merged.get("type") == "EXPENSE" and (not merged.get("category_id") or not merged.get("vendor_id")):
        raise HTTPException(status_code=400, detail="EXPENSE requires category_id and vendor_id")

    db.transactions.update_one({"_id": oid(transaction_id)}, {"$set": updates})
    return serialize(db.transactions.find_one({"_id": oid(transaction_id)}))


@app.delete("/transactions/{transaction_id}")
def delete_transaction(transaction_id: str, _: dict = Depends(require_admin)):
    result = db.transactions.delete_one({"_id": oid(transaction_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return {"ok": True}


@app.get("/transactions")
def list_transactions(
    type: str | None = None,
    category_id: str | None = None,
    vendor_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 200,
    _: dict = Depends(require_authenticated),
):
    q = {}
    if type:
        q["type"] = type
    if category_id:
        q["category_id"] = category_id
    if vendor_id:
        q["vendor_id"] = vendor_id
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to

    cur = db.transactions.find(q).sort([("date", -1), ("created_at", -1)]).limit(min(limit, 500))
    return [serialize(t) for t in cur]


@app.get("/stats/spend-by-category")
def spend_by_category(
    date_from: str | None = None,
    date_to: str | None = None,
    vendor_id: str | None = None,
    _: dict = Depends(require_authenticated),
):
    match = {"type": "EXPENSE"}
    if vendor_id:
        match["vendor_id"] = vendor_id
    if date_from or date_to:
        match["date"] = {}
        if date_from:
            match["date"]["$gte"] = date_from
        if date_to:
            match["date"]["$lte"] = date_to

    pipeline = [
        {"$match": match},
        {"$group": {"_id": "$category_id", "amount": {"$sum": "$amount"}}},
        {"$sort": {"amount": -1}},
    ]
    rows = list(db.transactions.aggregate(pipeline))
    total = sum(float(r["amount"]) for r in rows) if rows else 0.0

    cat_ids = [oid(r["_id"]) for r in rows if r.get("_id")]
    cats = {}
    if cat_ids:
        for c in db.categories.find({"_id": {"$in": cat_ids}}, {"name": 1}):
            cats[str(c["_id"])] = c["name"]

    out = []
    for r in rows:
        cid = r["_id"]
        amt = float(r["amount"])
        out.append(
            {
                "category_id": cid,
                "category_name": cats.get(cid, "(Sin categoría)"),
                "amount": round(amt, 2),
                "percent": round((amt / total * 100.0), 2) if total > 0 else 0.0,
            }
        )
    return {"total_expenses": round(total, 2), "rows": out}
