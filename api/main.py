from fastapi import FastAPI, HTTPException, Response, Depends, UploadFile, File, Query
from fastapi import Header
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, UpdateOne
from pymongo.errors import OperationFailure
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


def serialize_transaction_with_supplier(tx: dict, suppliers_by_id: dict[str, dict] | None = None):
    tx_doc = serialize(tx)
    supplier = None
    supplier_id = tx_doc.get("supplierId")

    if supplier_id and suppliers_by_id:
        supplier = suppliers_by_id.get(supplier_id)

    supplier_name = tx_doc.get("supplierName") or (supplier or {}).get("name") or ""
    supplier_card_code = tx_doc.get("supplierCardCode") or (supplier or {}).get("cardCode") or ""

    tx_doc["proveedorNombre"] = supplier_name
    tx_doc["proveedorCardCode"] = supplier_card_code

    if supplier:
        tx_doc["proveedor"] = serialize(supplier)

    return tx_doc


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
        [
            ("projectId", 1),
            ("source", 1),
            ("sourceDb", 1),
            ("sap.pagoNum", 1),
            ("sap.facturaNum", 1),
            ("sap.montoAplicado", 1),
        ],
        unique=True,
        partialFilterExpression={"source": "sap"},
    )
    db.transactions.create_index([("projectId", 1), ("date", -1)])
    text_index_name = "transactions_text_search"
    current_indexes = {idx.get("name"): idx for idx in db.transactions.list_indexes()}
    existing_text_index = current_indexes.get(text_index_name)
    expected_text_fields = [
        ("description", "text"),
        ("concept", "text"),
        ("supplierName", "text"),
        ("beneficiario", "text"),
    ]

    if existing_text_index and existing_text_index.get("key") != dict(expected_text_fields):
        db.transactions.drop_index(text_index_name)

    try:
        db.transactions.create_index(
            expected_text_fields,
            name=text_index_name,
            default_language="spanish",
        )
    except OperationFailure:
        # Si el índice de texto no puede crearse por restricciones del dataset,
        # se mantiene fallback de búsqueda por regex en el endpoint.
        pass


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
    if re.fullmatch(r"[A-Za-z]{3}", text):
        raise ValueError(f"Invalid number: {value}")

    normalized = text.replace("$", "").replace(" ", "").replace(",", "")
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"

    try:
        return round(float(Decimal(normalized)), 2)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid number: {value}") from exc


def parse_optional_decimal(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return parse_decimal(value)


def compute_monto_sin_iva(tx: dict):
    amount = round(float(tx.get("amount") or 0), 2)
    tax = tx.get("tax") if isinstance(tx.get("tax"), dict) else None
    if tax is None:
        return round(amount - compute_monto_iva(tx), 2)

    subtotal = tax.get("subtotal")
    total_factura = tax.get("totalFactura")

    try:
        subtotal_value = float(subtotal)
        total_factura_value = float(total_factura)
    except (TypeError, ValueError):
        return round(amount - compute_monto_iva(tx), 2)

    if total_factura_value == 0:
        return round(amount - compute_monto_iva(tx), 2)

    sign = -1 if amount < 0 else 1
    proporcional = round(subtotal_value * (abs(amount) / total_factura_value), 2)
    return round(sign * proporcional, 2)


def compute_monto_iva(tx: dict):
    amount = round(float(tx.get("amount") or 0), 2)
    tax = tx.get("tax") if isinstance(tx.get("tax"), dict) else None
    if tax is None:
        return 0.0

    iva = tax.get("iva")
    total_factura = tax.get("totalFactura")

    try:
        iva_value = float(iva)
        total_factura_value = float(total_factura)
    except (TypeError, ValueError):
        return 0.0

    if total_factura_value == 0:
        return 0.0

    sign = -1 if amount < 0 else 1
    proporcional = round(iva_value * (abs(amount) / total_factura_value), 2)
    return round(sign * proporcional, 2)


def build_transactions_query(
    type_value: str | None = None,
    category_id: str | None = None,
    vendor_id: str | None = None,
    supplier_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    project_id: str | None = None,
    origen: str | None = None,
    source: str | None = None,
    source_db: str | None = None,
    search_query: str | None = None,
):
    q = {}
    if type_value:
        q["type"] = type_value
    if category_id:
        q["category_id"] = category_id
    if vendor_id:
        q["vendor_id"] = vendor_id
    if supplier_id:
        q["supplierId"] = supplier_id
    if project_id:
        q["projectId"] = project_id
    if origen:
        q["source"] = origen
    if source:
        q["source"] = source
    if source_db:
        q["sourceDb"] = source_db
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to

    cleaned_search = (search_query or "").strip()
    if cleaned_search:
        escaped_search = re.escape(cleaned_search)
        q["$or"] = [
            {"description": {"$regex": escaped_search, "$options": "i"}},
            {"concept": {"$regex": escaped_search, "$options": "i"}},
            {"supplierName": {"$regex": escaped_search, "$options": "i"}},
            {"beneficiario": {"$regex": escaped_search, "$options": "i"}},
        ]
    return q


def build_transaction_totals(match_query: dict, search_query: str | None = None):
    aggregate_match = dict(match_query)
    cleaned_search = (search_query or "").strip()
    use_text_search = False

    if cleaned_search and "$or" in aggregate_match:
        aggregate_match.pop("$or", None)
        aggregate_match["$text"] = {"$search": cleaned_search}
        use_text_search = True

    pipeline = [
        {"$match": aggregate_match},
        {
            "$project": {
                "type": 1,
                "amount": {"$ifNull": ["$amount", 0]},
                "montoIva": {
                    "$let": {
                        "vars": {
                            "iva": {"$convert": {"input": "$tax.iva", "to": "double", "onError": None, "onNull": None}},
                            "totalFactura": {"$convert": {"input": "$tax.totalFactura", "to": "double", "onError": None, "onNull": None}},
                            "amountValue": {"$ifNull": ["$amount", 0]},
                        },
                        "in": {
                            "$cond": [
                                {"$or": [{"$eq": ["$$iva", None]}, {"$eq": ["$$totalFactura", None]}, {"$eq": ["$$totalFactura", 0]}]},
                                0,
                                {
                                    "$round": [
                                        {
                                            "$multiply": [
                                                {"$cond": [{"$lt": ["$$amountValue", 0]}, -1, 1]},
                                                "$$iva",
                                                {"$divide": [{"$abs": "$$amountValue"}, "$$totalFactura"]},
                                            ]
                                        },
                                        2,
                                    ]
                                },
                            ]
                        },
                    }
                },
                "montoSinIva": {
                    "$let": {
                        "vars": {
                            "subtotal": {"$convert": {"input": "$tax.subtotal", "to": "double", "onError": None, "onNull": None}},
                            "totalFactura": {"$convert": {"input": "$tax.totalFactura", "to": "double", "onError": None, "onNull": None}},
                            "amountValue": {"$ifNull": ["$amount", 0]},
                        },
                        "in": {
                            "$cond": [
                                {"$or": [{"$eq": ["$$subtotal", None]}, {"$eq": ["$$totalFactura", None]}, {"$eq": ["$$totalFactura", 0]}]},
                                {
                                    "$round": [
                                        {
                                            "$subtract": [
                                                "$$amountValue",
                                                {
                                                    "$let": {
                                                        "vars": {
                                                            "iva": {"$convert": {"input": "$tax.iva", "to": "double", "onError": None, "onNull": None}},
                                                            "totalFacturaIva": {"$convert": {"input": "$tax.totalFactura", "to": "double", "onError": None, "onNull": None}},
                                                        },
                                                        "in": {
                                                            "$cond": [
                                                                {"$or": [{"$eq": ["$$iva", None]}, {"$eq": ["$$totalFacturaIva", None]}, {"$eq": ["$$totalFacturaIva", 0]}]},
                                                                0,
                                                                {
                                                                    "$round": [
                                                                        {
                                                                            "$multiply": [
                                                                                {"$cond": [{"$lt": ["$$amountValue", 0]}, -1, 1]},
                                                                                "$$iva",
                                                                                {"$divide": [{"$abs": "$$amountValue"}, "$$totalFacturaIva"]},
                                                                            ]
                                                                        },
                                                                        2,
                                                                    ]
                                                                },
                                                            ]
                                                        },
                                                    }
                                                },
                                            ]
                                        },
                                        2,
                                    ]
                                },
                                {
                                    "$round": [
                                        {
                                            "$multiply": [
                                                {"$cond": [{"$lt": ["$$amountValue", 0]}, -1, 1]},
                                                "$$subtotal",
                                                {"$divide": [{"$abs": "$$amountValue"}, "$$totalFactura"]},
                                            ]
                                        },
                                        2,
                                    ]
                                },
                            ]
                        },
                    }
                },
            }
        },
        {
            "$group": {
                "_id": None,
                "expensesGross": {
                    "$sum": {"$cond": [{"$eq": ["$type", "EXPENSE"]}, "$amount", 0]}
                },
                "expensesTax": {
                    "$sum": {"$cond": [{"$eq": ["$type", "EXPENSE"]}, "$montoIva", 0]}
                },
                "expensesWithoutTax": {
                    "$sum": {"$cond": [{"$eq": ["$type", "EXPENSE"]}, "$montoSinIva", 0]}
                },
                "incomeGross": {
                    "$sum": {"$cond": [{"$eq": ["$type", "INCOME"]}, "$amount", 0]}
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "expensesGross": {"$round": ["$expensesGross", 2]},
                "expensesTax": {"$round": ["$expensesTax", 2]},
                "expensesWithoutTax": {"$round": ["$expensesWithoutTax", 2]},
                "incomeGross": {"$round": ["$incomeGross", 2]},
                "net": {"$round": [{"$subtract": ["$incomeGross", "$expensesWithoutTax"]}, 2]},
            }
        },
    ]

    try:
        rows = list(db.transactions.aggregate(pipeline))
    except OperationFailure:
        if use_text_search:
            return build_transaction_totals(match_query, search_query=None)
        rows = []

    if rows:
        return rows[0]

    return {
        "expensesGross": 0.0,
        "expensesTax": 0.0,
        "expensesWithoutTax": 0.0,
        "incomeGross": 0.0,
        "net": 0.0,
    }


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
    canonical_headers = [
        "pagonum",
        "fechapago",
        "cardcode",
        "beneficiario",
        "moneda",
        "totalpago",
        "conceptopago",
        "facturaproveedornum",
        "fechafactura",
        "montoaplicado",
    ]
    optional_tax_headers = ["subtotal", "iva", "retenciones", "totalfactura"]
    optional_movement_headers = ["sourcedb"]
    canonical_to_expected = dict(zip(canonical_headers, expected_headers))
    header_aliases = {
        "docnum": "pagonum",
        "cardname": "beneficiario",
        "doccurr": "moneda",
        "doctotal": "totalpago",
        "comments": "conceptopago",
        "facturanum": "facturaproveedornum",
        "facturaproveedor": "facturaproveedornum",
        "nrofactura": "facturaproveedornum",
        "numfactura": "facturaproveedornum",
        "facturaprov": "facturaproveedornum",
        "facturaprove": "facturaproveedornum",
        "fechafactur": "fechafactura",
        "montoaplica": "montoaplicado",
        "impuesto": "iva",
        "apvatsum": "iva",
        "apdoctotal": "totalfactura",
        "pch1linetotal": "subtotal",
    }

    def normalize_header(header_value):
        if header_value is None:
            return ""
        return re.sub(r"[^a-z0-9]", "", str(header_value).replace("\ufeff", "").strip().lower())

    def build_header_index(raw_headers):
        found_headers_normalized = [normalize_header(h) for h in raw_headers]
        has_fechafactura = any(h == "fechafactura" for h in found_headers_normalized)

        header_index = {}
        for idx, normalized in enumerate(found_headers_normalized):
            canonical = header_aliases.get(normalized, normalized)
            if normalized == "fecha" and not has_fechafactura:
                canonical = "fechapago"
            if canonical in (canonical_headers + optional_tax_headers + optional_movement_headers) and canonical not in header_index:
                header_index[canonical] = idx

        missing = [h for h in canonical_headers if h not in header_index]
        if missing:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid headers",
                    "missing": missing,
                    "foundHeadersNormalized": found_headers_normalized,
                    "foundHeadersRaw": ["" if h is None else str(h) for h in raw_headers],
                },
            )

        return header_index

    currency_codes = {"MXP", "MXN", "USD", "EUR", "CAD", "GBP"}

    def looks_like_integer(value):
        return bool(re.fullmatch(r"\d+", str(value or "").strip()))

    def looks_like_date(value):
        try:
            return normalizeDate(value) is not None
        except ValueError:
            return False

    def repair_csv_row(values):
        if len(values) < 7:
            raise ValueError("Unrepairable CSV row")

        repaired = [None] * len(expected_headers)
        repaired[0] = values[0]
        repaired[1] = values[1] if len(values) > 1 else None
        repaired[2] = values[2] if len(values) > 2 else None

        currency_idx = None
        for idx in range(3, len(values)):
            token = str(values[idx] or "").strip().upper()
            if token in currency_codes:
                currency_idx = idx
                break

        if currency_idx is None or currency_idx <= 3:
            raise ValueError("Unrepairable CSV row")

        repaired[3] = ",".join(values[3:currency_idx]).strip()
        repaired[4] = values[currency_idx]

        total_idx = currency_idx + 1
        if total_idx >= len(values):
            raise ValueError("Unrepairable CSV row")

        parse_decimal(values[total_idx])
        repaired[5] = values[total_idx]

        factura_idx = None
        for idx in range(len(values) - 3, total_idx, -1):
            if looks_like_integer(values[idx]) and looks_like_date(values[idx + 1]):
                try:
                    parse_decimal(values[-1])
                except ValueError as exc:
                    raise ValueError("Unrepairable CSV row") from exc
                factura_idx = idx
                break

        if factura_idx is None:
            raise ValueError("Unrepairable CSV row")

        repaired[6] = ",".join(values[total_idx + 1:factura_idx]).strip()
        repaired[7] = values[factura_idx]
        repaired[8] = values[factura_idx + 1]
        repaired[9] = values[-1]

        if any(repaired[idx] is None for idx in (4, 7, 8, 9)):
            raise ValueError("Unrepairable CSV row")

        return repaired

    if file_name.lower().endswith(".csv"):
        decoded = file_bytes.decode("utf-8-sig")
        reader = csv.reader(decoded.splitlines())
        rows = list(reader)
        if not rows:
            return []

        header_index = build_header_index(rows[0])

        parsed = []
        required_column_count = max(header_index.values()) + 1
        standard_layout = all(header_index[h] == idx for idx, h in enumerate(canonical_headers))
        for values in rows[1:]:
            row_values = values
            should_try_repair = (
                standard_layout
                and len(rows[0]) == len(expected_headers)
                and len(values) != len(rows[0])
            )
            if should_try_repair:
                row_values = repair_csv_row(values)
            elif len(values) < required_column_count:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid row length. Expected at least {required_column_count} columns, found {len(values)}",
                )

            row_dict = {}
            for canonical in canonical_headers:
                expected = canonical_to_expected[canonical]
                source_idx = header_index[canonical]
                row_dict[expected] = row_values[source_idx] if source_idx < len(row_values) else None
            for tax_key in optional_tax_headers:
                source_idx = header_index.get(tax_key)
                row_dict[tax_key] = row_values[source_idx] if source_idx is not None and source_idx < len(row_values) else None
            source_db_idx = header_index.get("sourcedb")
            row_dict["sourceDb"] = row_values[source_db_idx] if source_db_idx is not None and source_db_idx < len(row_values) else None

            if should_try_repair:
                row_dict["__csvRepairApplied"] = len(values) > len(rows[0])
                row_dict["__csvOriginalFieldCount"] = len(values)

            parsed.append(row_dict)

        return parsed

    if file_name.lower().endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        header_index = build_header_index(rows[0])

        parsed = []
        for values in rows[1:]:
            row_dict = {}
            for canonical in canonical_headers:
                expected = canonical_to_expected[canonical]
                source_idx = header_index[canonical]
                row_dict[expected] = values[source_idx] if source_idx < len(values) else None
            for tax_key in optional_tax_headers:
                source_idx = header_index.get(tax_key)
                row_dict[tax_key] = values[source_idx] if source_idx is not None and source_idx < len(values) else None
            source_db_idx = header_index.get("sourcedb")
            row_dict["sourceDb"] = values[source_db_idx] if source_db_idx is not None and source_db_idx < len(values) else None
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


def build_sap_vendor_upsert(card_code: str, beneficiary: str, project_id: str):
    return UpdateOne(
        {"source": "sap", "externalIds.sapCardCode": card_code},
        {
            "$set": {
                "name": beneficiary or card_code,
                "source": "sap",
                "externalIds.sapCardCode": card_code,
                "categoryId": None,
                "projectId": project_id,
                "active": True,
            },
            "$setOnInsert": {
                "category_ids": [],
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            "$addToSet": {"projectIds": project_id},
        },
        upsert=True,
    )


def sync_suppliers_into_vendors(suppliers: list[dict], project_id: str):
    if not suppliers:
        return 0

    vendor_ops = []
    for supplier in suppliers:
        card_code = str(supplier.get("cardCode") or "").strip()
        if not card_code:
            continue
        beneficiary = str(supplier.get("name") or "").strip()
        vendor_ops.append(build_sap_vendor_upsert(card_code, beneficiary, project_id))

    if not vendor_ops:
        return 0

    result = db.vendors.bulk_write(vendor_ops, ordered=False)
    return (result.upserted_count or 0) + (result.modified_count or 0)


def get_or_create_project_id(project: str):
    project_name = project.strip() or "CALDERON DE LA BARCA"
    project_doc = db.projects.find_one_and_update(
        {"name": project_name},
        {"$setOnInsert": {"name": project_name}},
        upsert=True,
    )
    if not project_doc:
        project_doc = db.projects.find_one({"name": project_name})
    return str(project_doc["_id"])


def run_sap_import(
    file_name: str,
    file_bytes: bytes,
    project: str,
    force: int,
    source: str = "sap-payments",
    mode: str = "upsert",
    confirm_rebuild: int = 0,
    allow_rebuild: bool = False,
):
    file_hash = sha256(file_bytes).hexdigest()

    existing_run = db.importRuns.find_one({"sha256": file_hash})
    existing_ok_run = existing_run and existing_run.get("status") == "ok"

    if existing_ok_run and force != 1:
        return {"already_imported": True, "importRunId": str(existing_run["_id"])}

    now = datetime.now(timezone.utc).isoformat()
    project_id = get_or_create_project_id(project)
    import_mode = (mode or "upsert").strip().lower()

    if import_mode not in ("upsert", "rebuild"):
        raise HTTPException(status_code=400, detail="Invalid mode. Use 'upsert' or 'rebuild'.")

    if import_mode == "rebuild":
        if not allow_rebuild:
            raise HTTPException(status_code=403, detail="Rebuild mode requires ADMIN role.")
        if confirm_rebuild != 1:
            raise HTTPException(
                status_code=400,
                detail="Rebuild mode requires explicit confirmation (?confirm_rebuild=1).",
            )
        db.transactions.delete_many({"projectId": project_id, "source": "sap"})

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
    suppliers_seen = {}
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

            subtotal = parse_optional_decimal(row.get("subtotal"))
            iva = parse_optional_decimal(row.get("iva"))
            retenciones = parse_optional_decimal(row.get("retenciones"))
            total_factura = parse_optional_decimal(row.get("totalfactura"))
            source_db = str(row.get("sourceDb") or "").strip() or "SAP"

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
            suppliers_seen[card_code] = beneficiary or card_code

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
                    "tax": {
                        "subtotal": subtotal,
                        "iva": iva,
                        "retenciones": retenciones,
                        "totalFactura": total_factura,
                    },
                    "sourceDb": source_db,
                }
            )
            rows_ok += 1
        except Exception as exc:
            rows_error += 1
            if len(errors_sample) < 50:
                errors_sample.append({"row": idx, "error": str(exc)})

    if suppliers_ops:
        db.suppliers.bulk_write(suppliers_ops, ordered=False)

    vendors_synced = sync_suppliers_into_vendors(
        [{"cardCode": card_code, "name": name} for card_code, name in suppliers_seen.items()],
        project_id,
    )

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
                    "tax": record["tax"],
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
                    "tax": record["tax"],
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
                "supplierName": record["beneficiary"] or record["cardCode"],
                "supplierCardCode": record["cardCode"],
                "categoryId": None,
                "category_id": None,
                "vendor_id": None,
                "source": "sap",
                "sourceDb": record["sourceDb"],
                "tax": record["tax"],
                "sap": {
                    "pagoNum": record["paymentNum"],
                    "facturaNum": record["invoiceNum"],
                    "montoAplicado": record["appliedAmount"],
                    "cardCode": record["cardCode"],
                },
            }
            sap_expense_ops.append(
                UpdateOne(
                    {
                        "projectId": project_id,
                        "source": "sap",
                        "sourceDb": record["sourceDb"],
                        "sap.pagoNum": record["paymentNum"],
                        "sap.facturaNum": record["invoiceNum"],
                        "sap.montoAplicado": record["appliedAmount"],
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
        "vendorsSynced": vendors_synced,
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
    mode: str = "upsert",
    confirm_rebuild: int = 0,
    admin_user: dict = Depends(require_admin),
):
    file_bytes = await file.read()
    return run_sap_import(
        file.filename or "",
        file_bytes,
        project,
        force,
        source="sap-payments",
        mode=mode,
        confirm_rebuild=confirm_rebuild,
        allow_rebuild=admin_user["role"] == "ADMIN",
    )


@app.post("/api/cron/import/sap-payments")
def cron_import_sap_payments(project: str = "CALDERON DE LA BARCA", force: int = 0, mode: str = "upsert"):
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

    return run_sap_import(file_name, file_bytes, project, force, source="sap-payments-cron", mode=mode)


@app.get("/api/expenses/summary-by-supplier")
def summary_expenses_by_supplier(
    project: str = "CALDERON DE LA BARCA",
    _: dict = Depends(require_authenticated),
):
    project_name = (project or "").strip() or "CALDERON DE LA BARCA"
    project_doc = db.projects.find_one({"name": project_name})
    if not project_doc:
        return []

    project_id = str(project_doc["_id"])
    pipeline = [
        {"$match": {"type": "EXPENSE", "projectId": project_id}},
        {"$group": {"_id": "$supplierId", "totalAmount": {"$sum": "$amount"}, "count": {"$sum": 1}}},
    ]
    rows = list(db.transactions.aggregate(pipeline))

    supplier_ids = [oid(row["_id"]) for row in rows if row.get("_id")]
    supplier_names = {}
    if supplier_ids:
        for supplier in db.suppliers.find({"_id": {"$in": supplier_ids}}, {"name": 1}):
            supplier_names[str(supplier["_id"])] = supplier.get("name") or "(Sin proveedor)"

    output = [
        {
            "supplierId": row.get("_id"),
            "supplierName": supplier_names.get(row.get("_id"), "(Sin proveedor)"),
            "totalAmount": round(float(row.get("totalAmount") or 0), 2),
            "count": int(row.get("count") or 0),
        }
        for row in rows
    ]

    output.sort(key=lambda item: (item["supplierName"] or "").lower())
    return output
@app.post("/api/admin/backfill/suppliers-to-vendors")
def backfill_suppliers_to_vendors(project: str = "CALDERON DE LA BARCA", _: dict = Depends(require_admin)):
    project_id = get_or_create_project_id(project)
    suppliers = list(db.suppliers.find({}, {"cardCode": 1, "name": 1}))
    vendors_synced = sync_suppliers_into_vendors(suppliers, project_id)
    return {
        "projectId": project_id,
        "suppliersScanned": len(suppliers),
        "vendorsSynced": vendors_synced,
    }


@app.post("/api/admin/backfill/tax-fields")
def backfill_tax_fields(project: str = "CALDERON DE LA BARCA", _: dict = Depends(require_admin)):
    project_id = get_or_create_project_id(project)
    scanned = 0
    updated = 0

    query = {
        "projectId": project_id,
        "source": "sap",
        "$or": [
            {"tax": {"$exists": False}},
            {"tax": None},
        ],
    }

    for movement in db.transactions.find(query, {"sap": 1, "amount": 1}):
        scanned += 1
        sap_payload = movement.get("sap") if isinstance(movement.get("sap"), dict) else {}
        payment_num = str(sap_payload.get("pagoNum") or "").strip()
        invoice_num = str(sap_payload.get("facturaNum") or "").strip()

        ap_invoice = db.apInvoices.find_one(
            {"projectId": project_id, "sapInvoiceNum": invoice_num},
            {"tax": 1},
        ) if invoice_num else None
        payment = db.payments.find_one(
            {"projectId": project_id, "sapPaymentNum": payment_num},
            {"tax": 1},
        ) if payment_num else None

        source_tax = None
        if isinstance(ap_invoice, dict) and isinstance(ap_invoice.get("tax"), dict):
            source_tax = ap_invoice.get("tax")
        elif isinstance(payment, dict) and isinstance(payment.get("tax"), dict):
            source_tax = payment.get("tax")
        elif isinstance(sap_payload.get("tax"), dict):
            source_tax = sap_payload.get("tax")

        tax_doc = {
            "subtotal": parse_optional_decimal((source_tax or {}).get("subtotal")),
            "iva": parse_optional_decimal((source_tax or {}).get("iva")),
            "retenciones": parse_optional_decimal((source_tax or {}).get("retenciones")),
            "totalFactura": parse_optional_decimal((source_tax or {}).get("totalFactura")),
        }

        result = db.transactions.update_one(
            {"_id": movement["_id"]},
            {"$set": {"tax": tax_doc}},
        )
        if result.modified_count:
            updated += 1

    return {"projectId": project_id, "scanned": scanned, "updated": updated}


@app.post("/api/admin/backfill/supplierName")
def backfill_supplier_name(_: dict = Depends(require_admin)):
    scanned = 0
    updated = 0

    query = {
        "source": "sap",
        "$or": [
            {"supplierName": {"$exists": False}},
            {"supplierName": None},
            {"supplierName": ""},
        ],
    }

    for movement in db.transactions.find(query, {"supplierId": 1, "sap": 1}):
        scanned += 1
        supplier = None

        supplier_id = movement.get("supplierId")
        if supplier_id:
            try:
                supplier = db.suppliers.find_one({"_id": oid(supplier_id)}, {"name": 1, "cardCode": 1})
            except HTTPException:
                supplier = None

        if not supplier:
            sap_card_code = str((movement.get("sap") or {}).get("cardCode") or "").strip()
            if sap_card_code:
                supplier = db.suppliers.find_one({"cardCode": sap_card_code}, {"name": 1, "cardCode": 1})

        if not supplier:
            continue

        result = db.transactions.update_one(
            {"_id": movement["_id"]},
            {
                "$set": {
                    "supplierName": supplier.get("name") or "",
                    "supplierCardCode": supplier.get("cardCode") or "",
                    "supplierId": str(supplier.get("_id")),
                }
            },
        )
        if result.modified_count:
            updated += 1

    return {"scanned": scanned, "updated": updated}


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
def list_vendors(
    active_only: bool = True,
    category_id: str | None = None,
    include_sap: bool = True,
    _: dict = Depends(require_authenticated),
):
    q = {"active": True} if active_only else {}
    if category_id:
        q["category_ids"] = category_id
    if include_sap:
        q["$or"] = [{"source": {"$exists": False}}, {"source": "manual"}, {"source": "sap"}]
    else:
        q["$or"] = [{"source": {"$exists": False}}, {"source": "manual"}]
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
        if tx.get("source") != "sap":
            raise HTTPException(status_code=400, detail="EXPENSE requires category_id and vendor_id")

    db.transactions.update_one({"_id": oid(transaction_id)}, {"$set": updates})

    category_id = updates.get("category_id")
    existing_category_id = tx.get("category_id") or tx.get("categoryId")
    should_apply_category_to_related = bool(category_id and not existing_category_id)

    if should_apply_category_to_related:
        supplier_id = tx.get("supplierId")
        if supplier_id:
            db.transactions.update_many(
                {"supplierId": supplier_id, "type": "EXPENSE"},
                {"$set": {"category_id": category_id, "categoryId": category_id}},
            )
        elif tx.get("vendor_id"):
            db.transactions.update_many(
                {"vendor_id": tx.get("vendor_id"), "type": "EXPENSE"},
                {"$set": {"category_id": category_id}},
            )

    return serialize(db.transactions.find_one({"_id": oid(transaction_id)}))


@app.delete("/transactions/{transaction_id}")
def delete_transaction(transaction_id: str, _: dict = Depends(require_admin)):
    result = db.transactions.delete_one({"_id": oid(transaction_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return {"ok": True}


@app.get("/transactions")
@app.get("/api/transactions")
@app.get("/api/movimientos")
def list_transactions(
    type: str | None = None,
    tipo: str | None = None,
    category_id: str | None = None,
    vendor_id: str | None = None,
    supplierId: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    projectId: str | None = None,
    origen: str | None = None,
    source: str | None = None,
    sourceDb: str | None = None,
    q: str | None = None,
    page: int = 1,
    limit: int = 50,
    _: dict = Depends(require_authenticated),
):
    normalized_page = max(page, 1)
    normalized_limit = min(max(limit, 1), 500)
    skip = (normalized_page - 1) * normalized_limit
    effective_date_from = from_date or date_from
    effective_date_to = to_date or date_to
    effective_type = type or tipo
    match_query = build_transactions_query(
        type_value=effective_type,
        category_id=category_id,
        vendor_id=vendor_id,
        supplier_id=supplierId,
        date_from=effective_date_from,
        date_to=effective_date_to,
        project_id=projectId,
        origen=origen,
        source=source,
        source_db=sourceDb,
        search_query=q,
    )

    total_count = db.transactions.count_documents(match_query)
    totals = build_transaction_totals(match_query, search_query=q)

    txs = list(
        db.transactions.find(match_query)
        .sort([("date", -1), ("_id", -1)])
        .skip(skip)
        .limit(normalized_limit)
    )

    supplier_ids = []
    for tx in txs:
        supplier_id = tx.get("supplierId")
        if supplier_id:
            try:
                supplier_ids.append(oid(supplier_id))
            except HTTPException:
                continue

    suppliers_by_id = {}
    if supplier_ids:
        for supplier in db.suppliers.find({"_id": {"$in": supplier_ids}}, {"name": 1, "cardCode": 1}):
            suppliers_by_id[str(supplier["_id"])] = supplier

    items = []
    for tx in txs:
        monto_iva = compute_monto_iva(tx)
        monto_sin_iva = compute_monto_sin_iva(tx)
        tx_doc = serialize_transaction_with_supplier(tx, suppliers_by_id)
        tx_doc["montoIva"] = monto_iva
        tx_doc["ivaAplicado"] = monto_iva
        tx_doc["montoSinIva"] = monto_sin_iva
        items.append(tx_doc)

    return {
        "items": items,
        "page": normalized_page,
        "limit": normalized_limit,
        "totalCount": total_count,
        "totals": totals,
    }


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
