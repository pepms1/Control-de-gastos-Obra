from fastapi import FastAPI, HTTPException, Response, Depends, UploadFile, File, Query, Request as FastAPIRequest, Security, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pymongo import MongoClient, UpdateOne, ReturnDocument
from pymongo.errors import BulkWriteError, OperationFailure, DuplicateKeyError
from bson import ObjectId
from datetime import date, datetime, timedelta, timezone
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from hashlib import sha256
from decimal import Decimal, InvalidOperation
from io import BytesIO
import json
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import boto3
import re
import csv
import openpyxl
import os
import logging
import unicodedata
import threading

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
SUSPICIOUS_PROJECT_RESOLUTIONS_DB_NAME = (
    os.getenv("SUSPICIOUS_PROJECT_RESOLUTIONS_DB")
    or os.getenv("SUSPICIOUS_PROJECT_RESOLUTION_DB")
    or "control_obra_v2"
)
SUSPICIOUS_PROJECT_RESOLUTIONS_COLLECTION_NAME = (
    os.getenv("SUSPICIOUS_PROJECT_RESOLUTIONS_COLLECTION")
    or os.getenv("SUSPICIOUS_PROJECT_RESOLUTION_COLLECTION")
    or "transactions"
)
JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "12"))

client = MongoClient(MONGO_URL)
db = client[DB_NAME]
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
logger = logging.getLogger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)
USER_ROLES = ("SUPERADMIN", "ADMIN", "VIEWER")
ROLE_SCHEMA_VERSION = 2


TELEGRAM_SETTINGS_KEY = "telegram_default_chat_id"
TELEGRAM_ACCESS_REQUEST_WINDOW_HOURS = 24
DEFAULT_PROJECT_S3_BUCKET = "calderon-sap-exports"
SAP_LATEST_ADMIN_RATE_LIMIT_SECONDS = 60
sap_latest_admin_locks: dict[str, bool] = {}
sap_latest_admin_last_request_at: dict[str, datetime] = {}
sap_latest_admin_guard = threading.Lock()


# ---------- helpers ----------
def env_get(primary_key: str, fallback_key: str | None = None, default: str | None = None):
    value = os.getenv(primary_key)
    if value is None and fallback_key:
        value = os.getenv(fallback_key)
    if value is None:
        return default
    return value


def default_viewer_accounts():
    return [
        ("viewer", "viewer123", "viewer"),
        ("rvh", "rvh123", "rvh"),
        ("dms", "dms123", "dms"),
        ("rma", "rma123", "rma"),
        ("jma", "jma123", "jma"),
    ]


def get_env_auth_users():
    default_admin_user = env_get("DEFAULT_ADMIN_USERNAME", "default_admin_username", "admin")
    default_admin_pass = env_get("DEFAULT_ADMIN_PASSWORD", "default_admin_password", "admin123")
    default_admin_name = env_get("DEFAULT_ADMIN_NAME", "default_admin_name", default_admin_user)
    viewer_users_raw = env_get("VIEWER_USERS", "viewer_users", "") or ""

    auth_users = {
        default_admin_user: {"password": default_admin_pass, "role": "SUPERADMIN", "displayName": default_admin_name},
    }

    for default_username, default_password, default_display_name in default_viewer_accounts():
        viewer_username = env_get(
            f"DEFAULT_{default_username.upper()}_USERNAME",
            f"default_{default_username}_username",
            default_username,
        )
        viewer_password = env_get(
            f"DEFAULT_{default_username.upper()}_PASSWORD",
            f"default_{default_username}_password",
            default_password,
        )
        viewer_display_name = env_get(
            f"DEFAULT_{default_username.upper()}_NAME",
            f"default_{default_username}_name",
            viewer_username,
        )

        auth_users[viewer_username] = {
            "password": viewer_password,
            "role": "VIEWER",
            "displayName": viewer_display_name or default_display_name,
        }

    for entry in viewer_users_raw.split(","):
        pair = entry.strip()
        if not pair:
            continue

        parts = pair.split(":", 2)
        if len(parts) < 2:
            continue

        viewer_username = parts[0].strip()
        viewer_password = parts[1].strip()
        viewer_display_name = (parts[2].strip() if len(parts) > 2 else "") or viewer_username
        if not viewer_username or not viewer_password:
            continue

        auth_users[viewer_username] = {
            "password": viewer_password,
            "role": "VIEWER",
            "displayName": viewer_display_name,
        }

    return auth_users


def oid(s: str) -> ObjectId:
    try:
        return ObjectId(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid id: {s}")


def serialize(doc):
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


def _serialize_any(value):
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _serialize_any(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize_any(v) for v in value]
    return value


def serialize_raw_doc(doc):
    raw = dict(doc)
    raw["id"] = str(raw.pop("_id"))
    return {k: _serialize_any(v) for k, v in raw.items()}


def normalize_raw_value(value):
    if isinstance(value, dict):
        return {k: normalize_raw_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_raw_value(v) for v in value]
    if isinstance(value, str):
        trimmed = value.strip()
        if ObjectId.is_valid(trimmed):
            return ObjectId(trimmed)
    return value


def normalize_project_slug(raw_slug: str | None) -> str:
    slug = (raw_slug or "").strip().lower().replace(" ", "-")
    if not slug:
        raise HTTPException(status_code=400, detail="slug is required")
    if not re.fullmatch(r"[a-z0-9-]+", slug):
        raise HTTPException(status_code=400, detail="slug must contain only lowercase letters, numbers and dashes")
    return slug


def normalize_slug_from_raw_project_name(raw_project_name: str | None) -> str:
    value = (raw_project_name or "").strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


def normalize_text_for_matching(value: str | None) -> str:
    normalized = unicodedata.normalize("NFD", str(value or "").strip().lower())
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", normalized).strip()


TRABAJOS_ESPECIALES_PREFIX = "trabajos especiales"
TRABAJOS_ESPECIALES_UNCLASSIFIED_ID = "trabajos_especiales_unclassified"
TRABAJOS_ESPECIALES_UNCLASSIFIED_NAME = "Trabajos Especiales sin clasificar"
UNRESOLVED_CATEGORY2_ID = "sin_categoria_2"
UNRESOLVED_CATEGORY2_NAME = "Sin categoría 2"


def build_supplier_key(supplier_card_code: str | None = None, business_partner: str | None = None, supplier_name: str | None = None) -> str | None:
    normalized_card_code = normalize_non_empty_string(supplier_card_code)
    normalized_business_partner = normalize_non_empty_string(business_partner)
    if normalized_business_partner and normalized_card_code:
        normalized_bp = normalize_text_for_matching(normalized_business_partner)
        normalized_cc = normalize_text_for_matching(normalized_card_code)
        return f"bpcc:{normalized_bp}|{normalized_cc}"

    if normalized_business_partner:
        return f"bp:{normalize_text_for_matching(normalized_business_partner)}"

    if normalized_card_code:
        return f"cardcode:{normalize_text_for_matching(normalized_card_code)}"

    normalized_supplier_name = normalize_non_empty_string(supplier_name)
    if normalized_supplier_name:
        return f"name:{normalize_text_for_matching(normalized_supplier_name)}"

    return None


def build_supplier_identity_from_transaction(tx: dict) -> dict:
    sap_doc = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
    supplier_card_code = normalize_non_empty_string(tx.get("supplierCardCode") or sap_doc.get("cardCode"))
    business_partner = normalize_non_empty_string(sap_doc.get("businessPartner") or tx.get("businessPartner"))
    supplier_name = normalize_non_empty_string(
        tx.get("supplierName")
        or tx.get("proveedorNombre")
        or tx.get("beneficiario")
        or business_partner
        or supplier_card_code
    )
    supplier_key = build_supplier_key(supplier_card_code, business_partner, supplier_name)

    return {
        "supplierKey": supplier_key,
        "supplierName": supplier_name,
        "supplierCardCode": supplier_card_code,
        "businessPartner": business_partner,
    }


def build_supplier_rule_indexes(supplier_rules: list[dict] | None) -> dict[str, dict]:
    by_key: dict[str, dict] = {}
    by_card_code: dict[str, dict] = {}
    by_business_partner: dict[str, dict] = {}
    by_supplier_name: dict[str, dict] = {}

    def register_unique(index: dict[str, dict], raw_value: str | None, rule: dict):
        normalized_value = normalize_text_for_matching(raw_value)
        if not normalized_value:
            return
        existing = index.get(normalized_value)
        if existing is None:
            index[normalized_value] = rule
            return

        same_category = (
            normalize_non_empty_string(existing.get("category2Id")) == normalize_non_empty_string(rule.get("category2Id"))
            and normalize_text_for_matching(existing.get("category2Name")) == normalize_text_for_matching(rule.get("category2Name"))
        )
        if same_category:
            return

        index[normalized_value] = {}

    for rule in supplier_rules or []:
        supplier_key = normalize_non_empty_string((rule or {}).get("supplierKey"))
        if supplier_key:
            by_key[supplier_key] = rule
        register_unique(by_card_code, rule.get("supplierCardCode"), rule)
        register_unique(by_business_partner, rule.get("businessPartner"), rule)
        register_unique(by_supplier_name, rule.get("supplierName"), rule)

    return {
        "by_key": by_key,
        "by_card_code": by_card_code,
        "by_business_partner": by_business_partner,
        "by_supplier_name": by_supplier_name,
    }


def resolve_supplier_category2_rule(
    tx_doc: dict,
    supplier_rules_by_key: dict[str, dict] | None = None,
    supplier_rule_indexes: dict[str, dict] | None = None,
) -> dict | None:
    identity = build_supplier_identity_from_transaction(tx_doc)
    supplier_key = normalize_non_empty_string(identity.get("supplierKey"))
    if supplier_key and isinstance(supplier_rules_by_key, dict):
        direct_match = supplier_rules_by_key.get(supplier_key)
        if direct_match:
            return direct_match

    indexes = supplier_rule_indexes if isinstance(supplier_rule_indexes, dict) else {}

    card_code_index = indexes.get("by_card_code") if isinstance(indexes.get("by_card_code"), dict) else {}
    normalized_card_code = normalize_text_for_matching(identity.get("supplierCardCode"))
    candidate = card_code_index.get(normalized_card_code)
    if normalized_card_code and candidate and candidate.get("supplierKey"):
        return candidate

    business_partner_index = indexes.get("by_business_partner") if isinstance(indexes.get("by_business_partner"), dict) else {}
    normalized_business_partner = normalize_text_for_matching(identity.get("businessPartner"))
    candidate = business_partner_index.get(normalized_business_partner)
    if normalized_business_partner and candidate and candidate.get("supplierKey"):
        return candidate

    supplier_name_index = indexes.get("by_supplier_name") if isinstance(indexes.get("by_supplier_name"), dict) else {}
    normalized_supplier_name = normalize_text_for_matching(identity.get("supplierName"))
    candidate = supplier_name_index.get(normalized_supplier_name)
    if normalized_supplier_name and candidate and candidate.get("supplierKey"):
        return candidate

    if supplier_key and supplier_rules_by_key is None:
        return db.supplierCategory2Rules.find_one({"supplierKey": supplier_key, "isActive": {"$ne": False}})
    return None


def resolve_transaction_category2(
    tx_doc: dict,
    supplier_rules_by_key: dict[str, dict] | None = None,
    supplier_rule_indexes: dict[str, dict] | None = None,
) -> dict:
    category1_id = normalize_non_empty_string(tx_doc.get("categoryEffectiveCode"))
    category1_name = normalize_non_empty_string(tx_doc.get("categoryEffectiveName"))
    normalized_category1_name = normalize_text_for_matching(category1_name)

    if not normalized_category1_name.startswith(TRABAJOS_ESPECIALES_PREFIX):
        return {
            "resolvedCategory2Id": category1_id,
            "resolvedCategory2Name": category1_name,
            "resolvedCategory2Source": "inherited",
        }

    rule = resolve_supplier_category2_rule(
        tx_doc,
        supplier_rules_by_key=supplier_rules_by_key,
        supplier_rule_indexes=supplier_rule_indexes,
    )

    if rule:
        return {
            "resolvedCategory2Id": normalize_non_empty_string(rule.get("category2Id")),
            "resolvedCategory2Name": normalize_non_empty_string(rule.get("category2Name")),
            "resolvedCategory2Source": "supplier_rule",
        }

    return {
        "resolvedCategory2Id": TRABAJOS_ESPECIALES_UNCLASSIFIED_ID,
        "resolvedCategory2Name": TRABAJOS_ESPECIALES_UNCLASSIFIED_NAME,
        "resolvedCategory2Source": "trabajos_especiales_unclassified",
    }


def normalize_project_prefix(raw_prefix: str | None, slug: str) -> str:
    prefix = (raw_prefix or "").strip()
    if not prefix:
        raise HTTPException(status_code=400, detail="s3Prefix is required")

    if prefix in {slug, f"{slug}/"}:
        prefix = f"exports/{slug}"

    prefix = prefix.rstrip("/")
    if not prefix.startswith("exports/"):
        raise HTTPException(status_code=400, detail="s3Prefix must start with exports/")
    return prefix


def resolve_effective_sap_project_fields(tx_doc: dict) -> dict:
    sap_doc = tx_doc.get("sap") if isinstance(tx_doc.get("sap"), dict) else {}

    manual_project_id = normalize_non_empty_string(
        sap_doc.get("manualResolvedProjectId") or tx_doc.get("manualResolvedProjectId")
    )
    manual_project_code = normalize_non_empty_string(
        sap_doc.get("manualResolvedProjectCode") or tx_doc.get("manualResolvedProjectCode")
    )
    manual_project_name = normalize_non_empty_string(
        sap_doc.get("manualResolvedProjectName") or tx_doc.get("manualResolvedProjectName")
    )
    raw_project_code = normalize_non_empty_string(sap_doc.get("rawProjectCode"))
    raw_project_name = normalize_non_empty_string(sap_doc.get("rawProjectName"))

    if manual_project_id:
        return {
            "effectiveProjectId": manual_project_id,
            "effectiveProjectCode": manual_project_code or raw_project_code,
            "effectiveProjectName": manual_project_name or raw_project_name,
            "effectiveProjectSource": "manual",
        }

    return {
        "effectiveProjectId": normalize_non_empty_string(tx_doc.get("projectId")),
        "effectiveProjectCode": raw_project_code,
        "effectiveProjectName": raw_project_name,
        "effectiveProjectSource": "automatic",
    }


def serialize_transaction_with_supplier(
    tx: dict,
    suppliers_by_id: dict[str, dict] | None = None,
    supplier_rules_by_key: dict[str, dict] | None = None,
    supplier_rule_indexes: dict[str, dict] | None = None,
):
    tx_doc = serialize(tx)
    supplier = None
    supplier_id = tx_doc.get("supplierId")
    supplier_id_key = str(supplier_id) if supplier_id else ""

    if supplier_id_key and suppliers_by_id:
        supplier = suppliers_by_id.get(supplier_id_key)

    supplier_name = (
        tx_doc.get("supplierName")
        or tx_doc.get("proveedorNombre")
        or tx_doc.get("beneficiario")
        or (supplier or {}).get("name")
        or ""
    )
    supplier_card_code = tx_doc.get("supplierCardCode") or (supplier or {}).get("cardCode") or ""

    tx_doc["proveedorNombre"] = supplier_name
    tx_doc["proveedorCardCode"] = supplier_card_code

    if supplier:
        tx_doc["proveedor"] = serialize(supplier)

    category_hint_code = normalize_non_empty_string(
        tx_doc.get("categoryHintCode") or tx_doc.get("category_hint_code") or tx_doc.get("categorySapCode")
    )
    category_hint_name = normalize_non_empty_string(
        tx_doc.get("categoryHintName") or tx_doc.get("category_hint_name") or tx_doc.get("categorySapName")
    )
    category_manual_code = normalize_non_empty_string(tx_doc.get("categoryManualCode"))
    category_manual_name = normalize_non_empty_string(tx_doc.get("categoryManualName"))
    effective_fields = build_effective_category_fields(
        category_manual_code,
        category_manual_name,
        category_hint_code,
        category_hint_name,
    )

    tx_doc["categoryHintCode"] = category_hint_code
    tx_doc["categoryHintName"] = category_hint_name
    tx_doc["categorySapCode"] = category_hint_code
    tx_doc["categorySapName"] = category_hint_name
    tx_doc["categoryManualCode"] = category_manual_code
    tx_doc["categoryManualName"] = category_manual_name
    tx_doc["categoryEffectiveCode"] = normalize_non_empty_string(
        tx_doc.get("categoryEffectiveCode") or effective_fields.get("categoryEffectiveCode")
    )
    tx_doc["categoryEffectiveName"] = normalize_non_empty_string(
        tx_doc.get("categoryEffectiveName") or effective_fields.get("categoryEffectiveName")
    )
    tx_doc.update(
        resolve_transaction_category2(
            tx_doc,
            supplier_rules_by_key=supplier_rules_by_key,
            supplier_rule_indexes=supplier_rule_indexes,
        )
    )

    effective_project_fields = resolve_effective_sap_project_fields(tx_doc)
    tx_doc.update(effective_project_fields)

    return tx_doc


def serialize_user(user):
    user_doc = serialize(user)
    user_doc.pop("password_hash", None)
    return user_doc


def serialize_admin_user(user):
    user_doc = serialize_user(user)
    user_doc["role"] = resolve_effective_user_role(user, fallback_role=user_doc.get("role"))
    user_doc["allowedProjectIds"] = normalize_allowed_project_ids(user_doc.get("allowedProjectIds"))
    user_doc["isActive"] = bool(user_doc.get("isActive", user_doc.get("active", True)))
    return user_doc


def normalize_allowed_project_ids(raw_ids) -> list[str]:
    if not isinstance(raw_ids, list):
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in raw_ids:
        candidate = str(raw or "").strip()
        if not candidate or not ObjectId.is_valid(candidate):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)

    return normalized


def normalize_hidden_project_ids(raw_ids) -> list[str]:
    return normalize_allowed_project_ids(raw_ids)


def normalize_ui_prefs(raw_ui_prefs) -> dict:
    ui_prefs = raw_ui_prefs if isinstance(raw_ui_prefs, dict) else {}
    default_project_id = str(ui_prefs.get("defaultProjectId") or "").strip()
    if not ObjectId.is_valid(default_project_id):
        default_project_id = ""
    return {
        "hiddenProjectIds": normalize_hidden_project_ids(ui_prefs.get("hiddenProjectIds")),
        "defaultProjectId": default_project_id,
    }


def normalize_user_role(raw_role: str | None) -> str:
    normalized = str(raw_role or "").strip().upper()
    if normalized in USER_ROLES:
        return normalized
    return "SUPERADMIN"


def resolve_effective_user_role(user_doc: dict | None, fallback_role: str | None = None) -> str:
    """
    Compatibilidad de transición de roles:
    - ADMIN legacy (sin roleVersion >= 2) => SUPERADMIN
    - VIEWER => VIEWER
    - SUPERADMIN => SUPERADMIN
    - ADMIN nuevo real (roleVersion >= 2) => ADMIN
    """
    source_role = (user_doc or {}).get("role") if isinstance(user_doc, dict) else fallback_role
    normalized_role = normalize_user_role(source_role)

    if normalized_role != "ADMIN":
        return normalized_role

    role_version_raw = (user_doc or {}).get("roleVersion") if isinstance(user_doc, dict) else None
    try:
        role_version = int(role_version_raw)
    except (TypeError, ValueError):
        role_version = 0

    if role_version >= ROLE_SCHEMA_VERSION:
        return "ADMIN"
    return "SUPERADMIN"


def is_user_active(user: dict | None) -> bool:
    if not isinstance(user, dict):
        return False
    if "isActive" in user:
        return bool(user.get("isActive"))
    return bool(user.get("active", True))


def is_superadmin(user: dict | None) -> bool:
    return normalize_user_role((user or {}).get("role")) == "SUPERADMIN"


def is_admin(user: dict | None) -> bool:
    return normalize_user_role((user or {}).get("role")) == "ADMIN"


def is_viewer(user: dict | None) -> bool:
    return normalize_user_role((user or {}).get("role")) == "VIEWER"


def count_superadmins(exclude_user_id: str | None = None) -> int:
    total = 0
    for user_doc in db.users.find({}, {"role": 1, "roleVersion": 1}):
        user_id = str(user_doc.get("_id") or "")
        if exclude_user_id and user_id == str(exclude_user_id):
            continue
        if resolve_effective_user_role(user_doc) == "SUPERADMIN":
            total += 1
    return total


def get_accessible_project_ids(user: dict | None) -> list[str] | None:
    if is_superadmin(user) or is_admin(user):
        return None
    if is_viewer(user):
        return normalize_allowed_project_ids((user or {}).get("allowedProjectIds"))
    return None


def can_access_project(user: dict | None, project_id: str | None) -> bool:
    normalized_project_id = str(project_id or "").strip()
    if not normalized_project_id:
        return False
    accessible_project_ids = get_accessible_project_ids(user)
    if accessible_project_ids is None:
        return True
    return normalized_project_id in set(accessible_project_ids)


def build_current_user_payload(username: str, role: str, display_name: str, user_doc: dict | None = None) -> dict:
    normalized_role = normalize_user_role(role)
    is_active = is_user_active(user_doc) if user_doc is not None else True
    resolved_email = ""
    user_id = username

    if user_doc:
        user_id = str(user_doc.get("_id") or username)
        resolved_email = str(user_doc.get("email") or "").strip()

    allowed_project_ids = normalize_allowed_project_ids((user_doc or {}).get("allowedProjectIds"))
    ui_prefs = normalize_ui_prefs((user_doc or {}).get("uiPrefs"))

    return {
        "id": user_id,
        "username": username,
        "name": (display_name or username),
        "displayName": (display_name or username),
        "email": resolved_email,
        "role": normalized_role,
        "isActive": is_active,
        "active": is_active,
        "allowedProjectIds": allowed_project_ids,
        "uiPrefs": ui_prefs,
    }


def role_from_token(credentials: HTTPAuthorizationCredentials | None = Security(bearer_scheme)):
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = credentials.credentials.strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    username = payload.get("sub")
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    user = db.users.find_one({"username": username})
    if user:
        if not is_user_active(user):
            raise HTTPException(status_code=401, detail="User inactive or not found")

        role = resolve_effective_user_role(user)

        env_user = get_env_auth_users().get(username)
        display_name = (
            user.get("displayName")
            or (env_user or {}).get("displayName")
            or payload.get("displayName")
            or payload.get("name")
            or user["username"]
        )
        return build_current_user_payload(user["username"], role, display_name, user_doc=user)

    token_role = normalize_user_role(payload.get("role"))
    env_user = get_env_auth_users().get(username)
    if not env_user:
        raise HTTPException(status_code=401, detail="User inactive or not found")

    role = normalize_user_role(env_user.get("role"))
    if role not in USER_ROLES or token_role != role:
        raise HTTPException(status_code=401, detail="Invalid role")
    display_name = payload.get("displayName") or payload.get("name") or env_user.get("displayName") or username
    return build_current_user_payload(username, role, display_name)


def require_admin(user=Depends(role_from_token)):
    if user["role"] != "SUPERADMIN":
        raise HTTPException(status_code=403, detail="SUPERADMIN role required")
    return user


def require_authenticated(user=Depends(role_from_token)):
    return user


def get_active_project_id(request: FastAPIRequest) -> str:
    header_project_id = (request.headers.get("X-Project-Id") or "").strip()
    # Frontend often sends projectId as query parameter; accept it as a fallback.
    query_project_id = (request.query_params.get("projectId") or "").strip()
    default_project_id = (os.getenv("DEFAULT_PROJECT_ID") or "").strip()

    active_project_id = header_project_id or query_project_id or default_project_id
    if not active_project_id:
        raise HTTPException(
            status_code=400,
            detail="Missing active project. Provide X-Project-Id header or configure DEFAULT_PROJECT_ID env var.",
        )

    if not ObjectId.is_valid(active_project_id):
        raise HTTPException(status_code=400, detail="Invalid active projectId")

    logger.info(
        "Resolved active projectId=%s via %s",
        active_project_id,
        "X-Project-Id" if header_project_id else ("query projectId" if query_project_id else "DEFAULT_PROJECT_ID"),
    )
    return active_project_id


def resolve_project_id(project_id: str | None = None) -> str:
    requested_project_id = (project_id or "").strip()
    using_default = not requested_project_id

    if using_default:
        requested_project_id = (os.getenv("DEFAULT_PROJECT_ID") or "").strip()
        if not requested_project_id:
            raise HTTPException(status_code=500, detail="DEFAULT_PROJECT_ID env var is required")

    if not ObjectId.is_valid(requested_project_id):
        if using_default:
            raise HTTPException(status_code=500, detail="DEFAULT_PROJECT_ID must be a valid ObjectId")
        logger.info("Invalid projectId received: %s", requested_project_id)
        raise HTTPException(status_code=400, detail="Invalid projectId")

    project_doc = db.projects.find_one({"_id": ObjectId(requested_project_id)}, {"_id": 1})
    if not project_doc:
        if using_default:
            raise HTTPException(status_code=500, detail="DEFAULT_PROJECT_ID project not found")
        logger.info("Project not found for projectId=%s", requested_project_id)
        raise HTTPException(status_code=404, detail="Project not found")

    logger.info(
        "Resolved projectId=%s via %s",
        requested_project_id,
        "DEFAULT_PROJECT_ID" if using_default else "query parameter",
    )
    return requested_project_id


def with_legacy_project_filter(query: dict, project_id: str) -> dict:
    # Multitenancy rule: nunca mezclar proyectos, contemplando registros legacy en sap.projectId.
    project_candidates: list[str | ObjectId] = [project_id]
    if ObjectId.is_valid(project_id):
        project_candidates.append(ObjectId(project_id))

    legacy_filter = {
        "$or": [
            {"projectId": {"$in": project_candidates}},
            {"sap.projectId": {"$in": project_candidates}},
        ]
    }
    if not query:
        return legacy_filter
    return {"$and": [legacy_filter, query]}


def ensure_default_users():
    users = db.users
    users.create_index("username", unique=True)

    default_admin_user = env_get("DEFAULT_ADMIN_USERNAME", "default_admin_username", "admin")
    default_admin_pass = env_get("DEFAULT_ADMIN_PASSWORD", "default_admin_password", "admin123")
    default_admin_name = env_get("DEFAULT_ADMIN_NAME", "default_admin_name", default_admin_user)
    defaults = [(default_admin_user, default_admin_pass, "SUPERADMIN", default_admin_name)]

    for default_username, default_password, _ in default_viewer_accounts():
        viewer_username = env_get(
            f"DEFAULT_{default_username.upper()}_USERNAME",
            f"default_{default_username}_username",
            default_username,
        )
        viewer_password = env_get(
            f"DEFAULT_{default_username.upper()}_PASSWORD",
            f"default_{default_username}_password",
            default_password,
        )
        viewer_display_name = env_get(
            f"DEFAULT_{default_username.upper()}_NAME",
            f"default_{default_username}_name",
            viewer_username,
        )
        defaults.append((viewer_username, viewer_password, "VIEWER", viewer_display_name))

    for username, plain_password, role, display_name in defaults:
        existing = users.find_one({"username": username})
        if existing:
            target_role = normalize_user_role(existing.get("role") or role)
            target_role_version = existing.get("roleVersion", ROLE_SCHEMA_VERSION)
            if username == default_admin_user:
                target_role = "SUPERADMIN"
                target_role_version = ROLE_SCHEMA_VERSION
            users.update_one(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        "displayName": existing.get("displayName") or display_name,
                        "role": target_role,
                        "roleVersion": target_role_version,
                        "allowedProjectIds": normalize_allowed_project_ids(existing.get("allowedProjectIds")),
                        "uiPrefs": normalize_ui_prefs(existing.get("uiPrefs")),
                    }
                },
            )
            continue
        users.insert_one(
            {
                "username": username,
                "password_hash": pwd_context.hash(plain_password),
                "role": normalize_user_role(role),
                "roleVersion": ROLE_SCHEMA_VERSION,
                "displayName": display_name,
                "active": True,
                "isActive": True,
                "allowedProjectIds": [],
                "uiPrefs": {"hiddenProjectIds": [], "defaultProjectId": ""},
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    users.update_many({"active": {"$exists": False}}, {"$set": {"active": True}})
    users.update_many({"isActive": {"$exists": False}}, [{"$set": {"isActive": {"$ifNull": ["$active", True]}}}])
    users.update_many({"role": {"$exists": False}}, {"$set": {"role": "SUPERADMIN"}})
    users.update_many({"allowedProjectIds": {"$exists": False}}, {"$set": {"allowedProjectIds": []}})
    users.update_many({"uiPrefs": {"$exists": False}}, {"$set": {"uiPrefs": {"hiddenProjectIds": [], "defaultProjectId": ""}}})
    users.update_many(
        {"allowedProjectIds": {"$type": "array"}},
        [{"$set": {"allowedProjectIds": {"$setUnion": [{"$ifNull": ["$allowedProjectIds", []]}, []]}}}],
    )
    users.update_many(
        {"uiPrefs.hiddenProjectIds": {"$exists": False}},
        [{"$set": {"uiPrefs.hiddenProjectIds": {"$setUnion": [{"$ifNull": ["$uiPrefs.hiddenProjectIds", []]}, []]}}}],
    )
    users.update_many(
        {"uiPrefs.defaultProjectId": {"$exists": False}},
        [{"$set": {"uiPrefs.defaultProjectId": {"$ifNull": ["$uiPrefs.defaultProjectId", ""]}}}],
    )
    users.update_many(
        {"role": "ADMIN", "roleVersion": {"$exists": False}},
        {
            "$set": {
                "legacyRole": "ADMIN",
                "role": "SUPERADMIN",
                "roleVersion": ROLE_SCHEMA_VERSION,
            }
        },
    )


def ensure_telegram_admin_user():
    admin_chat_id = _telegram_normalize_chat_id(get_telegram_admin_chat_id())
    if not admin_chat_id:
        return

    now_iso = _telegram_now_iso()
    db.telegram_users.update_one(
        {"chat_id": admin_chat_id},
        {
            "$set": {
                "chat_id": admin_chat_id,
                "status": "approved",
                "approved": True,
                "approved_at": now_iso,
                "revoked_at": None,
                "updated_at": now_iso,
                "is_admin": True,
            },
            "$setOnInsert": {"requested_at": now_iso},
        },
        upsert=True,
    )


def to_monto_aplicado_cents(value) -> int:
    return int(round(float(value or 0) * 100))


def normalize_sap_fields(sap_payload: dict | None, fallback_amount=None) -> dict:
    sap_doc = sap_payload if isinstance(sap_payload, dict) else {}
    pago_num = str(sap_doc.get("pagoNum") or "").strip()
    factura_num = str(sap_doc.get("facturaNum") or "").strip()
    amount_raw = sap_doc.get("montoAplicado", fallback_amount)
    amount_value = float(amount_raw or 0)
    monto_aplicado = round(amount_value, 2)
    monto_aplicado_cents = to_monto_aplicado_cents(monto_aplicado)
    return {
        "pagoNum": pago_num,
        "facturaNum": factura_num,
        "montoAplicado": monto_aplicado,
        "montoAplicadoCents": monto_aplicado_cents,
    }


def normalize_source_file_key(source_file: str | None, file_name: str | None = None) -> str:
    source_file_value = (source_file or file_name or "").strip()
    return source_file_value


def normalize_source_db_value(source_db: str | None) -> str:
    return (source_db or "").strip().upper()


def get_setting(key: str, default=None):
    doc = db.settings.find_one({"key": key})
    if not doc:
        return default
    return doc.get("value", default)


def set_setting(key: str, value):
    db.settings.update_one(
        {"key": key},
        {"$set": {"value": value, "updatedAt": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )


def get_allowed_telegram_chat_ids() -> set[int] | None:
    single_chat_id = (os.getenv("TELEGRAM_ALLOWED_CHAT_ID") or "").strip()
    raw = (os.getenv("TELEGRAM_ALLOWED_CHAT_IDS") or "").strip()
    if not raw and not single_chat_id:
        return None

    values = set()
    if single_chat_id:
        try:
            values.add(int(single_chat_id))
        except ValueError:
            logger.warning("Ignoring invalid TELEGRAM_ALLOWED_CHAT_ID value: %s", single_chat_id)

    if raw:
        try:
            values.update(set(int(item.strip()) for item in raw.split(",") if item.strip()))
        except ValueError:
            for item in raw.split(","):
                stripped = item.strip()
                if not stripped:
                    continue
                try:
                    values.add(int(stripped))
                except ValueError:
                    logger.warning("Ignoring invalid TELEGRAM_ALLOWED_CHAT_IDS value: %s", stripped)
    return values or None


def get_telegram_default_chat_id() -> int | None:
    env_chat_id = (os.getenv("TELEGRAM_DEFAULT_CHAT_ID") or "").strip()
    if env_chat_id:
        try:
            return int(env_chat_id)
        except ValueError:
            logger.warning("TELEGRAM_DEFAULT_CHAT_ID is not a valid integer: %s", env_chat_id)

    stored_chat_id = get_setting(TELEGRAM_SETTINGS_KEY)
    if stored_chat_id is None:
        return None
    try:
        return int(stored_chat_id)
    except (TypeError, ValueError):
        logger.warning("Stored telegram_default_chat_id is invalid: %s", stored_chat_id)
        return None


def tg_send(chat_id: int | str, text: str, reply_markup: dict | None = None) -> bool:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        logger.info("Telegram send skipped: TELEGRAM_BOT_TOKEN is not configured")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = {"chat_id": chat_id, "text": text}
    if reply_markup:
        body["reply_markup"] = reply_markup
    payload = json.dumps(body).encode("utf-8")
    req = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")

    try:
        with urlopen(req, timeout=15) as response:
            body = response.read().decode("utf-8")
            logger.info("Telegram message sent to chat_id=%s status=%s", chat_id, response.status)
            if response.status >= 400:
                logger.error("Telegram send failed for chat_id=%s body=%s", chat_id, body)
                return False
            return True
    except Exception as exc:
        logger.exception("Telegram send failed for chat_id=%s: %s", chat_id, exc)
        return False


def send_telegram(text: str) -> bool:
    return send_telegram_to_chat(text=text)


def send_telegram_to_chat(text: str, chat_id: int | str | None = None) -> bool:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    resolved_chat_id = str(chat_id).strip() if chat_id is not None else ""

    if not resolved_chat_id:
        resolved_chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

    if not resolved_chat_id:
        default_chat_id = get_telegram_default_chat_id()
        resolved_chat_id = str(default_chat_id) if default_chat_id is not None else ""

    if not token or not resolved_chat_id:
        logger.info("Telegram send skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not configured")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": resolved_chat_id, "text": text}).encode("utf-8")
    req = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")

    try:
        with urlopen(req, timeout=15) as response:
            if response.status >= 400:
                body = response.read().decode("utf-8")
                logger.error("Telegram send failed for chat_id=%s body=%s", resolved_chat_id, body)
                return False
            logger.info("Telegram message sent to chat_id=%s status=%s", resolved_chat_id, response.status)
            return True
    except Exception as exc:
        logger.exception("Telegram send failed for chat_id=%s: %s", resolved_chat_id, exc)
        return False


def get_telegram_imports_chat_id() -> str:
    imports_chat_id = (os.getenv("TELEGRAM_IMPORTS_CHAT_ID") or "").strip()
    if imports_chat_id:
        return imports_chat_id

    admin_chat_id = _telegram_normalize_chat_id(get_telegram_admin_chat_id())
    if admin_chat_id:
        return admin_chat_id

    fallback_chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if fallback_chat_id:
        return fallback_chat_id

    default_chat_id = get_telegram_default_chat_id()
    return str(default_chat_id).strip() if default_chat_id is not None else ""


def send_telegram_import_message(text: str) -> bool:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    imports_chat_id = get_telegram_imports_chat_id()

    if not token or not imports_chat_id:
        logger.warning(
            "Telegram imports notification skipped: TELEGRAM_BOT_TOKEN or chat id is not configured"
        )
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": imports_chat_id, "text": text}).encode("utf-8")
    req = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")

    try:
        with urlopen(req, timeout=15) as response:
            if response.status >= 400:
                body = response.read().decode("utf-8")
                logger.error(
                    "Telegram imports send failed for chat_id=%s body=%s",
                    imports_chat_id,
                    body,
                )
                return False
            logger.info(
                "Telegram imports message sent to chat_id=%s status=%s",
                imports_chat_id,
                response.status,
            )
            return True
    except Exception:
        logger.exception("Telegram imports send failed for chat_id=%s", imports_chat_id)
        return False


def get_telegram_notification_chat_ids() -> list[str]:
    chat_ids: set[str] = set()

    try:
        approved_users = db.telegram_users.find(
            {"$or": [{"status": "approved"}, {"approved": True}]},
            {"chat_id": 1},
        )
        for user_doc in approved_users:
            normalized = _telegram_normalize_chat_id(user_doc.get("chat_id"))
            if normalized:
                chat_ids.add(normalized)
    except Exception:
        logger.exception("Failed to resolve telegram_users recipients; falling back to env/default chat ids")

    env_chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if env_chat_id:
        chat_ids.add(env_chat_id)

    default_chat_id = get_telegram_default_chat_id()
    if default_chat_id is not None:
        chat_ids.add(str(default_chat_id))

    return sorted(chat_ids)


def send_telegram_broadcast(text: str) -> dict:
    recipients = get_telegram_notification_chat_ids()
    if not recipients:
        logger.info("Telegram broadcast skipped: no recipients configured")
        return {"total": 0, "sent": 0, "failed": 0}

    sent = 0
    failed = 0
    for chat_id in recipients:
        if tg_send(chat_id=chat_id, text=text):
            sent += 1
        else:
            failed += 1

    return {"total": len(recipients), "sent": sent, "failed": failed}


def tg_answer_callback_query(callback_query_id: str, text: str | None = None) -> bool:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token or not callback_query_id:
        return False

    url = f"https://api.telegram.org/bot{token}/answerCallbackQuery"
    body = {"callback_query_id": callback_query_id}
    if text:
        body["text"] = text
        body["show_alert"] = False
    payload = json.dumps(body).encode("utf-8")
    req = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(req, timeout=15) as response:
            return response.status < 400
    except Exception:
        logger.exception("Telegram answerCallbackQuery failed callback_query_id=%s", callback_query_id)
        return False


def tg_edit_message(chat_id: int | str, message_id: int | str, text: str, reply_markup: dict | None = None) -> bool:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        logger.info("Telegram edit skipped: TELEGRAM_BOT_TOKEN is not configured")
        return False

    url = f"https://api.telegram.org/bot{token}/editMessageText"
    body = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup:
        body["reply_markup"] = reply_markup
    payload = json.dumps(body).encode("utf-8")
    req = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(req, timeout=15) as response:
            return response.status < 400
    except Exception:
        logger.exception("Telegram editMessageText failed chat_id=%s message_id=%s", chat_id, message_id)
        return False


def get_telegram_admin_chat_id() -> str:
    return (os.getenv("TELEGRAM_ADMIN_CHAT_ID") or "13875693").strip()


def _telegram_normalize_chat_id(chat_id: int | str | None) -> str:
    return str(chat_id).strip() if chat_id is not None else ""


def _telegram_extract_user_data(from_user: dict | None) -> tuple[str | None, str | None, str | None]:
    if not isinstance(from_user, dict):
        return None, None, None
    username = (from_user.get("username") or "").strip() or None
    first_name = (from_user.get("first_name") or "").strip() or None
    last_name = (from_user.get("last_name") or "").strip() or None
    return username, first_name, last_name


def _telegram_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _telegram_is_admin_chat(chat_id: int | str | None) -> bool:
    normalized_chat_id = _telegram_normalize_chat_id(chat_id)
    return bool(normalized_chat_id) and normalized_chat_id == get_telegram_admin_chat_id()


def _telegram_compose_name(first_name: str | None, last_name: str | None) -> str:
    return f"{first_name or ''} {last_name or ''}".strip() or "-"


def _telegram_escape_regex_literal(value: str) -> str:
    return re.escape((value or "").strip())


def _telegram_is_chat_approved(chat_id: int | str | None) -> bool:
    normalized_chat_id = _telegram_normalize_chat_id(chat_id)
    if not normalized_chat_id:
        return False
    if _telegram_is_admin_chat(normalized_chat_id):
        return True

    user_doc = db.telegram_users.find_one({"chat_id": normalized_chat_id}, {"status": 1, "approved": 1})
    if not user_doc:
        return False
    status = str(user_doc.get("status") or "").strip().lower()
    if status:
        return status == "approved"
    return user_doc.get("approved") is True


def _telegram_has_recent_pending_request(chat_id: int | str | None) -> bool:
    normalized_chat_id = _telegram_normalize_chat_id(chat_id)
    if not normalized_chat_id:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(hours=TELEGRAM_ACCESS_REQUEST_WINDOW_HOURS)
    pending_doc = db.telegram_users.find_one(
        {
            "chat_id": normalized_chat_id,
            "status": "pending",
            "requested_at": {"$gte": cutoff.isoformat()},
        },
        {"_id": 1},
    )
    return pending_doc is not None


def _telegram_register_access_request(chat_id: int | str, from_user: dict | None) -> None:
    normalized_chat_id = _telegram_normalize_chat_id(chat_id)
    username, first_name, last_name = _telegram_extract_user_data(from_user)
    now_iso = _telegram_now_iso()
    db.telegram_users.update_one(
        {"chat_id": normalized_chat_id},
        {
            "$set": {
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "requested_at": now_iso,
                "status": "pending",
                "approved": False,
                "approved_at": None,
                "revoked_at": None,
                "updated_at": now_iso,
            }
        },
        upsert=True,
    )


def _telegram_upsert_user_status(chat_id: int | str, status: str, from_user: dict | None = None) -> tuple[str, str | None]:
    normalized_chat_id = _telegram_normalize_chat_id(chat_id)
    username, first_name, last_name = _telegram_extract_user_data(from_user)
    now_iso = _telegram_now_iso()
    update_set: dict = {
        "status": status,
        "updated_at": now_iso,
    }
    if username is not None:
        update_set["username"] = username
    if first_name is not None:
        update_set["first_name"] = first_name
    if last_name is not None:
        update_set["last_name"] = last_name

    if status == "approved":
        update_set.update({"approved": True, "approved_at": now_iso, "revoked_at": None})
    elif status == "revoked":
        update_set.update({"approved": False, "revoked_at": now_iso})

    db.telegram_users.update_one({"chat_id": normalized_chat_id}, {"$set": update_set, "$setOnInsert": {"requested_at": now_iso}}, upsert=True)
    doc = db.telegram_users.find_one({"chat_id": normalized_chat_id}, {"username": 1})
    return normalized_chat_id, (doc or {}).get("username")


def _telegram_handle_admin_command(chat_id: int | str, text: str) -> bool:
    if not (text.startswith("/users") or text.startswith("/user_find") or text.startswith("/user_add") or text.startswith("/user_remove")):
        return False
    if not _telegram_is_admin_chat(chat_id):
        send_telegram_to_chat("No autorizado", chat_id=chat_id)
        return True

    args = _telegram_parse_command_args(text)
    command = args[0] if args else ""

    if command == "/users":
        scope = (args[1] if len(args) > 1 else "approved").strip().lower()
        status_filter = {"approved", "pending", "all"}
        if scope not in status_filter:
            send_telegram_to_chat("Uso: /users [approved|pending|all]", chat_id=chat_id)
            return True
        query = {} if scope == "all" else {"status": scope}
        cursor = db.telegram_users.find(query, {"chat_id": 1, "username": 1, "first_name": 1, "last_name": 1, "status": 1, "updated_at": 1}).sort("updated_at", -1).limit(50)
        rows = list(cursor)
        if not rows:
            send_telegram_to_chat("Sin resultados", chat_id=chat_id)
            return True
        lines = [f"Usuarios ({scope})"]
        for row in rows:
            lines.append(
                f"{row.get('chat_id') or '-'} | @{row.get('username') or '-'} | {_telegram_compose_name(row.get('first_name'), row.get('last_name'))} | {row.get('status') or '-'} | {row.get('updated_at') or '-'}"
            )
        send_telegram_to_chat("\n".join(lines), chat_id=chat_id)
        return True

    if command == "/user_find":
        query_text = " ".join(args[1:]).strip()
        if not query_text:
            send_telegram_to_chat("Uso: /user_find <texto>", chat_id=chat_id)
            return True
        regex = {"$regex": _telegram_escape_regex_literal(query_text), "$options": "i"}
        cursor = db.telegram_users.find({"$or": [{"username": regex}, {"first_name": regex}, {"last_name": regex}]}, {"chat_id": 1, "username": 1, "first_name": 1, "last_name": 1, "status": 1, "updated_at": 1}).sort("updated_at", -1).limit(20)
        rows = list(cursor)
        if not rows:
            send_telegram_to_chat("Sin resultados", chat_id=chat_id)
            return True
        lines = [f"Coincidencias ({len(rows)}):"]
        for row in rows:
            lines.append(
                f"{row.get('chat_id') or '-'} | @{row.get('username') or '-'} | {_telegram_compose_name(row.get('first_name'), row.get('last_name'))} | {row.get('status') or '-'} | {row.get('updated_at') or '-'}"
            )
        send_telegram_to_chat("\n".join(lines), chat_id=chat_id)
        return True

    if command == "/user_add":
        if len(args) < 2:
            send_telegram_to_chat("Uso: /user_add <chat_id|@username>", chat_id=chat_id)
            return True
        target = args[1].strip()
        if target.startswith("@"):
            username = target[1:].strip()
            if not username:
                send_telegram_to_chat("Uso: /user_add @username", chat_id=chat_id)
                return True
            doc = db.telegram_users.find_one({"username": {"$regex": f"^{_telegram_escape_regex_literal(username)}$", "$options": "i"}}, {"chat_id": 1})
            if not doc:
                send_telegram_to_chat("No encontrado; el usuario debe enviar /start al bot primero", chat_id=chat_id)
                return True
            normalized_chat_id, resolved_username = _telegram_upsert_user_status(chat_id=doc.get("chat_id"), status="approved")
            send_telegram_to_chat(f"Usuario aprobado: {normalized_chat_id} (@{resolved_username or username})", chat_id=chat_id)
            return True

        normalized_chat_id, _ = _telegram_upsert_user_status(chat_id=target, status="approved")
        send_telegram_to_chat(f"Usuario aprobado: {normalized_chat_id}", chat_id=chat_id)
        return True

    if command == "/user_remove":
        if len(args) < 2:
            send_telegram_to_chat("Uso: /user_remove <chat_id|@username>", chat_id=chat_id)
            return True
        target = args[1].strip()
        if target.startswith("@"):
            username = target[1:].strip()
            doc = db.telegram_users.find_one({"username": {"$regex": f"^{_telegram_escape_regex_literal(username)}$", "$options": "i"}}, {"chat_id": 1})
            if not doc:
                send_telegram_to_chat("No encontrado", chat_id=chat_id)
                return True
            normalized_chat_id, _ = _telegram_upsert_user_status(chat_id=doc.get("chat_id"), status="revoked")
            send_telegram_to_chat(f"Usuario revocado: {normalized_chat_id}", chat_id=chat_id)
            return True

        normalized_chat_id, _ = _telegram_upsert_user_status(chat_id=target, status="revoked")
        send_telegram_to_chat(f"Usuario revocado: {normalized_chat_id}", chat_id=chat_id)
        return True

    return False


def _telegram_notify_admin_access_request(chat_id: int | str, from_user: dict | None, text: str, date_value: str | None) -> None:
    normalized_chat_id = _telegram_normalize_chat_id(chat_id)
    username, first_name, last_name = _telegram_extract_user_data(from_user)
    admin_chat_id = get_telegram_admin_chat_id()
    message = (
        "🔐 Solicitud de acceso Telegram\n"
        f"chat_id: {normalized_chat_id}\n"
        f"username: @{username if username else '-'}\n"
        f"nombre: {(first_name or '')} {(last_name or '')}".strip()
        + "\n"
        f"texto: {text or '-'}\n"
        f"fecha: {date_value or datetime.now(timezone.utc).isoformat()}"
    )
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "✅ Autorizar", "callback_data": f"approve:{normalized_chat_id}"},
                {"text": "❌ Rechazar", "callback_data": f"reject:{normalized_chat_id}"},
            ]
        ]
    }
    tg_send(chat_id=admin_chat_id, text=message, reply_markup=keyboard)


def _telegram_handle_access_callback(callback_query: dict) -> dict:
    callback_query_id = str(callback_query.get("id") or "")
    data = str(callback_query.get("data") or "").strip()
    callback_chat = callback_query.get("message", {}).get("chat") if isinstance(callback_query.get("message"), dict) else None
    actor_chat_id = _telegram_normalize_chat_id(callback_chat.get("id") if isinstance(callback_chat, dict) else None)
    admin_chat_id = get_telegram_admin_chat_id()

    if actor_chat_id != admin_chat_id:
        tg_answer_callback_query(callback_query_id, "Solo el admin puede usar esta acción")
        return {"ok": True, "ignored": "callback_not_admin"}

    if ":" not in data:
        tg_answer_callback_query(callback_query_id, "Acción inválida")
        return {"ok": True, "ignored": "invalid_callback_data"}

    action, target_chat_id = data.split(":", 1)
    target_chat_id = _telegram_normalize_chat_id(target_chat_id)
    if action not in {"approve", "reject"} or not target_chat_id:
        tg_answer_callback_query(callback_query_id, "Acción inválida")
        return {"ok": True, "ignored": "invalid_callback_data"}

    if action == "approve":
        _telegram_upsert_user_status(chat_id=target_chat_id, status="approved")
        send_telegram_to_chat(f"Aprobado chat_id={target_chat_id}", chat_id=admin_chat_id)
        send_telegram_to_chat("✅ Ya estás autorizado. Escribe /help", chat_id=target_chat_id)
        tg_answer_callback_query(callback_query_id, "Usuario aprobado")
        return {"ok": True, "result": "approved", "chat_id": target_chat_id}

    _telegram_upsert_user_status(chat_id=target_chat_id, status="revoked")
    send_telegram_to_chat(f"Rechazado chat_id={target_chat_id}", chat_id=admin_chat_id)
    send_telegram_to_chat("❌ Tu solicitud fue rechazada por el admin.", chat_id=target_chat_id)
    tg_answer_callback_query(callback_query_id, "Usuario rechazado")
    return {"ok": True, "result": "rejected", "chat_id": target_chat_id}


def _telegram_current_month_token() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _telegram_month_bounds(month_token: str) -> tuple[str, str] | None:
    if not re.fullmatch(r"\d{4}-\d{2}", month_token or ""):
        return None
    year, month = month_token.split("-", 1)
    month_int = int(month)
    if month_int < 1 or month_int > 12:
        return None
    start_date = f"{year}-{month_int:02d}-01"
    next_year = int(year) + (1 if month_int == 12 else 0)
    next_month = 1 if month_int == 12 else month_int + 1
    end_date = f"{next_year:04d}-{next_month:02d}-01"
    return start_date, end_date


def _telegram_recent_months(limit: int = 12) -> list[str]:
    now = datetime.now(timezone.utc)
    year = now.year
    month = now.month
    values = []
    for _ in range(limit):
        values.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return values


def _telegram_state_collection():
    return db.telegram_state


def _telegram_get_state(chat_id: int | str) -> dict | None:
    return _telegram_state_collection().find_one({"chat_id": _telegram_normalize_chat_id(chat_id)})


def _telegram_save_state(chat_id: int | str, payload: dict) -> None:
    normalized_chat_id = _telegram_normalize_chat_id(chat_id)
    data = {
        "chat_id": normalized_chat_id,
        "mode": payload.get("mode"),
        "projectId": payload.get("projectId"),
        "supplierId": payload.get("supplierId"),
        "categoryId": payload.get("categoryId"),
        "pendingCommand": payload.get("pendingCommand"),
        "pendingText": payload.get("pendingText"),
        "month": payload.get("month") or _telegram_current_month_token(),
        "page": _telegram_parse_page(str(payload.get("page") or 1)),
        "limit": _telegram_parse_limit(str(payload.get("limit") or 25), default=25, maximum=50),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    _telegram_state_collection().update_one({"chat_id": normalized_chat_id}, {"$set": data}, upsert=True)


def _telegram_clear_state(chat_id: int | str) -> None:
    _telegram_state_collection().delete_one({"chat_id": _telegram_normalize_chat_id(chat_id)})


def _telegram_get_picker_state(chat_id: int | str) -> dict:
    state = _telegram_get_state(chat_id) or {}
    picker = state.get("picker")
    return picker if isinstance(picker, dict) else {}


def _telegram_save_picker_state(chat_id: int | str, mode: str, search_text: str, page: int, limit: int = 10) -> dict:
    normalized_chat_id = _telegram_normalize_chat_id(chat_id)
    picker = {
        "mode": mode,
        "searchText": str(search_text or "").strip(),
        "page": _telegram_parse_page(str(page)),
        "limit": _telegram_parse_limit(str(limit), default=10, maximum=25),
    }
    _telegram_state_collection().update_one(
        {"chat_id": normalized_chat_id},
        {"$set": {"chat_id": normalized_chat_id, "picker": picker, "updated_at": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )
    return picker


def _telegram_list_projects() -> list[dict]:
    rows = db.projects.find({}, {"name": 1, "slug": 1}).sort("name", 1)
    return [{"_id": str(row.get("_id")), "name": (row.get("name") or "(sin nombre)").strip(), "slug": row.get("slug")} for row in rows]


def _telegram_build_projects_keyboard(selected_project_id: str | None = None) -> tuple[str, dict | None]:
    projects = _telegram_list_projects()
    if not projects:
        return "No hay proyectos disponibles.", None

    keyboard_rows = []
    for project in projects[:20]:
        project_id = project.get("_id") or ""
        marker = "✅ " if selected_project_id and project_id == selected_project_id else ""
        keyboard_rows.append([{"text": f"{marker}{project.get('name')}", "callback_data": f"projectSel:{project_id}"}])
    keyboard_rows.append([{"text": "❌ Cerrar", "callback_data": "close"}])
    return "Selecciona el proyecto para continuar:", {"inline_keyboard": keyboard_rows}


def _telegram_get_selected_project_id(chat_id: int | str) -> str | None:
    state = _telegram_get_state(chat_id) or {}
    selected_project_id = str(state.get("projectId") or "").strip()
    if selected_project_id and ObjectId.is_valid(selected_project_id):
        exists = db.projects.find_one({"_id": ObjectId(selected_project_id)}, {"_id": 1})
        if exists:
            return selected_project_id
    return None


def _telegram_require_project_selection(chat_id: int | str, pending_command: str, pending_text: str) -> bool:
    selected_project_id = _telegram_get_selected_project_id(chat_id)
    if selected_project_id:
        return False

    _telegram_save_state(
        chat_id,
        {
            "projectId": None,
            "mode": None,
            "supplierId": None,
            "categoryId": None,
            "pendingCommand": pending_command,
            "pendingText": pending_text,
            "page": 1,
            "limit": 25,
        },
    )
    prompt_text, prompt_keyboard = _telegram_build_projects_keyboard()
    tg_send(chat_id=chat_id, text=prompt_text, reply_markup=prompt_keyboard)
    return True


def _telegram_build_suppliers_keyboard(project_id: str, search_text: str, page: int = 1, limit: int = 10) -> tuple[str, dict | None, dict]:
    clean_search = str(search_text or "").strip()
    current_page = _telegram_parse_page(str(page))
    effective_limit = _telegram_parse_limit(str(limit), default=10, maximum=25)

    query: dict = {"$or": [{"projectId": project_id}, {"projectIds": project_id}]}
    if clean_search:
        escaped = re.escape(clean_search)
        query = {
            "$and": [
                query,
                {"$or": [{"name": {"$regex": escaped, "$options": "i"}}, {"cardCode": {"$regex": escaped, "$options": "i"}}]},
            ]
        }

    total = db.suppliers.count_documents(query)
    last_page = max(1, (total + effective_limit - 1) // effective_limit)
    current_page = min(current_page, last_page)
    skip = (current_page - 1) * effective_limit

    rows = list(db.suppliers.find(query, {"name": 1, "cardCode": 1}).sort([("name", 1), ("_id", 1)]).skip(skip).limit(effective_limit))
    if not rows:
        return "Sin resultados de proveedores.", None, {"mode": "prov", "searchText": clean_search, "page": 1, "limit": effective_limit}

    keyboard_rows = []
    for row in rows:
        supplier_id = str(row.get("_id"))
        supplier_name = (row.get("name") or "(sin nombre)").strip()
        card_code = (row.get("cardCode") or "-").strip()
        keyboard_rows.append([{"text": f"{supplier_name} ({card_code})", "callback_data": f"provSel:{supplier_id}"}])

    nav_row = [
        {"text": "⬅️ Prev", "callback_data": "provPick:prev"},
        {"text": f"{current_page}/{last_page}", "callback_data": "noop"},
        {"text": "➡️ Next", "callback_data": "provPick:next"},
    ]
    keyboard_rows.append(nav_row)
    keyboard_rows.append([{"text": "❌ Cerrar", "callback_data": "close"}])

    title = "Selecciona un proveedor:" if clean_search else "Proveedores disponibles:"
    return title, {"inline_keyboard": keyboard_rows}, {"mode": "prov", "searchText": clean_search, "page": current_page, "limit": effective_limit}


def _telegram_build_categories_keyboard(project_id: str, search_text: str, page: int = 1, limit: int = 10) -> tuple[str, dict | None, dict]:
    clean_search = str(search_text or "").strip()
    current_page = _telegram_parse_page(str(page))
    effective_limit = _telegram_parse_limit(str(limit), default=10, maximum=25)

    query: dict = {"projectId": project_id}
    if clean_search:
        escaped = re.escape(clean_search)
        query = {"$and": [query, {"name": {"$regex": escaped, "$options": "i"}}]}

    total = db.categories.count_documents(query)
    last_page = max(1, (total + effective_limit - 1) // effective_limit)
    current_page = min(current_page, last_page)
    skip = (current_page - 1) * effective_limit

    rows = list(db.categories.find(query, {"name": 1}).sort([("name", 1), ("_id", 1)]).skip(skip).limit(effective_limit))
    if not rows:
        return "Sin resultados de categorías.", None, {"mode": "cat", "searchText": clean_search, "page": 1, "limit": effective_limit}

    keyboard_rows = []
    for row in rows:
        category_id = str(row.get("_id"))
        name = (row.get("name") or "(sin nombre)").strip()
        keyboard_rows.append([{"text": name, "callback_data": f"catSel:{category_id}"}])

    nav_row = [
        {"text": "⬅️ Prev", "callback_data": "catPick:prev"},
        {"text": f"{current_page}/{last_page}", "callback_data": "noop"},
        {"text": "➡️ Next", "callback_data": "catPick:next"},
    ]
    keyboard_rows.append(nav_row)
    keyboard_rows.append([{"text": "❌ Cerrar", "callback_data": "close"}])

    title = "Selecciona una categoría:" if clean_search else "Categorías disponibles:"
    return title, {"inline_keyboard": keyboard_rows}, {"mode": "cat", "searchText": clean_search, "page": current_page, "limit": effective_limit}


def _telegram_search_suppliers_keyboard(chat_id: int | str, raw_text: str) -> tuple[str, dict | None]:
    args = _telegram_parse_command_args(raw_text)
    text = " ".join(args[1:]).strip() if len(args) > 1 else ""
    picker_state = _telegram_get_picker_state(chat_id)

    if text:
        picker = _telegram_save_picker_state(chat_id=chat_id, mode="prov", search_text=text, page=1, limit=10)
    else:
        if str(picker_state.get("mode") or "").strip() == "prov":
            picker = _telegram_save_picker_state(
                chat_id=chat_id,
                mode="prov",
                search_text=str(picker_state.get("searchText") or ""),
                page=_telegram_parse_page(str(picker_state.get("page") or 1)),
                limit=_telegram_parse_limit(str(picker_state.get("limit") or 10), default=10, maximum=25),
            )
        else:
            picker = _telegram_save_picker_state(chat_id=chat_id, mode="prov", search_text="", page=1, limit=10)

    selected_project_id = _telegram_get_selected_project_id(chat_id)
    if not selected_project_id:
        return "Primero selecciona un proyecto con /project", None

    response_text, keyboard, picker = _telegram_build_suppliers_keyboard(
        project_id=selected_project_id,
        search_text=picker.get("searchText") or "",
        page=picker.get("page") or 1,
        limit=picker.get("limit") or 10,
    )
    _telegram_save_picker_state(
        chat_id=chat_id,
        mode=picker.get("mode") or "prov",
        search_text=picker.get("searchText") or "",
        page=picker.get("page") or 1,
        limit=picker.get("limit") or 10,
    )
    return response_text, keyboard


def _telegram_search_categories_keyboard(chat_id: int | str, raw_text: str) -> tuple[str, dict | None]:
    args = _telegram_parse_command_args(raw_text)
    text = " ".join(args[1:]).strip() if len(args) > 1 else ""
    picker_state = _telegram_get_picker_state(chat_id)

    if text:
        picker = _telegram_save_picker_state(chat_id=chat_id, mode="cat", search_text=text, page=1, limit=10)
    else:
        if str(picker_state.get("mode") or "").strip() == "cat":
            picker = _telegram_save_picker_state(
                chat_id=chat_id,
                mode="cat",
                search_text=str(picker_state.get("searchText") or ""),
                page=_telegram_parse_page(str(picker_state.get("page") or 1)),
                limit=_telegram_parse_limit(str(picker_state.get("limit") or 10), default=10, maximum=25),
            )
        else:
            picker = _telegram_save_picker_state(chat_id=chat_id, mode="cat", search_text="", page=1, limit=10)

    selected_project_id = _telegram_get_selected_project_id(chat_id)
    if not selected_project_id:
        return "Primero selecciona un proyecto con /project", None

    response_text, keyboard, picker = _telegram_build_categories_keyboard(
        project_id=selected_project_id,
        search_text=picker.get("searchText") or "",
        page=picker.get("page") or 1,
        limit=picker.get("limit") or 10,
    )
    _telegram_save_picker_state(
        chat_id=chat_id,
        mode=picker.get("mode") or "cat",
        search_text=picker.get("searchText") or "",
        page=picker.get("page") or 1,
        limit=picker.get("limit") or 10,
    )
    return response_text, keyboard


def _telegram_build_transaction_message(state: dict) -> tuple[str, dict]:
    mode = str(state.get("mode") or "").strip()
    page = _telegram_parse_page(str(state.get("page") or 1))
    limit = _telegram_parse_limit(str(state.get("limit") or 25), default=25, maximum=50)
    project_id = str(state.get("projectId") or "").strip()
    if not project_id:
        return "Primero selecciona un proyecto con /project", {"inline_keyboard": [[{"text": "Seleccionar proyecto", "callback_data": "projectPicker"}]]}

    query: dict = {"projectId": project_id}
    title = ""
    if mode == "prov":
        supplier_id = str(state.get("supplierId") or "").strip()
        query["$or"] = [{"supplierId": supplier_id}, {"supplier_id": supplier_id}]
        supplier = db.suppliers.find_one({"_id": ObjectId(supplier_id)}) if ObjectId.is_valid(supplier_id) else None
        supplier_name = (supplier or {}).get("name") or "Proveedor"
        title = f"Proveedor: {supplier_name}"
    elif mode == "cat":
        category_id = str(state.get("categoryId") or "").strip()
        query["$or"] = [{"categoryId": category_id}, {"category_id": category_id}]
        category = db.categories.find_one({"_id": ObjectId(category_id)}) if ObjectId.is_valid(category_id) else None
        category_name = (category or {}).get("name") or "Categoría"
        title = f"Categoría: {category_name}"

    skip = (page - 1) * limit
    projection = {"date": 1, "amount": 1, "tax": 1, "concept": 1, "description": 1, "supplierName": 1, "sourceDb": 1}
    rows = list(db.transactions.find(query, projection).sort([("date", -1), ("_id", -1)]).skip(skip).limit(limit))

    lines = [title, "Listado completo (todos los pagos)", ""]
    if not rows:
        lines.append("Sin movimientos para la selección actual.")
    else:
        for tx in rows:
            source_db = str(tx.get("sourceDb") or "").strip().upper()
            amount = float(tx.get("amount") or 0)
            tax = tx.get("tax") if isinstance(tx.get("tax"), dict) else {}
            subtotal = float(tax.get("subtotal") or 0)
            shown_amount = subtotal if source_db == "IVA" else amount
            date_value = str(tx.get("date") or "?")[:10]
            concept = str(tx.get("concept") or tx.get("description") or "(sin concepto)")
            lines.append(f"{date_value} | {_telegram_format_currency(shown_amount)} | {concept}")
    lines.extend(["", f"Página {page} · límite {limit}"])

    text = "\n".join(lines)
    if len(text) > 4000:
        text = f"{text[:3940].rstrip()}\n\nTexto truncado, usa Next."

    keyboard = {
        "inline_keyboard": [
            [{"text": "⬅️ Prev", "callback_data": "nav:prev"}, {"text": "➡️ Next", "callback_data": "nav:next"}],
            [{"text": "🧾 Total", "callback_data": "total"}, {"text": "❌ Cerrar", "callback_data": "close"}],
        ]
    }
    return text, keyboard


def _telegram_compute_total_for_state(state: dict) -> float:
    mode = str(state.get("mode") or "").strip()
    project_id = str(state.get("projectId") or "").strip()
    if not project_id:
        return 0.0
    query: dict = {"projectId": project_id}
    if mode == "prov":
        supplier_id = str(state.get("supplierId") or "").strip()
        query["$or"] = [{"supplierId": supplier_id}, {"supplier_id": supplier_id}]
    else:
        category_id = str(state.get("categoryId") or "").strip()
        query["$or"] = [{"categoryId": category_id}, {"category_id": category_id}]

    total = 0.0
    for tx in db.transactions.find(query, {"amount": 1, "tax": 1, "sourceDb": 1}):
        source_db = str(tx.get("sourceDb") or "").strip().upper()
        amount = float(tx.get("amount") or 0)
        tax = tx.get("tax") if isinstance(tx.get("tax"), dict) else {}
        subtotal = float(tax.get("subtotal") or 0)
        total += subtotal if source_db == "IVA" else amount
    return total


def _telegram_handle_callback(callback_query: dict) -> dict:
    callback_query_id = str(callback_query.get("id") or "")
    data = str(callback_query.get("data") or "").strip()
    message = callback_query.get("message") if isinstance(callback_query.get("message"), dict) else {}
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")

    if data.startswith("approve:") or data.startswith("reject:"):
        return _telegram_handle_access_callback(callback_query)

    if not _telegram_is_chat_approved(chat_id):
        tg_answer_callback_query(callback_query_id, "No autorizado")
        return {"ok": True, "ignored": "chat_id_not_approved"}

    normalized_chat_id = _telegram_normalize_chat_id(chat_id)

    picker_state = _telegram_get_picker_state(normalized_chat_id)

    if data == "projectPicker":
        selected_project_id = _telegram_get_selected_project_id(normalized_chat_id)
        response_text, keyboard = _telegram_build_projects_keyboard(selected_project_id=selected_project_id)
        tg_edit_message(chat_id=chat_id, message_id=message_id, text=response_text, reply_markup=keyboard)
        tg_answer_callback_query(callback_query_id)
        return {"ok": True}

    if data.startswith("projectSel:"):
        selected_project_id = data.split(":", 1)[1].strip()
        if not ObjectId.is_valid(selected_project_id) or not db.projects.find_one({"_id": ObjectId(selected_project_id)}, {"_id": 1}):
            tg_answer_callback_query(callback_query_id, "Proyecto inválido")
            return {"ok": True, "ignored": "invalid_project"}

        current_state = _telegram_get_state(normalized_chat_id) or {}
        pending_command = str(current_state.get("pendingCommand") or "").strip()
        pending_text = str(current_state.get("pendingText") or "").strip()
        _telegram_save_state(
            normalized_chat_id,
            {
                "projectId": selected_project_id,
                "mode": current_state.get("mode"),
                "supplierId": current_state.get("supplierId"),
                "categoryId": current_state.get("categoryId"),
                "pendingCommand": None,
                "pendingText": None,
                "page": current_state.get("page") or 1,
                "limit": current_state.get("limit") or 25,
            },
        )

        if pending_command == "prov":
            response_text, keyboard = _telegram_search_suppliers_keyboard(normalized_chat_id, pending_text or "/prov")
            tg_edit_message(chat_id=chat_id, message_id=message_id, text=response_text, reply_markup=keyboard)
        elif pending_command == "cat":
            response_text, keyboard = _telegram_search_categories_keyboard(normalized_chat_id, pending_text or "/cat")
            tg_edit_message(chat_id=chat_id, message_id=message_id, text=response_text, reply_markup=keyboard)
        elif pending_command == "count":
            tg_edit_message(chat_id=chat_id, message_id=message_id, text=_telegram_count_transactions(selected_project_id))
        elif pending_command == "sum":
            _, _, month_token = (pending_text or "").partition(" ")
            tg_edit_message(chat_id=chat_id, message_id=message_id, text=_telegram_sum_expenses(selected_project_id, month_token.strip(), include_iva=False))
        elif pending_command == "catid":
            tg_edit_message(chat_id=chat_id, message_id=message_id, text=_telegram_search_category_id(selected_project_id, pending_text or "/catid"))
        elif pending_command == "find":
            tg_edit_message(chat_id=chat_id, message_id=message_id, text=_telegram_search_find(selected_project_id, pending_text or "/find"))
        elif pending_command == "ask":
            ask_text = (pending_text or "").split(" ", 1)[1].strip() if (pending_text or "").startswith("/ask") else (pending_text or "")
            tg_edit_message(chat_id=chat_id, message_id=message_id, text=_telegram_ask_transactions(selected_project_id, ask_text))
        else:
            project_doc = db.projects.find_one({"_id": ObjectId(selected_project_id)}, {"name": 1}) or {}
            project_name = project_doc.get("name") or selected_project_id
            tg_edit_message(chat_id=chat_id, message_id=message_id, text=f"Proyecto seleccionado: {project_name}")

        tg_answer_callback_query(callback_query_id, "Proyecto seleccionado")
        return {"ok": True}

    if data in {"provPick:prev", "provPick:next", "catPick:prev", "catPick:next", "noop"}:
        mode = str(picker_state.get("mode") or "").strip()
        search_text = str(picker_state.get("searchText") or "").strip()
        page = _telegram_parse_page(str(picker_state.get("page") or 1))
        limit = _telegram_parse_limit(str(picker_state.get("limit") or 10), default=10, maximum=25)

        if data.endswith(":prev"):
            page = max(1, page - 1)
        elif data.endswith(":next"):
            page = page + 1

        selected_project_id = _telegram_get_selected_project_id(normalized_chat_id)
        if not selected_project_id:
            tg_answer_callback_query(callback_query_id, "Selecciona proyecto")
            return {"ok": True, "ignored": "missing_project"}

        if mode == "prov":
            response_text, keyboard, picker = _telegram_build_suppliers_keyboard(project_id=selected_project_id, search_text=search_text, page=page, limit=limit)
        elif mode == "cat":
            response_text, keyboard, picker = _telegram_build_categories_keyboard(project_id=selected_project_id, search_text=search_text, page=page, limit=limit)
        else:
            tg_answer_callback_query(callback_query_id, "Expiró selección")
            return {"ok": True, "ignored": "missing_picker_state"}

        _telegram_save_picker_state(
            chat_id=normalized_chat_id,
            mode=picker.get("mode") or mode,
            search_text=picker.get("searchText") or search_text,
            page=picker.get("page") or page,
            limit=picker.get("limit") or limit,
        )
        tg_edit_message(chat_id=chat_id, message_id=message_id, text=response_text, reply_markup=keyboard)
        tg_answer_callback_query(callback_query_id)
        return {"ok": True}

    if data.startswith("provSel:"):
        supplier_id = data.split(":", 1)[1].strip()
        selected_project_id = _telegram_get_selected_project_id(normalized_chat_id)
        state = {"projectId": selected_project_id, "mode": "prov", "supplierId": supplier_id, "categoryId": None, "page": 1, "limit": 25}
        _telegram_save_state(normalized_chat_id, state)
        text, keyboard = _telegram_build_transaction_message(state)
        tg_edit_message(chat_id=chat_id, message_id=message_id, text=text, reply_markup=keyboard)
        tg_answer_callback_query(callback_query_id)
        return {"ok": True}

    if data.startswith("catSel:"):
        category_id = data.split(":", 1)[1].strip()
        selected_project_id = _telegram_get_selected_project_id(normalized_chat_id)
        state = {"projectId": selected_project_id, "mode": "cat", "supplierId": None, "categoryId": category_id, "page": 1, "limit": 25}
        _telegram_save_state(normalized_chat_id, state)
        text, keyboard = _telegram_build_transaction_message(state)
        tg_edit_message(chat_id=chat_id, message_id=message_id, text=text, reply_markup=keyboard)
        tg_answer_callback_query(callback_query_id)
        return {"ok": True}

    state = _telegram_get_state(normalized_chat_id)
    if not state:
        tg_answer_callback_query(callback_query_id, "Expiró selección")
        return {"ok": True, "ignored": "missing_state"}

    if data == "nav:prev":
        state["page"] = max(1, _telegram_parse_page(str(state.get("page") or 1)) - 1)
    elif data == "nav:next":
        state["page"] = _telegram_parse_page(str(state.get("page") or 1)) + 1
    elif data == "total":
        total = _telegram_compute_total_for_state(state)
        tg_answer_callback_query(callback_query_id, f"Total acumulado: {_telegram_format_currency(total)}")
        return {"ok": True}
    elif data == "close":
        _telegram_clear_state(normalized_chat_id)
        tg_edit_message(chat_id=chat_id, message_id=message_id, text="Consulta cerrada.")
        tg_answer_callback_query(callback_query_id)
        return {"ok": True}
    else:
        tg_answer_callback_query(callback_query_id, "Acción inválida")
        return {"ok": True, "ignored": "invalid_callback_data"}

    _telegram_save_state(normalized_chat_id, state)
    text, keyboard = _telegram_build_transaction_message(state)
    tg_edit_message(chat_id=chat_id, message_id=message_id, text=text, reply_markup=keyboard)
    tg_answer_callback_query(callback_query_id)
    return {"ok": True}


def _resolve_default_project_id() -> str | None:
    default_project_id = (os.getenv("DEFAULT_PROJECT_ID") or "").strip()
    if default_project_id:
        return default_project_id
    return "699f9b894678d62c8d69f86d"


def _telegram_import_status_summary() -> str:
    cursor = db.importRuns.find({}, {"sourceDb": 1, "importRunId": 1, "_id": 1, "finishedAt": 1}).sort(
        [("finishedAt", -1), ("_id", -1)]
    )
    latest_by_source = {}
    for run in cursor:
        source_db = str(run.get("sourceDb") or "").strip().upper()
        if source_db not in {"IVA", "EFECTIVO"}:
            continue
        latest_by_source.setdefault(source_db, run)
        if len(latest_by_source) >= 2:
            break

    if not latest_by_source:
        return "Sin importRuns recientes de IVA/EFECTIVO"

    lines = ["📦 Import status"]
    for source_db in ("IVA", "EFECTIVO"):
        run = latest_by_source.get(source_db)
        if not run:
            lines.append(f"{source_db}: sin datos")
            continue
        run_id = str(run.get("_id"))
        finished_at = str(run.get("finishedAt") or "n/a")
        lines.append(f"{source_db}: importRunId={run_id} fecha={finished_at}")
    return "\n".join(lines)


def _telegram_count_transactions(project_id: str) -> str:
    total = db.transactions.count_documents({"projectId": project_id})
    return f"Total transactions projectId={project_id}: {total}"


def _telegram_sum_expenses(project_id: str, month_token: str, include_iva: bool = False) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", month_token or ""):
        return "Uso: /sum YYYY-MM"

    year, month = month_token.split("-", 1)
    start_date = f"{year}-{month}-01"
    month_int = int(month)
    next_year = int(year) + (1 if month_int == 12 else 0)
    next_month = 1 if month_int == 12 else month_int + 1
    end_date = f"{next_year:04d}-{next_month:02d}-01"

    movements = db.transactions.find(
        {"type": "EXPENSE", "projectId": project_id, "date": {"$gte": start_date, "$lt": end_date}},
        {"amount": 1, "tax": 1},
    )

    total = 0.0
    count = 0
    for tx in movements:
        count += 1
        amount_value = float(tx.get("amount") or 0)
        total += amount_value if include_iva else compute_monto_sin_iva(tx)

    return f"Egresos {month_token} (include_iva={str(include_iva).lower()}): {round(total, 2)} en {count} tx"


def _telegram_help_text() -> str:
    return (
        "Hola 👋\n"
        "Comandos soportados:\n"
        "/help - mostrar esta ayuda\n"
        "/start - mostrar esta ayuda\n"
        "/ping - prueba de conectividad\n"
        "/import_status - estado de última importación\n"
        "/count - contar transacciones del proyecto seleccionado\n"
        "/sum YYYY-MM - sumar egresos del mes (sin IVA)\n"
        "/project - seleccionar proyecto activo para consultas\n"
        "/prov [texto] - listar o buscar proveedor con botones\n"
        "/cat [texto] - listar o buscar categoría con botones\n"
        "/catid <categoryId> [limit] [page] - buscar por categoría\n"
        "/find <texto> [limit] [page] - buscar en concepto/descripción\n"
        "/ask <texto> - consulta natural simple (sin LLM)\n"
        "/chatid - mostrar y registrar este chat\n"
        "/users [approved|pending|all] - (admin) listar usuarios autorizados\n"
        "/user_find <texto> - (admin) buscar usuario telegram\n"
        "/user_add <chat_id|@username> - (admin) aprobar usuario\n"
        "/user_remove <chat_id|@username> - (admin) revocar usuario\n\n"
        "Ejemplos:\n"
        "/prov cementos\n"
        "/ping"
    )


def _telegram_parse_limit(value: str | None, default: int = 25, maximum: int = 50) -> int:
    if value is None or str(value).strip() == "":
        return default
    try:
        parsed = int(str(value).strip())
    except ValueError:
        return default
    if parsed <= 0:
        return default
    return min(parsed, maximum)


def _telegram_parse_page(value: str | None) -> int:
    if value is None or str(value).strip() == "":
        return 1
    try:
        parsed = int(str(value).strip())
    except ValueError:
        return 1
    return parsed if parsed > 0 else 1


def _telegram_parse_command_args(text: str) -> list[str]:
    return [token.strip() for token in (text or "").split() if token.strip()]


def _telegram_parse_search_params(raw_text: str) -> tuple[str, int, int]:
    args = _telegram_parse_command_args(raw_text)
    if len(args) < 2:
        return "", 25, 1

    tokens = args[1:]
    page = 1
    limit = 25
    if tokens and re.fullmatch(r"\d+", tokens[-1]):
        page = _telegram_parse_page(tokens[-1])
        tokens = tokens[:-1]
    if tokens and re.fullmatch(r"\d+", tokens[-1]):
        limit = _telegram_parse_limit(tokens[-1])
        tokens = tokens[:-1]

    search_text = " ".join(tokens).strip()
    return search_text, limit, page


def _telegram_format_currency(amount: float) -> str:
    return f"${round(float(amount or 0), 2):,.2f}"


def _telegram_monto_principal(tx: dict) -> tuple[float, float | None, float | None]:
    source_db = str(tx.get("sourceDb") or "").strip().upper()
    amount = round(float(tx.get("amount") or 0), 2)
    tax = tx.get("tax") if isinstance(tx.get("tax"), dict) else {}
    tax_subtotal = tax.get("subtotal")
    tax_iva = tax.get("iva")
    tax_total_factura = tax.get("totalFactura")

    subtotal_value = None
    iva_value = None
    total_factura_value = None
    try:
        if tax_subtotal is not None:
            subtotal_value = round(float(tax_subtotal), 2)
    except (TypeError, ValueError):
        subtotal_value = None
    try:
        if tax_iva is not None:
            iva_value = round(float(tax_iva), 2)
    except (TypeError, ValueError):
        iva_value = None
    try:
        if tax_total_factura is not None:
            total_factura_value = round(float(tax_total_factura), 2)
    except (TypeError, ValueError):
        total_factura_value = None

    if source_db == "IVA":
        return (subtotal_value if subtotal_value is not None else amount), iva_value, total_factura_value
    return amount, iva_value, total_factura_value


def _telegram_build_transactions_list(transactions: list[dict], limit_used: int, page: int) -> str:
    if not transactions:
        return f"Sin resultados. limit usados: {limit_used}, page: {page}"

    lines: list[str] = []
    total_principal = 0.0
    for tx in transactions:
        principal, iva_value, total_factura_value = _telegram_monto_principal(tx)
        total_principal += principal
        date_value = str(tx.get("date") or "").strip()[:10] or "(sin fecha)"
        concept = str(tx.get("concept") or tx.get("description") or "").strip() or "(sin concepto)"
        supplier_name = str(tx.get("supplierName") or "").strip() or "(sin proveedor)"
        source_db = str(tx.get("sourceDb") or "").strip().upper() or "N/A"

        line = f"{date_value} | {_telegram_format_currency(principal)} | {concept} | {supplier_name} | {source_db}"
        if source_db == "IVA":
            iva_text = _telegram_format_currency(iva_value or 0)
            total_factura_text = _telegram_format_currency(total_factura_value or 0)
            line += f" | IVA: {iva_text} | totalFactura: {total_factura_text}"
        lines.append(line)

    lines.append("")
    lines.append(f"Total listado: {_telegram_format_currency(total_principal)}")
    lines.append(f"limit usados: {limit_used}")
    lines.append(f"page: {page}")

    full_text = "\n".join(lines)
    if len(full_text) <= 3500:
        return full_text

    truncated = full_text[:3450].rstrip()
    return f"{truncated}\n\nTruncado, usa page {page + 1}"


def _telegram_query_transactions(project_id: str, query: dict, limit: int, page: int) -> str:
    effective_query = {"projectId": project_id, **query}
    skip = (page - 1) * limit
    projection = {
        "date": 1,
        "amount": 1,
        "tax": 1,
        "concept": 1,
        "description": 1,
        "supplierName": 1,
        "sourceDb": 1,
    }
    rows = list(db.transactions.find(effective_query, projection).sort([("date", -1), ("_id", -1)]).skip(skip).limit(limit))
    return _telegram_build_transactions_list(rows, limit_used=limit, page=page)


def _telegram_search_supplier(project_id: str, raw_text: str) -> str:
    text, limit, page = _telegram_parse_search_params(raw_text)
    if not text:
        return "Uso: /prov <texto> [limit] [page]"
    escaped = re.escape(text)
    query = {
        "$or": [
            {"supplierName": {"$regex": escaped, "$options": "i"}},
            {"supplierCardCode": {"$regex": escaped, "$options": "i"}},
        ]
    }
    return _telegram_query_transactions(project_id=project_id, query=query, limit=limit, page=page)


def _telegram_search_find(project_id: str, raw_text: str) -> str:
    text, limit, page = _telegram_parse_search_params(raw_text)
    if not text:
        return "Uso: /find <texto> [limit] [page]"
    escaped = re.escape(text)
    query = {
        "$or": [
            {"concept": {"$regex": escaped, "$options": "i"}},
            {"description": {"$regex": escaped, "$options": "i"}},
        ]
    }
    return _telegram_query_transactions(project_id=project_id, query=query, limit=limit, page=page)


def _telegram_search_category_name(project_id: str, raw_text: str) -> str:
    text, limit, _ = _telegram_parse_search_params(raw_text)
    if not text:
        return "Uso: /cat <texto> [limit]"
    escaped = re.escape(text)
    matches = list(
        db.categories.find({"projectId": project_id, "name": {"$regex": escaped, "$options": "i"}}, {"name": 1}).sort([("name", 1)]).limit(20)
    )

    if not matches:
        return "No encontré categorías con ese nombre"
    if len(matches) == 1:
        category_id = str(matches[0]["_id"])
        return _telegram_query_transactions(
            project_id=project_id,
            query={"$or": [{"categoryId": category_id}, {"category_id": category_id}]},
            limit=limit,
            page=1,
        )

    lines = ["Coincidencias de categoría:"]
    for index, row in enumerate(matches, start=1):
        lines.append(f"{index}. {row.get('name') or '(sin nombre)'} | id: {row.get('_id')}")
    lines.append("")
    lines.append(f"elige: /catid <id> [{limit}]")
    return "\n".join(lines)


def _telegram_search_category_id(project_id: str, raw_text: str) -> str:
    args = _telegram_parse_command_args(raw_text)
    if len(args) < 2:
        return "Uso: /catid <categoryId> [limit] [page]"

    category_id = args[1]
    limit = _telegram_parse_limit(args[2] if len(args) > 2 else None)
    page = _telegram_parse_page(args[3] if len(args) > 3 else None)
    return _telegram_query_transactions(
        project_id=project_id,
        query={"$or": [{"categoryId": category_id}, {"category_id": category_id}]},
        limit=limit,
        page=page,
    )


def _telegram_normalize_nlp_text(text: str) -> str:
    lowered = (text or "").strip().lower()
    normalized = unicodedata.normalize("NFD", lowered)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _telegram_classify_ask_action(text: str) -> str:
    normalized = _telegram_normalize_nlp_text(text)
    if "top" in normalized:
        return "top_suppliers"
    if any(token in normalized for token in ["cuanto", "total", "suma", "gastado"]):
        return "sum+list"
    if any(token in normalized for token in ["lista", "dame", "muestrame"]):
        return "list"
    return "sum+list"


def _telegram_detect_ask_keyword(text: str) -> str:
    normalized = _telegram_normalize_nlp_text(text)
    if " en " in f" {normalized}":
        after_en = normalized.rsplit(" en ", 1)[-1]
        phrase = re.split(r"\b(este|esta|mes|hoy|ayer|ultimos|ultimas|dias|de|del|por|para)\b", after_en, maxsplit=1)[0]
        candidate = " ".join(part for part in phrase.strip().split() if part)
        if candidate:
            return candidate

    stopwords = {
        "cuanto",
        "total",
        "suma",
        "gastado",
        "lista",
        "dame",
        "muestrame",
        "top",
        "proveedores",
        "proveedor",
        "este",
        "esta",
        "mes",
        "de",
        "del",
        "en",
    }
    words = [word for word in re.findall(r"[a-z0-9]+", normalized) if len(word) > 2 and word not in stopwords]
    return words[-1] if words else ""


def _telegram_keyword_synonyms(keyword: str) -> tuple[str, list[str]]:
    normalized_keyword = _telegram_normalize_nlp_text(keyword).strip()

    # Preferir categorías reales para que /ask siga la lógica del catálogo vivo.
    category_rows = list(db.categories.find({"active": {"$ne": False}}, {"name": 1}).limit(5000))
    category_candidates: list[tuple[str, str, list[str]]] = []
    for row in category_rows:
        raw_name = str(row.get("name") or "").strip()
        if not raw_name:
            continue

        normalized_name = _telegram_normalize_nlp_text(raw_name)
        tokens = [token for token in re.findall(r"[a-z0-9]+", normalized_name) if len(token) > 2]
        terms = [raw_name, normalized_name, *tokens]
        deduped_terms = [term for term in dict.fromkeys(term.strip() for term in terms if term and term.strip())]
        category_candidates.append((raw_name, normalized_name, deduped_terms))

    if normalized_keyword:
        for canonical_name, normalized_name, terms in category_candidates:
            if normalized_keyword == normalized_name:
                return canonical_name, terms

        for canonical_name, normalized_name, terms in category_candidates:
            if normalized_keyword in normalized_name or normalized_name in normalized_keyword:
                return canonical_name, terms

        keyword_tokens = [token for token in re.findall(r"[a-z0-9]+", normalized_keyword) if len(token) > 2]
        for canonical_name, _, terms in category_candidates:
            normalized_terms = {_telegram_normalize_nlp_text(term) for term in terms}
            if any(token in normalized_terms for token in keyword_tokens):
                return canonical_name, terms

    synonym_map = {
        "madera": ["madera", "carpinter", "mdf", "triplay", "melamina", "pino", "tablero", "cimbra"],
        "electricidad": ["electricidad", "electr", "cable", "apagador", "contacto", "cfe", "centro de carga"],
        "cemento": ["cemento", "concreto", "mortero", "block", "varilla", "acero"],
        "pintura": ["pintura", "esmalte", "sellador", "impermeabilizante"],
        "plomeria": ["plomeria", "tuberia", "hidraul", "sanitari", "wc", "llave"],
    }

    for canonical, synonyms in synonym_map.items():
        for synonym in synonyms:
            normalized_synonym = _telegram_normalize_nlp_text(synonym)
            if normalized_keyword == normalized_synonym or normalized_synonym in normalized_keyword or normalized_keyword in normalized_synonym:
                return canonical, synonyms

    fallback = normalized_keyword.strip()
    return (fallback or "general", [fallback or "gasto"])


def _telegram_ask_transactions(project_id: str, text: str) -> str:
    action = _telegram_classify_ask_action(text)
    raw_keyword = _telegram_detect_ask_keyword(text)
    keyword, synonyms = _telegram_keyword_synonyms(raw_keyword)
    regex_terms = [re.escape(term) for term in synonyms if term]
    if not regex_terms:
        regex_terms = [re.escape(keyword)]
    keyword_regex = "|".join(regex_terms)

    category_matches = list(
        db.categories.find(
            {"projectId": project_id, "name": {"$regex": keyword_regex, "$options": "i"}},
            {"_id": 1},
        ).limit(200)
    )
    category_ids = [str(row.get("_id")) for row in category_matches if row.get("_id") is not None]

    base_match = {
        "projectId": project_id,
        "type": "EXPENSE",
    }

    search_or: list[dict] = [
        {"concept": {"$regex": keyword_regex, "$options": "i"}},
        {"description": {"$regex": keyword_regex, "$options": "i"}},
        {"supplierName": {"$regex": keyword_regex, "$options": "i"}},
    ]
    if category_ids:
        search_or.append({"categoryId": {"$in": category_ids}})
        search_or.append({"category_id": {"$in": category_ids}})

    query = {**base_match, "$or": search_or}

    if action == "top_suppliers":
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": {"$ifNull": ["$supplierName", "(sin proveedor)"]},
                    "total": {"$sum": {"$ifNull": ["$amount", 0]}},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"total": -1}},
            {"$limit": 10},
        ]
        rows = list(db.transactions.aggregate(pipeline))
        if not rows:
            return f"Sin resultados para '{keyword}'"
        lines = [f"Top proveedores '{keyword}' (global):"]
        grand_total = 0.0
        for idx, row in enumerate(rows, start=1):
            total = round(float(row.get("total") or 0), 2)
            grand_total += total
            lines.append(f"{idx}. {row.get('_id') or '(sin proveedor)'} | {_telegram_format_currency(total)} | {row.get('count', 0)} tx")
        lines.append("")
        lines.append(f"Total top: {_telegram_format_currency(grand_total)}")
        return "\n".join(lines)

    rows = list(
        db.transactions.find(
            query,
            {"date": 1, "amount": 1, "tax": 1, "concept": 1, "description": 1, "supplierName": 1, "sourceDb": 1},
        )
        .sort([("date", -1), ("_id", -1)])
        .limit(25)
    )

    if not rows:
        return f"Sin resultados para '{keyword}'"

    header = f"Consulta '{keyword}' ({action}) | rango global"
    body = _telegram_build_transactions_list(rows, limit_used=25, page=1)
    return f"{header}\n\n{body}"


def _format_import_bucket(label: str, summary: dict | None) -> str:
    bucket = summary if isinstance(summary, dict) else {}
    rows_ok = int(bucket.get("rowsOk", 0) or 0)
    duplicates_skipped = int(bucket.get("duplicatesSkipped", 0) or 0)
    new_rows = rows_ok - duplicates_skipped
    import_run_id = bucket.get("importRunId")

    line = f"{label}: new={new_rows} parsed={rows_ok} dup={duplicates_skipped}"
    if new_rows == 0:
        line += " | Sin cambios"
    if import_run_id:
        line += f" | importRunId={import_run_id}"
    return line


def _summarize_error(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        detail = exc.detail
        if isinstance(detail, dict):
            values = [str(detail.get(k) or "") for k in ("status", "error", "detail")]
            message = " ".join(value for value in values if value).strip() or str(detail)
        else:
            message = str(detail)
    else:
        message = str(exc)
    return (message or exc.__class__.__name__).replace("\n", " ").strip()[:300]


def _normalize_trigger_source(trigger_source: str | None) -> str:
    normalized = str(trigger_source or "").strip().lower()
    if normalized in {"cron", "frontend", "api"}:
        return normalized
    return "api"


def _summarize_actor(actor: str | None) -> str:
    normalized = str(actor or "").strip()
    return normalized or "system"


def _build_sap_movements_success_message(
    *,
    sbo: str,
    mode: str,
    trigger_source: str,
    actor: str,
    result: dict,
) -> str:
    lines = [
        "✅ SAP import",
        f"SBO: {sbo}",
        f"Modo: {mode}",
        f"Origen: {trigger_source}",
        f"Actor: {actor}",
        f"Rows total: {int(result.get('rowsTotal') or 0)}",
        f"Rows ok: {int(result.get('rowsOk') or 0)}",
        f"Imported: {int(result.get('imported') or 0)}",
        f"Updated: {int(result.get('updated') or 0)}",
        f"Unmatched: {int(result.get('unmatched') or 0)}",
        f"ImportRunId: {result.get('importRunId') or '-'}",
    ]
    return "\n".join(lines)


def _build_sap_movements_already_imported_message(
    *,
    sbo: str,
    mode: str,
    trigger_source: str,
    actor: str,
    import_run_id: str | None,
) -> str:
    lines = [
        "ℹ️ SAP import",
        f"SBO: {sbo}",
        f"Modo: {mode}",
        f"Origen: {trigger_source}",
        f"Actor: {actor}",
        "Resultado: already imported",
        f"ImportRunId: {import_run_id or '-'}",
    ]
    return "\n".join(lines)


def _build_sap_movements_error_message(*, sbo: str, mode: str, trigger_source: str, actor: str, error: str) -> str:
    lines = [
        "❌ SAP import",
        f"SBO: {sbo}",
        f"Modo: {mode}",
        f"Origen: {trigger_source}",
        f"Actor: {actor}",
        f"Error: {error}",
    ]
    return "\n".join(lines)


def notify_sap_movements_by_sbo_result(
    *,
    sbo: str,
    mode: str,
    trigger_source: str,
    actor: str,
    result: dict,
) -> None:
    normalized_source = _normalize_trigger_source(trigger_source)
    normalized_actor = _summarize_actor(actor)

    if result.get("already_imported"):
        message = _build_sap_movements_already_imported_message(
            sbo=sbo,
            mode=mode,
            trigger_source=normalized_source,
            actor=normalized_actor,
            import_run_id=str(result.get("importRunId") or "") or None,
        )
    else:
        message = _build_sap_movements_success_message(
            sbo=sbo,
            mode=mode,
            trigger_source=normalized_source,
            actor=normalized_actor,
            result=result,
        )

    sent = send_telegram_import_message(message)
    logger.info(
        "Telegram SAP movements notification delivered source=%s actor=%s sent=%s",
        normalized_source,
        normalized_actor,
        sent,
    )


def notify_sap_movements_by_sbo_error(
    *,
    sbo: str,
    mode: str,
    trigger_source: str,
    actor: str,
    exc: Exception,
) -> None:
    message = _build_sap_movements_error_message(
        sbo=sbo,
        mode=mode,
        trigger_source=_normalize_trigger_source(trigger_source),
        actor=_summarize_actor(actor),
        error=_summarize_error(exc),
    )
    sent = send_telegram_import_message(message)
    logger.info("Telegram SAP movements error notification delivered sent=%s", sent)


def notify_sap_latest_import_success(project: str, result: dict):
    message = (
        f"✅ SAP import OK ({project})\n"
        f"{_format_import_bucket('IVA', result.get('iva'))}\n"
        f"{_format_import_bucket('EFECTIVO', result.get('efectivo'))}\n"
        f"Fecha: {datetime.now(timezone.utc).isoformat()}"
    )
    send_telegram(message)


def notify_sap_latest_import_failure(project: str, exc: Exception):
    message = (
        f"❌ SAP import FAIL ({project})\n"
        f"Error: {_summarize_error(exc)}\n"
        f"Fecha: {datetime.now(timezone.utc).isoformat()}"
    )
    send_telegram(message)


def notify_import(summary: dict):
    chat_id = get_telegram_default_chat_id()
    if chat_id is None:
        logger.info("SAP import notification skipped: no Telegram chat_id configured")
        return False

    message = (
        "📦 Import SAP finalizado\n"
        f"Rows OK: {summary.get('rowsOk', 0)}\n"
        f"Duplicados: {summary.get('duplicates', 0)}\n"
        f"Errores: {summary.get('errors', 0)}"
    )
    return tg_send(chat_id=chat_id, text=message)


def ensure_indexes():
    db.users.create_index("username", unique=True)
    db.suppliers.create_index("cardCode", unique=True)
    try:
        db.categories.create_index(
            [("projectId", 1), ("code", 1)],
            unique=True,
            name="categories_project_code_unique",
        )
    except OperationFailure as exc:
        error_message = str(exc)
        if exc.code == 86 or "same name" in error_message.lower():
            logger.warning(
                "Skipping categories_project_code_unique creation due to index conflict: %s",
                error_message,
            )
        else:
            raise
    db.supplierCategoryCatalog.create_index("name", unique=True)
    try:
        db.supplierCategories.drop_index("name_1")
    except OperationFailure:
        pass
    db.supplierCategories.create_index(
        [("projectId", 1), ("supplierId", 1)],
        unique=True,
        name="supplier_categories_project_supplier_unique",
    )
    db.supplierCategory2Rules.update_many(
        {"supplierKey": ""},
        {"$unset": {"supplierKey": ""}},
    )
    db.supplierCategory2Rules.create_index(
        [("supplierKey", 1)],
        unique=True,
        name="supplier_category2_rules_supplier_key_unique",
        partialFilterExpression={"supplierKey": {"$exists": True, "$type": "string", "$gt": ""}},
    )
    db.supplierCategory2Rules.create_index([("isActive", 1), ("updatedAt", -1)], name="supplier_category2_rules_active_updated_idx")
    db.projects.create_index("name", unique=True)
    db.projects.create_index("slug", unique=True)
    db.payments.create_index([("projectId", 1), ("sapPaymentNum", 1)], unique=True)
    db.apInvoices.create_index([("projectId", 1), ("sapInvoiceNum", 1)], unique=True)
    db.paymentLines.create_index([("paymentId", 1), ("apInvoiceId", 1), ("appliedAmount", 1)], unique=True)
    import_runs_indexes = {idx.get("name"): idx for idx in db.importRuns.list_indexes()}
    legacy_sha_index = import_runs_indexes.get("sha256_1")
    if legacy_sha_index and legacy_sha_index.get("unique"):
        db.importRuns.drop_index("sha256_1")
    db.importRuns.create_index([("projectId", 1), ("sha256", 1)], unique=True, name="import_runs_project_sha256_unique")
    db.importRuns.create_index([("projectId", 1), ("importKey", 1)], name="import_runs_project_import_key_idx")
    db.sap_import_state.create_index(
        [("projectId", 1), ("sourceDb", 1), ("sourceSbo", 1)],
        unique=True,
        name="sap_import_state_project_source_unique",
    )
    db.unmatched_projects.create_index(
        [("sourceSbo", 1), ("normalizedProjectName", 1)],
        unique=True,
        name="unmatched_projects_source_sbo_project_name_unique",
    )
    db.adminActions.create_index([("projectId", 1), ("requestedAt", -1)])
    db.adminActions.create_index([("action", 1), ("requestedAt", -1)])
    db.settings.create_index("key", unique=True)
    db.telegram_users.create_index("chat_id", unique=True)
    db.telegram_users.create_index([("status", 1), ("updated_at", -1)])
    db.telegram_users.create_index([("username", 1)])
    db.telegram_state.create_index("chat_id", unique=True)
    db.telegram_state.create_index([("updated_at", -1)])
    try:
        backfill_sap_transactions_metadata()
    except Exception:
        logger.exception("SAP metadata backfill failed during startup; continuing without blocking server startup")
    try:
        backfill_sbo_project_resolution_suspicious_flags()
    except Exception:
        logger.exception("SBO suspicious project-resolution backfill failed during startup; continuing without blocking server startup")
    try:
        repair_partial_suspicious_project_resolutions()
    except Exception:
        logger.exception("SBO suspicious project-resolution repair failed during startup; continuing without blocking server startup")
    dedupe_sap_transactions_for_unique_index()
    db.transactions.create_index(
        [
            ("projectId", 1),
            ("source", 1),
            ("sourceDb", 1),
            ("sap.pagoNum", 1),
            ("sap.facturaNum", 1),
            ("sap.montoAplicadoCents", 1),
        ],
        unique=True,
        partialFilterExpression={"source": "sap"},
        name="sap_transactions_unique_v2_cents",
    )
    db.transactions.create_index([("dedupeKey", 1)], name="transactions_dedupe_key_idx")
    db.transactions.create_index([("projectId", 1), ("date", -1)])
    db.transactions.create_index([("projectId", 1)], name="transactions_project_id_idx")
    db.transactions.create_index(
        [("projectId", 1), ("sourceDb", 1), ("category_id", 1)],
        name="transactions_project_source_db_category_idx",
    )
    db.transactions.create_index([("sap.projectId", 1)], name="transactions_sap_project_id_idx")
    db.transactions.create_index(
        [("sap.isProjectResolutionSuspicious", 1), ("sap.manualResolvedProjectId", 1), ("date", -1)],
        name="transactions_sap_project_resolution_suspicious_idx",
    )
    db.vendors.create_index(
        [("projectId", 1), ("supplierCardCode", 1)],
        unique=True,
        name="vendors_project_supplier_card_code_unique",
        partialFilterExpression={"projectId": {"$exists": True}, "supplierCardCode": {"$exists": True, "$type": "string"}},
    )
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


def infer_sap_source_db(tx: dict) -> str:
    source_db = str(tx.get("sourceDb") or "").strip().upper()
    if source_db:
        return source_db

    tax = tx.get("tax") or {}
    iva_value = tax.get("iva")
    if iva_value is None:
        return "UNKNOWN"

    try:
        if Decimal(str(iva_value)) > 0:
            return "IVA"
    except (InvalidOperation, ValueError, TypeError):
        return "UNKNOWN"
    return "EFECTIVO"


def infer_sap_source_db_for_backfill(tx: dict) -> str:
    tax = tx.get("tax") or {}
    iva_value = tax.get("iva")
    try:
        return "IVA" if Decimal(str(iva_value)) > 0 else "EFECTIVO"
    except (InvalidOperation, ValueError, TypeError):
        return "EFECTIVO"


def backfill_sbo_project_resolution_suspicious_flags():
    query = {
        "source": "sap-sbo",
        "sap": {"$exists": True},
        "sap.movementType": "egreso",
        "sap.documentProjectCode": {"$exists": True},
        "sap.paymentProjectCode": {"$exists": True},
    }
    ops = []
    projection = {
        "sap.documentProjectCode": 1,
        "sap.documentProjectName": 1,
        "sap.paymentProjectCode": 1,
        "sap.paymentProjectName": 1,
        "sap.isProjectResolutionSuspicious": 1,
        "sap.projectResolutionSuspicionReasons": 1,
        "sap.suggestedProjectCode": 1,
        "sap.suggestedProjectName": 1,
        "sap.conflictingPaymentProjectCode": 1,
        "sap.conflictingPaymentProjectName": 1,
        "sap.rawProjectCode": 1,
        "sap.rawProjectName": 1,
        "sap.projectResolutionSource": 1,
    }

    scanned = 0
    for tx in db.transactions.find(query, projection):
        scanned += 1
        sap_doc = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
        project_fields = {
            "document_project_code": normalize_non_empty_string(sap_doc.get("documentProjectCode")),
            "document_project_name": normalize_non_empty_string(sap_doc.get("documentProjectName")),
            "payment_project_code": normalize_non_empty_string(sap_doc.get("paymentProjectCode")),
            "payment_project_name": normalize_non_empty_string(sap_doc.get("paymentProjectName")),
        }
        suspicious = build_project_resolution_suspicion_fields("egreso", project_fields)

        desired_set = {
            "sap.isProjectResolutionSuspicious": suspicious["isProjectResolutionSuspicious"],
            "sap.projectResolutionSuspicionReasons": suspicious["projectResolutionSuspicionReasons"],
            "sap.suggestedProjectCode": suspicious["suggestedProjectCode"],
            "sap.suggestedProjectName": suspicious["suggestedProjectName"],
            "sap.conflictingPaymentProjectCode": suspicious["conflictingPaymentProjectCode"],
            "sap.conflictingPaymentProjectName": suspicious["conflictingPaymentProjectName"],
            "sap.rawProjectCode": project_fields.get("document_project_code") or normalize_non_empty_string(sap_doc.get("rawProjectCode")),
            "sap.rawProjectName": project_fields.get("document_project_name") or normalize_non_empty_string(sap_doc.get("rawProjectName")),
            "sap.projectResolutionSource": "document",
        }

        requires = False
        for k,v in desired_set.items():
            parts=k.split('.')[1:]
            cur=sap_doc
            for part in parts:
                cur=cur.get(part) if isinstance(cur,dict) else None
            if cur!=v:
                requires=True
                break
        if not requires:
            continue
        ops.append(UpdateOne({"_id": tx["_id"]}, {"$set": desired_set}))

    if ops:
        result = db.transactions.bulk_write(ops, ordered=False)
        return {"scanned": scanned, "updated": (result.modified_count or 0)}
    return {"scanned": scanned, "updated": 0}




def repair_partial_suspicious_project_resolutions():
    query = {
        "source": "sap-sbo",
        "sap.isProjectResolutionSuspicious": True,
        "$expr": {
            "$eq": [
                {
                    "$strLenCP": {
                        "$trim": {
                            "input": {"$toString": {"$ifNull": ["$sap.manualResolvedProjectId", ""]}}
                        }
                    }
                },
                0,
            ]
        },
        "$or": [
            {"sap.manualResolvedProjectName": {"$exists": True, "$ne": None}},
            {"sap.manualResolvedAt": {"$exists": True, "$ne": None}},
        ],
    }
    projection = {
        "sap.manualResolvedProjectCode": 1,
        "sap.manualResolvedProjectName": 1,
        "sap.manualResolvedBy": 1,
        "sap.manualResolvedAt": 1,
        "sap.manualResolutionReason": 1,
    }
    scanned = 0
    fixed = 0
    cleared = 0
    for tx in db.transactions.find(query, projection):
        scanned += 1
        sap_doc = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
        project_id, project_code, project_name = resolve_project_metadata_from_code_or_name(
            project_code=normalize_non_empty_string(sap_doc.get("manualResolvedProjectCode")),
            project_name=normalize_non_empty_string(sap_doc.get("manualResolvedProjectName")),
        )
        if project_id:
            db.transactions.update_one(
                {"_id": tx.get("_id")},
                {
                    "$set": {
                        "sap.manualResolvedProjectId": project_id,
                        "sap.manualResolvedProjectCode": project_code,
                        "sap.manualResolvedProjectName": project_name,
                    }
                },
            )
            fixed += 1
            continue

        db.transactions.update_one(
            {"_id": tx.get("_id")},
            {
                "$unset": {
                    "sap.manualResolvedProjectId": "",
                    "sap.manualResolvedProjectCode": "",
                    "sap.manualResolvedProjectName": "",
                    "sap.manualResolvedBy": "",
                    "sap.manualResolvedAt": "",
                    "sap.manualResolutionReason": "",
                }
            },
        )
        cleared += 1

    return {"scanned": scanned, "fixed": fixed, "cleared": cleared}

def backfill_sap_transactions_metadata():
    query = {
        "$or": [
            {"source": "sap"},
            {"sap": {"$exists": True}},
        ]
    }
    ops = []
    projection = {
        "source": 1,
        "sourceDb": 1,
        "sourceDbInferred": 1,
        "tax": 1,
        "amount": 1,
        "sap": 1,
    }
    for tx in db.transactions.find(query, projection):
        sap_doc = tx.get("sap") if isinstance(tx.get("sap"), dict) else None
        if not sap_doc:
            continue

        normalized_sap = normalize_sap_fields(sap_doc, fallback_amount=tx.get("amount"))
        current_source_db = str(tx.get("sourceDb") or "").strip().upper()
        source_db_was_missing = not current_source_db
        normalized_source_db = current_source_db or infer_sap_source_db_for_backfill(tx)
        inferred_flag_missing = source_db_was_missing and tx.get("sourceDbInferred") is not True

        current_pago_num = str(sap_doc.get("pagoNum") or "").strip()
        current_factura_num = str(sap_doc.get("facturaNum") or "").strip()
        current_amount = round(float(sap_doc.get("montoAplicado") or tx.get("amount") or 0), 2)
        current_cents = sap_doc.get("montoAplicadoCents")

        requires_update = any(
            [
                tx.get("source") != "sap",
                current_source_db != normalized_source_db,
                inferred_flag_missing,
                current_pago_num != normalized_sap["pagoNum"],
                current_factura_num != normalized_sap["facturaNum"],
                current_amount != normalized_sap["montoAplicado"],
                current_cents != normalized_sap["montoAplicadoCents"],
            ]
        )
        if not requires_update:
            continue

        set_values = {
            "source": "sap",
            "sourceDb": normalized_source_db,
            "sap.pagoNum": normalized_sap["pagoNum"],
            "sap.facturaNum": normalized_sap["facturaNum"],
            "sap.montoAplicado": normalized_sap["montoAplicado"],
            "sap.montoAplicadoCents": normalized_sap["montoAplicadoCents"],
        }
        if source_db_was_missing:
            set_values["sourceDbInferred"] = True

        ops.append(
            UpdateOne(
                {"_id": tx["_id"]},
                {
                    "$set": set_values
                },
            )
        )

    if ops:
        result = db.transactions.bulk_write(ops, ordered=False)
        return {"scanned": len(ops), "updated": (result.modified_count or 0) + (result.upserted_count or 0)}
    return {"scanned": 0, "updated": 0}


def _is_missing(value) -> bool:
    return value in (None, "", [], {})


def _pick_sap_winner(candidates: list[dict]) -> dict:
    def sort_key(doc: dict):
        has_category = doc.get("categoryId") or doc.get("category_id")
        created_at = str(doc.get("created_at") or "")
        return (1 if has_category else 0, created_at, doc.get("_id"))

    return max(candidates, key=sort_key)


def dedupe_sap_transactions_for_unique_index():
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
    for tx in db.transactions.find({"source": "sap", "sap": {"$exists": True}}, projection):
        normalized_sap = normalize_sap_fields(tx.get("sap"), fallback_amount=tx.get("amount"))
        key = (
            tx.get("projectId"),
            infer_sap_source_db(tx),
            normalized_sap["pagoNum"],
            normalized_sap["facturaNum"],
            normalized_sap["montoAplicadoCents"],
        )
        groups.setdefault(key, []).append(tx)

    duplicate_groups = [group for group in groups.values() if len(group) > 1]
    if not duplicate_groups:
        return {"groups": 0, "deleted": 0}

    deleted_ids = []
    winner_updates = []
    merge_fields = ["categoryId", "category_id", "supplierId", "supplierName", "supplierCardCode", "vendor_id", "tax"]
    for group in duplicate_groups:
        winner = _pick_sap_winner(group)
        merged_values = {}

        for candidate in group:
            if candidate["_id"] == winner["_id"]:
                continue
            deleted_ids.append(candidate["_id"])
            for field in merge_fields:
                if _is_missing(winner.get(field)) and not _is_missing(candidate.get(field)):
                    winner[field] = candidate.get(field)
                    merged_values[field] = candidate.get(field)

        if merged_values:
            winner_updates.append(UpdateOne({"_id": winner["_id"]}, {"$set": merged_values}))

    if winner_updates:
        db.transactions.bulk_write(winner_updates, ordered=False)
    if deleted_ids:
        db.transactions.delete_many({"_id": {"$in": deleted_ids}})

    return {"groups": len(duplicate_groups), "deleted": len(deleted_ids)}


def run_sap_manual_migration_and_dedupe():
    backfill_result = backfill_sap_transactions_metadata()
    dedupe_result = dedupe_sap_transactions_for_unique_index()
    return {
        "backfill": backfill_result,
        "dedupe": dedupe_result,
    }
    result = dedupe_sap_transactions()
    return result


def dedupe_sap_transactions(project_id: str | None = None, dry_run: bool = True) -> dict:
    query = {"source": "sap", "sap": {"$exists": True}}
    if project_id:
        query["projectId"] = project_id

    projection = {
        "projectId": 1,
        "sourceDb": 1,
        "sap": 1,
        "amount": 1,
        "categoryId": 1,
        "category_id": 1,
    }

    grouped = {}
    for tx in db.transactions.find(query, projection):
        normalized_sap = normalize_sap_fields(tx.get("sap"), fallback_amount=tx.get("amount"))
        group_key = (
            str(tx.get("projectId") or "").strip(),
            normalize_source_db_value(tx.get("sourceDb")),
            normalized_sap["pagoNum"],
            normalized_sap["facturaNum"],
            normalized_sap["montoAplicadoCents"],
        )
        grouped.setdefault(group_key, []).append(tx)

    groups_found = 0
    docs_deleted = 0
    categories_copied = 0

    updates = []
    ids_to_delete = []

    for docs in grouped.values():
        if len(docs) <= 1:
            continue
        groups_found += 1

        winner_candidates = sorted(
            docs,
            key=lambda doc: (
                1 if _has_category(doc) else 0,
                str(doc.get("_id") or ""),
            ),
            reverse=True,
        )
        winner = winner_candidates[0]

        if not _has_category(winner):
            source_category = None
            for candidate in winner_candidates[1:]:
                if str(candidate.get("categoryId") or "").strip():
                    source_category = {
                        "categoryId": candidate.get("categoryId"),
                        "category_id": candidate.get("categoryId"),
                    }
                    break
                if str(candidate.get("category_id") or "").strip():
                    source_category = {
                        "categoryId": candidate.get("category_id"),
                        "category_id": candidate.get("category_id"),
                    }
                    break

            if source_category:
                categories_copied += 1
                updates.append(UpdateOne({"_id": winner["_id"]}, {"$set": source_category}))

        for candidate in docs:
            if candidate["_id"] == winner["_id"]:
                continue
            ids_to_delete.append(candidate["_id"])

    docs_deleted = len(ids_to_delete)

    if not dry_run:
        if updates:
            db.transactions.bulk_write(updates, ordered=False)
        if ids_to_delete:
            db.transactions.delete_many({"_id": {"$in": ids_to_delete}})

    return {
        "mode": "bySapKey",
        "projectId": project_id,
        "dryRun": dry_run,
        "groupsFound": groups_found,
        "docsDeleted": docs_deleted,
        "categoriesCopied": categories_copied,
    }


def _has_category(tx: dict) -> bool:
    return bool(str(tx.get("categoryId") or tx.get("category_id") or "").strip())


def _build_iva_duplicate_key(tx: dict):
    amount_value = round(float(tx.get("amount") or 0), 2)
    return (
        str(tx.get("projectId") or "").strip(),
        str(tx.get("date") or "").strip(),
        amount_value,
        str(tx.get("supplierCardCode") or "").strip(),
        str(tx.get("concept") or "").strip(),
    )


def cleanup_sap_iva_duplicates(project_id: str, dry_run: bool = True):
    projection = {
        "projectId": 1,
        "date": 1,
        "amount": 1,
        "supplierCardCode": 1,
        "concept": 1,
        "source": 1,
        "sourceDb": 1,
        "categoryId": 1,
        "category_id": 1,
    }
    query = {
        "projectId": project_id,
        "source": "sap",
        "sourceDb": {"$in": ["IVA", "EFECTIVO"]},
    }

    transactions = list(db.transactions.find(query, projection))
    grouped = {}
    for tx in transactions:
        key = _build_iva_duplicate_key(tx)
        grouped.setdefault(key, {"IVA": [], "EFECTIVO": []})
        source_db = str(tx.get("sourceDb") or "").strip().upper()
        if source_db in grouped[key]:
            grouped[key][source_db].append(tx)

    pair_examples = []
    iva_ids_to_delete = []
    updates_to_apply = []
    categories_copied = 0
    pairs_found = 0

    for key, buckets in grouped.items():
        iva_docs = sorted(buckets["IVA"], key=lambda doc: str(doc.get("_id")))
        efectivo_docs = sorted(buckets["EFECTIVO"], key=lambda doc: str(doc.get("_id")))
        if not iva_docs or not efectivo_docs:
            continue

        pair_count = min(len(iva_docs), len(efectivo_docs))
        pairs_found += pair_count

        for idx in range(pair_count):
            iva_doc = iva_docs[idx]
            efectivo_doc = efectivo_docs[idx]
            iva_ids_to_delete.append(iva_doc["_id"])

            should_copy_category = _has_category(iva_doc) and not _has_category(efectivo_doc)
            if should_copy_category:
                category_update = {}
                if str(iva_doc.get("categoryId") or "").strip():
                    category_update["categoryId"] = iva_doc.get("categoryId")
                if str(iva_doc.get("category_id") or "").strip():
                    category_update["category_id"] = iva_doc.get("category_id")

                if category_update:
                    categories_copied += 1
                    updates_to_apply.append(UpdateOne({"_id": efectivo_doc["_id"]}, {"$set": category_update}))

            if len(pair_examples) < 20:
                pair_examples.append(
                    {
                        "ivaId": str(iva_doc["_id"]),
                        "efectivoId": str(efectivo_doc["_id"]),
                        "projectId": key[0],
                        "date": key[1],
                        "amount": key[2],
                        "supplierCardCode": key[3],
                        "concept": key[4],
                        "categoryCopied": should_copy_category,
                    }
                )

    if not dry_run:
        if updates_to_apply:
            db.transactions.bulk_write(updates_to_apply, ordered=False)
        if iva_ids_to_delete:
            db.transactions.delete_many({"_id": {"$in": iva_ids_to_delete}})

    return {
        "pairsFound": pairs_found,
        "ivaDeleted": 0 if dry_run else len(iva_ids_to_delete),
        "categoriesCopied": categories_copied,
        "examples": pair_examples,
        "dryRun": dry_run,
        "projectId": project_id,
    }


def drop_legacy_sap_unique_index():
    legacy_index_name = "projectId_1_sap.pagoNum_1_sap.facturaNum_1_sap.montoAplicado_1"
    current_indexes = {idx.get("name"): idx for idx in db.transactions.list_indexes()}
    if legacy_index_name in current_indexes:
        db.transactions.drop_index(legacy_index_name)


def create_token(username: str, role: str, display_name: str):
    exp = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS)
    payload = {"sub": username, "role": role, "displayName": display_name, "name": display_name, "exp": exp}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

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


def normalize_category_name(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def normalize_global_category_key(value: str | None) -> str:
    return normalize_text_for_matching(value)


def clean_global_category_name(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


GLOBAL_CATEGORIES_PROJECT_ID = "__global__"


def build_global_category_code(value: str | None) -> str | None:
    key = normalize_global_category_key(value)
    if not key:
        return None
    token = re.sub(r"[^a-z0-9]+", "-", key).strip("-")
    token = re.sub(r"-+", "-", token)
    if not token:
        token = sha256(key.encode("utf-8")).hexdigest()[:12]
    return f"global:{token}"


def iter_existing_global_category_names() -> list[str]:
    names: list[str] = []

    for row in db.transactions.aggregate(
        [
            {
                "$project": {
                    "names": [
                        "$categoryEffectiveName",
                        "$categoryManualName",
                        "$categoryHintName",
                    ]
                }
            },
            {"$unwind": "$names"},
            {"$match": {"names": {"$type": "string", "$nin": ["", None]}}},
            {"$group": {"_id": "$names"}},
        ]
    ):
        name = clean_global_category_name(row.get("_id"))
        if name:
            names.append(name)

    for row in db.supplierCategory2Rules.find(
        {"category2Name": {"$exists": True, "$nin": [None, ""]}},
        {"category2Name": 1},
    ):
        name = clean_global_category_name(row.get("category2Name"))
        if name:
            names.append(name)

    return names


def ensure_global_categories_catalog() -> dict:
    existing_rows = list(db.categories.find({}, {"name": 1, "active": 1, "nameKey": 1, "normalizedName": 1, "code": 1, "projectId": 1}))
    by_key: dict[str, dict] = {}
    by_code: dict[str, dict] = {}

    for row in existing_rows:
        name = clean_global_category_name(row.get("name"))
        key = normalize_global_category_key(name)
        if not key:
            continue

        row_id = row.get("_id")
        current = by_key.get(key)
        if current is None:
            by_key[key] = row
        else:
            current_is_active = current.get("active") is not False
            row_is_active = row.get("active") is not False
            if row_is_active and not current_is_active:
                by_key[key] = row
            elif row_is_active == current_is_active and str(row_id) < str(current.get("_id")):
                by_key[key] = row

        code = normalize_non_empty_string(row.get("code"))
        if code and code not in by_code:
            by_code[code] = row

    candidate_names = [
        *[clean_global_category_name(row.get("name")) for row in existing_rows],
        *DEFAULT_CATEGORIES,
        *iter_existing_global_category_names(),
    ]

    seeded = 0
    reactivated = 0
    normalized_updates = 0

    for raw_name in candidate_names:
        name = clean_global_category_name(raw_name)
        key = normalize_global_category_key(name)
        code = build_global_category_code(name)
        if not name or not key or not code:
            continue

        keeper = by_key.get(key)
        if keeper is None:
            keeper = by_code.get(code)
            if keeper is not None:
                by_key[key] = keeper

        if keeper is None:
            try:
                inserted_id = db.categories.insert_one(
                    {
                        "name": name,
                        "active": True,
                        "nameKey": key,
                        "normalizedName": key,
                        "code": code,
                        "projectId": GLOBAL_CATEGORIES_PROJECT_ID,
                    }
                ).inserted_id
                keeper = {
                    "_id": inserted_id,
                    "name": name,
                    "active": True,
                    "nameKey": key,
                    "normalizedName": key,
                    "code": code,
                    "projectId": GLOBAL_CATEGORIES_PROJECT_ID,
                }
                by_key[key] = keeper
                by_code[code] = keeper
                seeded += 1
            except DuplicateKeyError:
                keeper = db.categories.find_one(
                    {
                        "$or": [
                            {"nameKey": key},
                            {"normalizedName": key},
                            {"projectId": GLOBAL_CATEGORIES_PROJECT_ID, "code": code},
                            {"code": code},
                        ]
                    },
                    {"name": 1, "active": 1, "nameKey": 1, "normalizedName": 1, "code": 1, "projectId": 1},
                )
                if keeper is None:
                    logger.warning("Global categories bootstrap skipped problematic category name=%s code=%s", name, code)
                    continue
                by_key[key] = keeper
                existing_code = normalize_non_empty_string(keeper.get("code"))
                if existing_code:
                    by_code[existing_code] = keeper
            continue

        updates: dict = {}
        if keeper.get("active") is False:
            updates["active"] = True
            reactivated += 1
        if keeper.get("nameKey") != key:
            updates["nameKey"] = key
        if keeper.get("normalizedName") != key:
            updates["normalizedName"] = key
        if normalize_non_empty_string(keeper.get("code")) != code:
            updates["code"] = code
        if clean_global_category_name(keeper.get("name")) != name:
            updates["name"] = name

        if updates:
            try:
                db.categories.update_one({"_id": keeper.get("_id")}, {"$set": updates})
                keeper.update(updates)
                normalized_updates += 1
                updated_code = normalize_non_empty_string(keeper.get("code"))
                if updated_code:
                    by_code[updated_code] = keeper
            except DuplicateKeyError:
                logger.warning(
                    "Global categories bootstrap skipped update due to duplicate key for category_id=%s name=%s code=%s",
                    keeper.get("_id"),
                    name,
                    code,
                )
                continue

    return {
        "seeded": seeded,
        "reactivated": reactivated,
        "normalizedUpdates": normalized_updates,
    }


def normalize_non_empty_string(value) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.upper() in {"N/A", "NA"}:
        return None
    return normalized


def build_effective_category_fields(manual_code, manual_name, hint_code, hint_name) -> dict:
    normalized_manual_code = normalize_non_empty_string(manual_code)
    normalized_manual_name = normalize_non_empty_string(manual_name)
    normalized_hint_code = normalize_non_empty_string(hint_code)
    normalized_hint_name = normalize_non_empty_string(hint_name)
    return {
        "categoryEffectiveCode": normalized_manual_code or normalized_hint_code,
        "categoryEffectiveName": normalized_manual_name or normalized_hint_name,
    }


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


def resolve_transaction_tax_components(tx: dict) -> tuple[float | None, float | None, float | None]:
    tax = tx.get("tax") if isinstance(tx.get("tax"), dict) else {}
    sap = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
    source = str(tx.get("source") or "").strip().lower()

    # SBO: por seguridad no exponemos desglose fiscal hasta que el export
    # garantice misma moneda/escala para amount vs invoiceSubtotal/IVA/Total.
    # Si en el futuro existe una señal explícita, permitirlo con este flag.
    sbo_tax_breakdown_compatible = bool(sap.get("taxBreakdownSameCurrency"))
    if source == "sap-sbo" and not sbo_tax_breakdown_compatible:
        return None, None, None

    subtotal = parse_optional_decimal(tax.get("subtotal"))
    iva = parse_optional_decimal(tax.get("iva"))
    total_factura = parse_optional_decimal(tax.get("totalFactura"))

    if subtotal is None:
        subtotal = parse_optional_decimal(tx.get("subtotal"))
    if subtotal is None:
        subtotal = parse_optional_decimal(tx.get("montoSinIva"))
    if subtotal is None:
        subtotal = parse_optional_decimal(sap.get("invoiceSubtotal"))

    if iva is None:
        iva = parse_optional_decimal(tx.get("iva"))
    if iva is None:
        iva = parse_optional_decimal(tx.get("montoIva"))
    if iva is None:
        iva = parse_optional_decimal(sap.get("invoiceIva"))

    if total_factura is None:
        total_factura = parse_optional_decimal(tx.get("totalFactura"))
    if total_factura is None:
        total_factura = parse_optional_decimal(sap.get("invoiceTotal"))

    return subtotal, iva, total_factura


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
        normalized_type = str(type_value).strip().upper()
        if normalized_type == "EXPENSE":
            type_filter = {
                "$or": [
                    {"type": "EXPENSE"},
                    {
                        "$and": [
                            {"source": "sap-sbo"},
                            {"$or": [{"type": {"$exists": False}}, {"type": None}, {"type": ""}]},
                        ]
                    },
                ]
            }
            q.update(type_filter)
        else:
            q["type"] = normalized_type
    if category_id:
        if category_id == "__UNCATEGORIZED__":
            q["$or"] = [
                {"categoryEffectiveCode": None},
                {"categoryEffectiveCode": ""},
                {"categoryEffectiveCode": {"$exists": False}},
                {"categoryEffectiveName": None},
                {"categoryEffectiveName": ""},
                {"categoryEffectiveName": {"$exists": False}},
            ]
        else:
            q["$or"] = [
                {"resolvedCategory2Id": category_id},
                {"resolvedCategory2Name": category_id},
                {"categoryEffectiveCode": category_id},
                {"categoryEffectiveName": category_id},
                {"categoryManualCode": category_id},
                {"categoryHintCode": category_id},
                {"categoryHintName": category_id},
                {"category_id": category_id},
                {"categoryId": category_id},
            ]
    if vendor_id:
        q["vendor_id"] = vendor_id
    if supplier_id:
        supplier_filter = [{"supplierId": supplier_id}, {"supplier_id": supplier_id}, {"vendor_id": supplier_id}]

        stable_supplier_match = re.match(r"^sap-sbo:([^:]+):(.+)$", str(supplier_id).strip(), re.IGNORECASE)
        if stable_supplier_match:
            stable_project_id = stable_supplier_match.group(1).strip()
            stable_vendor_key = stable_supplier_match.group(2).strip()
            if stable_project_id and stable_vendor_key:
                escaped_vendor_key = re.escape(stable_vendor_key)
                supplier_filter.append(
                    {
                        "$and": [
                            {"projectId": stable_project_id},
                            {
                                "$or": [
                                    {"sap.cardCode": {"$regex": f"^{escaped_vendor_key}$", "$options": "i"}},
                                    {"supplierName": {"$regex": f"^{escaped_vendor_key}$", "$options": "i"}},
                                    {"sap.businessPartner": {"$regex": f"^{escaped_vendor_key}$", "$options": "i"}},
                                ]
                            },
                        ]
                    }
                )
        if ObjectId.is_valid(supplier_id):
            supplier_filter.append({"supplierId": oid(supplier_id)})

        vendor_lookup_filters = [{"_id": supplier_id}, {"id": supplier_id}]
        if ObjectId.is_valid(supplier_id):
            vendor_lookup_filters.append({"_id": oid(supplier_id)})

        vendor_doc = db.vendors.find_one(
            {"$or": vendor_lookup_filters},
            {"_id": 1, "id": 1, "source": 1, "projectId": 1, "supplierCardCode": 1, "cardCode": 1, "name": 1},
        )
        if vendor_doc:
            vendor_db_id = str(vendor_doc.get("_id") or "").strip()
            vendor_stable_id = str(vendor_doc.get("id") or "").strip()
            if vendor_db_id and vendor_db_id != supplier_id:
                supplier_filter.append({"vendor_id": vendor_db_id})
            if vendor_stable_id and vendor_stable_id != supplier_id:
                supplier_filter.append({"vendor_id": vendor_stable_id})

            card_code = str(vendor_doc.get("supplierCardCode") or vendor_doc.get("cardCode") or "").strip()
            vendor_name = str(vendor_doc.get("name") or "").strip()
            sap_conditions = []
            if card_code:
                sap_conditions.append({"sap.cardCode": {"$regex": f"^{re.escape(card_code)}$", "$options": "i"}})
            if vendor_name:
                escaped_name = re.escape(vendor_name)
                sap_conditions.extend(
                    [
                        {"supplierName": {"$regex": f"^{escaped_name}$", "$options": "i"}},
                        {"sap.businessPartner": {"$regex": f"^{escaped_name}$", "$options": "i"}},
                    ]
                )

            if sap_conditions:
                sap_query = {}
                vendor_project_id = str(vendor_doc.get("projectId") or "").strip()
                if vendor_project_id:
                    sap_query["projectId"] = vendor_project_id
                supplier_filter.append({"$and": [sap_query, {"$or": sap_conditions}]})
        else:
            escaped_supplier = re.escape(supplier_id)
            supplier_filter.extend(
                [
                    {"sap.cardCode": {"$regex": f"^{escaped_supplier}$", "$options": "i"}},
                    {"supplierName": {"$regex": f"^{escaped_supplier}$", "$options": "i"}},
                    {"sap.businessPartner": {"$regex": f"^{escaped_supplier}$", "$options": "i"}},
                ]
            )

        if "$or" in q:
            q["$and"] = [{"$or": q.pop("$or")}, {"$or": supplier_filter}]
        else:
            q["$or"] = supplier_filter
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

    def _build_trabajos_especiales_search_condition(raw_search: str) -> dict | None:
        escaped = re.escape(raw_search)
        matching_rules = list(
            db.supplierCategory2Rules.find(
                {
                    "isActive": {"$ne": False},
                    "category2Name": {"$regex": escaped, "$options": "i"},
                },
                {
                    "supplierKey": 1,
                    "supplierCardCode": 1,
                    "businessPartner": 1,
                    "supplierName": 1,
                },
            )
        )
        if not matching_rules:
            return None

        supplier_conditions: list[dict] = []
        seen_supplier_conditions: set[str] = set()

        def register_supplier_condition(condition: dict):
            signature = str(condition)
            if signature in seen_supplier_conditions:
                return
            seen_supplier_conditions.add(signature)
            supplier_conditions.append(condition)

        for rule in matching_rules:
            raw_supplier_key = normalize_non_empty_string(rule.get("supplierKey"))
            if raw_supplier_key.startswith("cardcode:"):
                supplier_key_card_code = raw_supplier_key.split(":", 1)[1].strip()
                if supplier_key_card_code:
                    register_supplier_condition(
                        {"sap.cardCode": {"$regex": f"^{re.escape(supplier_key_card_code)}$", "$options": "i"}}
                    )

            supplier_card_code = normalize_non_empty_string(rule.get("supplierCardCode"))
            business_partner = normalize_non_empty_string(rule.get("businessPartner"))
            supplier_name = normalize_non_empty_string(rule.get("supplierName"))

            if supplier_card_code:
                escaped_card_code = re.escape(supplier_card_code)
                register_supplier_condition(
                    {"sap.cardCode": {"$regex": f"^{escaped_card_code}$", "$options": "i"}}
                )
                register_supplier_condition(
                    {"supplierCardCode": {"$regex": f"^{escaped_card_code}$", "$options": "i"}}
                )

            if business_partner:
                escaped_business_partner = re.escape(business_partner)
                register_supplier_condition(
                    {"sap.businessPartner": {"$regex": f"^{escaped_business_partner}$", "$options": "i"}}
                )
                register_supplier_condition(
                    {"businessPartner": {"$regex": f"^{escaped_business_partner}$", "$options": "i"}}
                )

            if supplier_name:
                escaped_supplier_name = re.escape(supplier_name)
                register_supplier_condition(
                    {"supplierName": {"$regex": f"^{escaped_supplier_name}$", "$options": "i"}}
                )
                register_supplier_condition(
                    {"proveedorNombre": {"$regex": f"^{escaped_supplier_name}$", "$options": "i"}}
                )

        if not supplier_conditions:
            return None

        trabajos_prefix_regex = f"^{re.escape(TRABAJOS_ESPECIALES_PREFIX)}"
        trabajos_especiales_condition = {
            "$or": [
                {"categoryEffectiveName": {"$regex": trabajos_prefix_regex, "$options": "i"}},
                {"categoryManualName": {"$regex": trabajos_prefix_regex, "$options": "i"}},
                {"categoryHintName": {"$regex": trabajos_prefix_regex, "$options": "i"}},
            ]
        }

        return {
            "$and": [
                trabajos_especiales_condition,
                {"$or": supplier_conditions},
            ]
        }

    if cleaned_search:
        escaped_search = re.escape(cleaned_search)
        search_conditions = [
            {"description": {"$regex": escaped_search, "$options": "i"}},
            {"concept": {"$regex": escaped_search, "$options": "i"}},
            {"supplierName": {"$regex": escaped_search, "$options": "i"}},
            {"proveedorNombre": {"$regex": escaped_search, "$options": "i"}},
            {"beneficiario": {"$regex": escaped_search, "$options": "i"}},
            {"businessPartner": {"$regex": escaped_search, "$options": "i"}},
            {"supplierCardCode": {"$regex": escaped_search, "$options": "i"}},
            {"proveedor.name": {"$regex": escaped_search, "$options": "i"}},
            {"sap.businessPartner": {"$regex": escaped_search, "$options": "i"}},
            {"sap.cardCode": {"$regex": escaped_search, "$options": "i"}},
            {"resolvedCategory2Name": {"$regex": escaped_search, "$options": "i"}},
            {"resolvedCategory2Id": {"$regex": escaped_search, "$options": "i"}},
            {"categoryHintName": {"$regex": escaped_search, "$options": "i"}},
            {"categoryHintCode": {"$regex": escaped_search, "$options": "i"}},
        ]

        trabajos_especiales_search_condition = _build_trabajos_especiales_search_condition(cleaned_search)
        if trabajos_especiales_search_condition:
            search_conditions.append(trabajos_especiales_search_condition)

        normalized_search = normalize_category_name(cleaned_search)
        category_name_filters = [
            {"name": {"$regex": escaped_search, "$options": "i"}},
        ]
        if normalized_search:
            category_name_filters.append({"normalizedName": {"$regex": re.escape(normalized_search), "$options": "i"}})

        matching_categories = list(db.categories.find({"$or": category_name_filters}, {"_id": 1, "normalizedName": 1}))
        matching_category_ids = [str(category["_id"]) for category in matching_categories]
        category_search_conditions = []
        if matching_category_ids:
            category_search_conditions = [
                {"category_id": {"$in": matching_category_ids}},
                {
                    "$and": [
                        {"categoryId": {"$in": matching_category_ids}},
                        {
                            "$or": [
                                {"category_id": None},
                                {"category_id": ""},
                                {"category_id": {"$exists": False}},
                            ]
                        },
                    ]
                },
            ]

            search_conditions.extend(category_search_conditions)
        if "$or" in q:
            q["$and"] = [{"$or": q.pop("$or")}, {"$or": search_conditions}]
        else:
            q["$or"] = search_conditions
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
                "typeNormalized": {
                    "$cond": [
                        {
                            "$and": [
                                {"$eq": ["$source", "sap-sbo"]},
                                {
                                    "$or": [
                                        {"$eq": ["$type", None]},
                                        {"$eq": ["$type", ""]},
                                    ]
                                },
                            ]
                        },
                        "EXPENSE",
                        "$type",
                    ]
                },
                "amount": {"$ifNull": ["$amount", 0]},
                "montoIva": {
                    "$let": {
                        "vars": {
                            "isSapSbo": {"$eq": ["$source", "sap-sbo"]},
                            "sboTaxBreakdownSameCurrency": {"$ifNull": ["$sap.taxBreakdownSameCurrency", False]},
                            "iva": {
                                "$convert": {
                                    "input": {"$ifNull": ["$tax.iva", {"$ifNull": ["$iva", {"$ifNull": ["$montoIva", "$sap.invoiceIva"]}]}]},
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            "totalFactura": {
                                "$convert": {
                                    "input": {"$ifNull": ["$tax.totalFactura", {"$ifNull": ["$totalFactura", "$sap.invoiceTotal"]}]},
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            "amountValue": {"$ifNull": ["$amount", 0]},
                        },
                        "in": {
                            "$cond": [
                                {
                                    "$or": [
                                        {
                                            "$and": [
                                                "$$isSapSbo",
                                                {"$ne": ["$$sboTaxBreakdownSameCurrency", True]},
                                            ]
                                        },
                                        {"$eq": ["$$iva", None]},
                                        {"$eq": ["$$totalFactura", None]},
                                        {"$eq": ["$$totalFactura", 0]},
                                    ]
                                },
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
                            "isSapSbo": {"$eq": ["$source", "sap-sbo"]},
                            "sboTaxBreakdownSameCurrency": {"$ifNull": ["$sap.taxBreakdownSameCurrency", False]},
                            "subtotal": {
                                "$convert": {
                                    "input": {"$ifNull": ["$tax.subtotal", {"$ifNull": ["$subtotal", {"$ifNull": ["$montoSinIva", "$sap.invoiceSubtotal"]}]}]},
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            "totalFactura": {
                                "$convert": {
                                    "input": {"$ifNull": ["$tax.totalFactura", {"$ifNull": ["$totalFactura", "$sap.invoiceTotal"]}]},
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            "amountValue": {"$ifNull": ["$amount", 0]},
                        },
                        "in": {
                            "$cond": [
                                {
                                    "$or": [
                                        {
                                            "$and": [
                                                "$$isSapSbo",
                                                {"$ne": ["$$sboTaxBreakdownSameCurrency", True]},
                                            ]
                                        },
                                        {"$eq": ["$$subtotal", None]},
                                        {"$eq": ["$$totalFactura", None]},
                                        {"$eq": ["$$totalFactura", 0]},
                                    ]
                                },
                                {
                                    "$round": [
                                        {
                                            "$subtract": [
                                                "$$amountValue",
                                                {
                                                    "$let": {
                                                        "vars": {
                                                            "iva": {
                                                                "$convert": {
                                                                    "input": {"$ifNull": ["$tax.iva", {"$ifNull": ["$iva", {"$ifNull": ["$montoIva", "$sap.invoiceIva"]}]}]},
                                                                    "to": "double",
                                                                    "onError": None,
                                                                    "onNull": None,
                                                                }
                                                            },
                                                            "totalFacturaIva": {
                                                                "$convert": {
                                                                    "input": {"$ifNull": ["$tax.totalFactura", {"$ifNull": ["$totalFactura", "$sap.invoiceTotal"]}]},
                                                                    "to": "double",
                                                                    "onError": None,
                                                                    "onNull": None,
                                                                }
                                                            },
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
                    "$sum": {"$cond": [{"$eq": ["$typeNormalized", "EXPENSE"]}, "$amount", 0]}
                },
                "expensesTax": {
                    "$sum": {"$cond": [{"$eq": ["$typeNormalized", "EXPENSE"]}, "$montoIva", 0]}
                },
                "expensesWithoutTax": {
                    "$sum": {"$cond": [{"$eq": ["$typeNormalized", "EXPENSE"]}, "$montoSinIva", 0]}
                },
                "incomeGross": {
                    "$sum": {"$cond": [{"$eq": ["$typeNormalized", "INCOME"]}, "$amount", 0]}
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
    optional_category_hint_headers = ["categoryhintcode", "categoryhintname", "categoryhintproject"]
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
        # Explicit aliases requested for SAP exports with variant spacing/casing.
        # normalize_header removes spaces/underscores/BOM before this lookup.
        "subtotal": "subtotal",  # Subtotal | Sub total | SubTotal
        "iva": "iva",  # IVA | Iva
        "totalfactura": "totalfactura",  # TotalFactura | Total Factura | Total_Factura
        "categoryhintcode": "categoryhintcode",
        "categoryhintname": "categoryhintname",
        "categoryhintproject": "categoryhintproject",
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
            if canonical in (
                canonical_headers
                + optional_tax_headers
                + optional_movement_headers
                + optional_category_hint_headers
            ) and canonical not in header_index:
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
            for hint_key in optional_category_hint_headers:
                source_idx = header_index.get(hint_key)
                row_dict[hint_key] = row_values[source_idx] if source_idx is not None and source_idx < len(row_values) else None

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
            for hint_key in optional_category_hint_headers:
                source_idx = header_index.get(hint_key)
                row_dict[hint_key] = values[source_idx] if source_idx is not None and source_idx < len(values) else None
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


@app.get("/api/version")
def api_version():
    commit_sha = (
        env_get("RENDER_GIT_COMMIT")
        or env_get("GIT_COMMIT")
        or env_get("COMMIT_SHA")
        or env_get("VERCEL_GIT_COMMIT_SHA")
        or env_get("RAILWAY_GIT_COMMIT_SHA")
        or env_get("SOURCE_VERSION")
    )

    route_paths = {route.path for route in app.routes}
    cleanup_route = "/api/admin/sap/cleanup-iva-duplicates"

    return {
        "commitSha": commit_sha,
        "routes": {
            "cleanupEndpointRegistered": cleanup_route in route_paths,
            "key": [
                "/health",
                "/api/version",
                "/api/transactions",
                cleanup_route,
            ],
        },
    }


@app.head("/health")
def health_head():
    return Response(status_code=200)


# ---------- auth ----------
class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    token: str | None = None
    role: str | None = None
    username: str | None = None
    displayName: str | None = None


@app.post("/auth/login", response_model=LoginResponse)
@app.post("/api/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest):
    username = payload.username.strip()
    password = payload.password
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password are required")

    env_user = get_env_auth_users().get(username)
    if env_user and password == env_user.get("password"):
        role = normalize_user_role(env_user.get("role", "VIEWER"))
        display_name = env_user.get("displayName") or username
    else:
        user = db.users.find_one({"username": username})
        if not user or not pwd_context.verify(password, user.get("password_hash", "")):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if not is_user_active(user):
            raise HTTPException(status_code=403, detail="User is inactive")
        role = resolve_effective_user_role(user, fallback_role="SUPERADMIN")
        env_display_name = (get_env_auth_users().get(username) or {}).get("displayName")
        display_name = user.get("displayName") or env_display_name or username
        display_name = user.get("displayName") or username

    token = create_token(username, role, display_name)
    return {
        "access_token": token,
        "token_type": "bearer",
        "token": token,
        "role": role,
        "username": username,
        "displayName": display_name,
    }


@app.get("/auth/me")
@app.get("/api/me")
def me(user=Depends(require_authenticated)):
    return {
        "id": user.get("id"),
        "name": user.get("name") or user.get("displayName") or user.get("username"),
        "email": user.get("email") or "",
        "role": normalize_user_role(user.get("role")),
        "isActive": bool(user.get("isActive", user.get("active", True))),
        "username": user.get("username"),
        "displayName": user.get("displayName") or user.get("name") or user.get("username"),
        "allowedProjectIds": normalize_allowed_project_ids(user.get("allowedProjectIds")),
        "uiPrefs": normalize_ui_prefs(user.get("uiPrefs")),
    }


@app.patch("/api/me/preferences")
def update_my_preferences(payload: dict, user: dict = Depends(require_authenticated)):
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="payload must be an object")

    source_ui_prefs = payload.get("uiPrefs") if isinstance(payload.get("uiPrefs"), dict) else payload
    current_ui_prefs = normalize_ui_prefs(user.get("uiPrefs"))

    should_update_hidden_projects = isinstance(source_ui_prefs, dict) and ("hiddenProjectIds" in source_ui_prefs)
    raw_hidden_project_ids = source_ui_prefs.get("hiddenProjectIds") if should_update_hidden_projects else current_ui_prefs.get("hiddenProjectIds")

    should_update_default_project = isinstance(source_ui_prefs, dict) and ("defaultProjectId" in source_ui_prefs)
    raw_default_project_id = source_ui_prefs.get("defaultProjectId") if should_update_default_project else current_ui_prefs.get("defaultProjectId")

    normalized_hidden_project_ids = normalize_hidden_project_ids(raw_hidden_project_ids)
    if normalized_hidden_project_ids:
        candidate_object_ids = [ObjectId(project_id) for project_id in normalized_hidden_project_ids]
        rows = db.projects.find(
            {"_id": {"$in": candidate_object_ids}},
            {"_id": 1},
        )
        existing_ids = {str(row.get("_id")) for row in rows}
        normalized_hidden_project_ids = [project_id for project_id in normalized_hidden_project_ids if project_id in existing_ids]

    normalized_default_project_id = str(raw_default_project_id or "").strip()
    if normalized_default_project_id:
        if not ObjectId.is_valid(normalized_default_project_id):
            raise HTTPException(status_code=400, detail="defaultProjectId must be a valid project id")

        project_exists = db.projects.find_one(
            {"_id": ObjectId(normalized_default_project_id)},
            {"_id": 1},
        )
        if not project_exists:
            raise HTTPException(status_code=400, detail="defaultProjectId does not exist")
        if not can_access_project(user, normalized_default_project_id):
            raise HTTPException(status_code=403, detail="defaultProjectId is not allowed for this user")

    user_id = str(user.get("id") or "").strip()
    if not user_id or not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=404, detail="User not found")

    ui_prefs = {
        "hiddenProjectIds": normalized_hidden_project_ids,
        "defaultProjectId": normalized_default_project_id,
    }
    updated = db.users.find_one_and_update(
        {"_id": ObjectId(user_id)},
        {
            "$set": {
                "uiPrefs": ui_prefs,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")

    return {"ok": True, "uiPrefs": normalize_ui_prefs(updated.get("uiPrefs"))}


@app.post("/users")
def create_user(payload: dict, _: dict = Depends(require_admin)):
    username = (payload.get("username") or "").strip()
    password = payload.get("password") or ""
    role_raw = (payload.get("role") or "").strip().upper()
    role = normalize_user_role(role_raw)
    active = bool(payload.get("active", payload.get("isActive", True)))
    display_name = (payload.get("displayName") or payload.get("name") or "").strip()
    email = str(payload.get("email") or "").strip()
    allowed_project_ids = normalize_allowed_project_ids(payload.get("allowedProjectIds")) if role == "VIEWER" else []

    if len(username) < 3:
        raise HTTPException(status_code=400, detail="username must have at least 3 characters")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="password must have at least 6 characters")
    if role_raw not in USER_ROLES:
        raise HTTPException(status_code=400, detail="role must be SUPERADMIN, ADMIN or VIEWER")
    if db.users.find_one({"username": username}):
        raise HTTPException(status_code=409, detail="User already exists")

    doc = {
        "username": username,
        "password_hash": pwd_context.hash(password),
        "role": role,
        "roleVersion": ROLE_SCHEMA_VERSION,
        "displayName": display_name or username,
        "email": email,
        "active": active,
        "isActive": active,
        "allowedProjectIds": allowed_project_ids,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _id = db.users.insert_one(doc).inserted_id
    return serialize_user(db.users.find_one({"_id": _id}))


@app.post("/api/admin/users")
def create_admin_user(payload: dict, _: dict = Depends(require_admin)):
    return create_user(payload, _)


@app.get("/users")
def list_users(_: dict = Depends(require_admin)):
    users = db.users.find({}, {"password_hash": 0}).sort("created_at", -1)
    return [serialize(u) for u in users]


@app.get("/api/admin/users")
def list_admin_users(_: dict = Depends(require_admin)):
    users = db.users.find({}, {"password_hash": 0}).sort("created_at", -1)
    return [serialize_admin_user(u) for u in users]


@app.patch("/api/admin/users/{user_id}")
def update_admin_user(user_id: str, payload: dict, _: dict = Depends(require_admin)):
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="payload must be an object")

    existing = db.users.find_one({"_id": oid(user_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="User not found")

    update_fields: dict = {}

    if "allowedProjectIds" in payload:
        update_fields["allowedProjectIds"] = normalize_allowed_project_ids(payload.get("allowedProjectIds"))

    if "role" in payload:
        next_role = normalize_user_role(payload.get("role"))
        if next_role not in USER_ROLES:
            raise HTTPException(status_code=400, detail="role must be SUPERADMIN, ADMIN or VIEWER")

        current_role = resolve_effective_user_role(existing, fallback_role=existing.get("role"))
        if current_role == "SUPERADMIN" and next_role != "SUPERADMIN":
            remaining_superadmins = count_superadmins(exclude_user_id=user_id)
            if remaining_superadmins < 1:
                raise HTTPException(status_code=400, detail="No se puede degradar al último SUPERADMIN")

        update_fields["role"] = next_role
        update_fields["roleVersion"] = ROLE_SCHEMA_VERSION

    if "displayName" in payload or "name" in payload:
        display_name = str(payload.get("displayName") or payload.get("name") or "").strip()
        if not display_name:
            raise HTTPException(status_code=400, detail="displayName cannot be empty")
        update_fields["displayName"] = display_name

    if not update_fields:
        raise HTTPException(status_code=400, detail="No editable fields in payload")

    update_fields["updated_at"] = datetime.now(timezone.utc).isoformat()

    updated = db.users.find_one_and_update(
        {"_id": oid(user_id)},
        {"$set": update_fields},
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")

    return serialize_admin_user(updated)


@app.get("/api/admin/raw-data/collections")
def raw_data_collections(_: dict = Depends(require_admin)):
    excluded = {"system.indexes", "system.profile"}
    names = [name for name in db.list_collection_names() if name not in excluded and not name.startswith("system.")]
    return {"collections": sorted(names)}


@app.get("/api/admin/raw-data/{collection}")
def raw_data_rows(collection: str, limit: int = 200, _: dict = Depends(require_admin)):
    if collection.startswith("system."):
        raise HTTPException(status_code=400, detail="Collection not allowed")

    safe_limit = max(1, min(limit, 500))
    cursor = db[collection].find({}).limit(safe_limit)
    docs = [serialize_raw_doc(doc) for doc in cursor]

    field_names = set()
    for doc in docs:
        field_names.update(doc.keys())

    sorted_fields = sorted(field_names, key=lambda field: (field == "id", field))
    return {
        "collection": collection,
        "count": len(docs),
        "fields": sorted_fields,
        "rows": docs,
    }


@app.patch("/api/admin/raw-data/{collection}/{row_id}")
def raw_data_update_row(collection: str, row_id: str, payload: dict, _: dict = Depends(require_admin)):
    if collection.startswith("system."):
        raise HTTPException(status_code=400, detail="Collection not allowed")

    changes = payload.get("changes")
    if not isinstance(changes, dict) or not changes:
        raise HTTPException(status_code=400, detail="changes is required")

    if "id" in changes or "_id" in changes:
        raise HTTPException(status_code=400, detail="id cannot be updated")

    existing = db[collection].find_one({"_id": oid(row_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="Row not found")

    normalized = {key: normalize_raw_value(value) for key, value in changes.items()}
    db[collection].update_one({"_id": oid(row_id)}, {"$set": normalized})
    updated = db[collection].find_one({"_id": oid(row_id)})
    return serialize_raw_doc(updated)


def build_suspicious_project_resolutions_query(
    source_sbo: str | None = None,
    supplier: str | None = None,
    document_project: str | None = None,
    payment_project: str | None = None,
    status: str | None = None,
    text: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:

    query: dict = {
        "sap.isProjectResolutionSuspicious": True,
    }

    manual_resolved_id_len_expr = {
        "$strLenCP": {
            "$trim": {
                "input": {
                    "$toString": {
                        "$ifNull": ["$sap.manualResolvedProjectId", ""]
                    }
                }
            }
        }
    }

    normalized_status = (status or "pending").strip().lower()
    if normalized_status == "pending":
        query["$expr"] = {"$eq": [manual_resolved_id_len_expr, 0]}
    elif normalized_status == "resolved":
        query["$expr"] = {"$gt": [manual_resolved_id_len_expr, 0]}

    normalized_source_sbo = normalize_non_empty_string(source_sbo)
    if normalized_source_sbo:
        query["sourceSbo"] = normalized_source_sbo

    normalized_supplier = normalize_non_empty_string(supplier)
    if normalized_supplier:
        query["supplierName"] = {"$regex": re.escape(normalized_supplier), "$options": "i"}

    normalized_document_project = normalize_non_empty_string(document_project)
    if normalized_document_project:
        query["sap.documentProjectName"] = {"$regex": re.escape(normalized_document_project), "$options": "i"}

    normalized_payment_project = normalize_non_empty_string(payment_project)
    if normalized_payment_project:
        query["sap.paymentProjectName"] = {"$regex": re.escape(normalized_payment_project), "$options": "i"}

    parsed_from = parse_excel_date(date_from) if date_from else None
    parsed_to = parse_excel_date(date_to) if date_to else None
    if parsed_from or parsed_to:
        date_query = {}
        if parsed_from:
            date_query["$gte"] = parsed_from
        if parsed_to:
            date_query["$lte"] = parsed_to
        query["date"] = date_query

    normalized_text = normalize_non_empty_string(text)
    if normalized_text:
        escaped = re.escape(normalized_text)
        query["$or"] = [
            {"sap.paymentNum": {"$regex": escaped, "$options": "i"}},
            {"sap.invoiceNum": {"$regex": escaped, "$options": "i"}},
            {"supplierName": {"$regex": escaped, "$options": "i"}},
            {"sap.businessPartner": {"$regex": escaped, "$options": "i"}},
        ]

    return query


def normalize_suspicious_resolution_payload(payload: dict | None) -> dict:
    incoming = payload if isinstance(payload, dict) else {}
    aliases = {
        "projectId": "project_id",
        "projectCode": "project_code",
        "projectName": "project_name",
        "resolutionReason": "resolution_reason",
        "resolveTo": "resolve_to",
        "manualResolvedProjectId": "manual_resolved_project_id",
        "manualResolvedProjectCode": "manual_resolved_project_code",
        "manualResolvedProjectName": "manual_resolved_project_name",
    }
    normalized = dict(incoming)
    for camel_key, snake_key in aliases.items():
        if normalized.get(snake_key) in (None, "") and normalized.get(camel_key) not in (None, ""):
            normalized[snake_key] = normalized.get(camel_key)
    if normalized.get("resolve_to") in (None, "") and normalized.get("resolution") not in (None, ""):
        normalized["resolve_to"] = normalized.get("resolution")
    if normalized.get("resolution_reason") in (None, ""):
        for fallback_key in ("reason", "note"):
            if normalized.get(fallback_key) not in (None, ""):
                normalized["resolution_reason"] = normalized.get(fallback_key)
                break
    return normalized


def resolve_project_metadata_from_id(project_id: str | None) -> tuple[str | None, str | None, str | None]:
    normalized_id = normalize_non_empty_string(project_id)
    if not normalized_id or not ObjectId.is_valid(normalized_id):
        return None, None, None

    project_doc = db.projects.find_one({"_id": ObjectId(normalized_id)}) or {}
    if not project_doc:
        return None, None, None

    sap_doc = project_doc.get("sap") if isinstance(project_doc.get("sap"), dict) else {}
    project_code = normalize_non_empty_string(
        project_doc.get("code") or project_doc.get("projectCode") or sap_doc.get("projectCode") or sap_doc.get("sapName")
    )
    project_name = normalize_non_empty_string(project_doc.get("name") or sap_doc.get("projectName") or project_doc.get("projectName"))

    return normalized_id, project_code, project_name


def resolve_project_metadata_from_code_or_name(
    project_code: str | None = None,
    project_name: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    normalized_code = normalize_non_empty_string(project_code)
    normalized_name = normalize_non_empty_string(project_name)
    if not normalized_code and not normalized_name:
        return None, None, None

    normalized_code_folded = normalized_code.casefold() if normalized_code else None
    normalized_name_folded = normalized_name.casefold() if normalized_name else None
    matches: dict[str, tuple[str, str | None, str | None]] = {}

    projection = {
        "_id": 1,
        "name": 1,
        "projectName": 1,
        "code": 1,
        "projectCode": 1,
        "sap.projectCode": 1,
        "sap.sapName": 1,
        "sap.projectName": 1,
        "sap.projectNames": 1,
    }
    for project_doc in db.projects.find({}, projection):
        sap_doc = project_doc.get("sap") if isinstance(project_doc.get("sap"), dict) else {}
        candidate_id = normalize_non_empty_string(project_doc.get("_id"))
        candidate_code = normalize_non_empty_string(
            project_doc.get("code") or project_doc.get("projectCode") or sap_doc.get("projectCode") or sap_doc.get("sapName")
        )
        candidate_name = normalize_non_empty_string(project_doc.get("name") or sap_doc.get("projectName") or project_doc.get("projectName"))
        candidate_name_aliases = [candidate_name]
        sap_project_names = sap_doc.get("projectNames") if isinstance(sap_doc.get("projectNames"), list) else []
        candidate_name_aliases.extend(normalize_non_empty_string(name) for name in sap_project_names)

        code_matches = (not normalized_code_folded) or (candidate_code and candidate_code.casefold() == normalized_code_folded)
        name_matches = (not normalized_name_folded) or any(
            alias and alias.casefold() == normalized_name_folded for alias in candidate_name_aliases
        )
        if code_matches and name_matches and candidate_id:
            matches[candidate_id] = (candidate_id, candidate_code, candidate_name)

    if len(matches) != 1:
        return None, None, None

    return next(iter(matches.values()))


def serialize_suspicious_project_resolution_row(tx: dict) -> dict:
    tx_doc = serialize_transaction_with_supplier(tx)
    sap_doc = tx_doc.get("sap") if isinstance(tx_doc.get("sap"), dict) else {}
    mongo_id = normalize_non_empty_string(tx_doc.get("id") or tx.get("_id"))
    return {
        "id": mongo_id,
        "transactionId": mongo_id,
        "_id": mongo_id,
        "date": tx_doc.get("date"),
        "sourceSbo": tx_doc.get("sourceSbo") or sap_doc.get("sourceSbo"),
        "sourceDb": tx_doc.get("sourceDb") or sap_doc.get("sourceDb"),
        "supplier": tx_doc.get("supplierName") or sap_doc.get("businessPartner"),
        "paymentNum": sap_doc.get("paymentNum"),
        "invoiceNum": sap_doc.get("invoiceNum"),
        "amount": tx_doc.get("amount"),
        "invoiceTotal": sap_doc.get("invoiceTotal"),
        "documentProjectCode": sap_doc.get("documentProjectCode"),
        "documentProjectName": sap_doc.get("documentProjectName"),
        "paymentProjectCode": sap_doc.get("paymentProjectCode"),
        "paymentProjectName": sap_doc.get("paymentProjectName"),
        "projectResolutionSource": sap_doc.get("projectResolutionSource"),
        "suspicionReasons": sap_doc.get("projectResolutionSuspicionReasons") or [],
        "isProjectResolutionSuspicious": bool(sap_doc.get("isProjectResolutionSuspicious")),
        "currentAssignedProjectId": tx_doc.get("effectiveProjectId"),
        "currentAssignedProjectCode": tx_doc.get("effectiveProjectCode"),
        "currentAssignedProjectName": tx_doc.get("effectiveProjectName"),
        "status": "resolved" if normalize_non_empty_string(sap_doc.get("manualResolvedProjectId")) else "pending",
        "manualResolvedProjectId": sap_doc.get("manualResolvedProjectId"),
        "manualResolvedProjectCode": sap_doc.get("manualResolvedProjectCode"),
        "manualResolvedProjectName": sap_doc.get("manualResolvedProjectName"),
        "manualResolvedBy": sap_doc.get("manualResolvedBy"),
        "manualResolvedAt": sap_doc.get("manualResolvedAt"),
        "manualResolutionReason": sap_doc.get("manualResolutionReason"),
        "sap": sap_doc,
    }


def get_suspicious_project_resolutions_collection():
    db_name = normalize_non_empty_string(SUSPICIOUS_PROJECT_RESOLUTIONS_DB_NAME) or DB_NAME
    collection_name = normalize_non_empty_string(SUSPICIOUS_PROJECT_RESOLUTIONS_COLLECTION_NAME) or "transactions"

    current_db_name = normalize_non_empty_string(getattr(db, "name", None))
    if hasattr(db, collection_name) and (not current_db_name or current_db_name == db_name):
        return getattr(db, collection_name), current_db_name or db_name, collection_name

    return client[db_name][collection_name], db_name, collection_name


@app.get("/api/admin/suspicious-project-resolutions")
def list_admin_suspicious_project_resolutions(
    sourceSbo: str | None = None,
    supplier: str | None = None,
    documentProject: str | None = None,
    paymentProject: str | None = None,
    status: str | None = "pending",
    q: str | None = None,
    dateFrom: str | None = None,
    dateTo: str | None = None,
    page: int = 1,
    limit: int = 50,
    _: dict = Depends(require_admin),
):
    normalized_limit = min(max(limit, 1), 500)
    normalized_page = max(page, 1)
    skip = (normalized_page - 1) * normalized_limit

    query = build_suspicious_project_resolutions_query(
        source_sbo=sourceSbo,
        supplier=supplier,
        document_project=documentProject,
        payment_project=paymentProject,
        status=status,
        text=q,
        date_from=dateFrom,
        date_to=dateTo,
    )

    suspicious_collection, _, _ = get_suspicious_project_resolutions_collection()
    total_count = suspicious_collection.count_documents(query)
    rows = list(suspicious_collection.find(query).sort([("date", -1), ("_id", -1)]).skip(skip).limit(normalized_limit))
    return {
        "items": [serialize_suspicious_project_resolution_row(row) for row in rows],
        "page": normalized_page,
        "limit": normalized_limit,
        "totalCount": total_count,
    }


@app.get("/api/admin/suspicious-project-resolutions/{transaction_id}")
def get_admin_suspicious_project_resolution_detail(transaction_id: str, _: dict = Depends(require_admin)):
    suspicious_collection, _, _ = get_suspicious_project_resolutions_collection()
    tx = suspicious_collection.find_one({"_id": oid(transaction_id), "source": "sap-sbo"})
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return serialize_suspicious_project_resolution_row(tx)


@app.post("/api/admin/suspicious-project-resolutions/{transaction_id}/resolve")
def resolve_admin_suspicious_project_resolution(
    transaction_id: str,
    payload: dict,
    request: FastAPIRequest = None,
    user: dict = Depends(require_admin),
):
    raw_transaction_id = str(transaction_id or "")
    normalized_transaction_id = raw_transaction_id.strip()
    logger.info("resolve suspicious-project incoming transactionId=%s normalized=%s", raw_transaction_id, normalized_transaction_id)
    if not normalized_transaction_id or not ObjectId.is_valid(normalized_transaction_id):
        raise HTTPException(status_code=400, detail="transactionId is required and must be a valid ObjectId")

    normalized_payload = normalize_suspicious_resolution_payload(payload)
    logger.info(
        "resolve suspicious-project transactionId=%s payload=%s",
        normalized_transaction_id,
        _serialize_any(normalized_payload),
    )

    resolve_to = str(normalized_payload.get("resolve_to") or "").strip().lower()
    reason = normalize_non_empty_string(normalized_payload.get("resolution_reason"))

    oid_transaction_id = ObjectId(normalized_transaction_id)
    suspicious_collection, suspicious_db_name, suspicious_collection_name = get_suspicious_project_resolutions_collection()
    request_project_id = ""
    if request is not None and getattr(request, "headers", None) is not None:
        request_project_id = (request.headers.get("x-project-id") or "").strip()

    tx_lookup_filter = {"_id": oid_transaction_id}
    logger.info(
        "resolve suspicious-project lookup transactionId=%s x-project-id=%s filter=%s",
        normalized_transaction_id,
        request_project_id or None,
        _serialize_any(tx_lookup_filter),
    )

    tx = suspicious_collection.find_one(tx_lookup_filter)
    logger.info(
        "resolve suspicious-project lookup result transactionId=%s objectId=%s db=%s collection=%s found=%s",
        normalized_transaction_id,
        str(oid_transaction_id),
        suspicious_db_name,
        suspicious_collection_name,
        bool(tx),
    )
    if not tx:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "Transaction not found",
                "transactionId": normalized_transaction_id,
                "db": suspicious_db_name,
                "collection": suspicious_collection_name,
                "objectIdParsed": str(oid_transaction_id),
            },
        )

    sap_doc = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
    document_code = normalize_non_empty_string(sap_doc.get("documentProjectCode"))
    document_name = normalize_non_empty_string(sap_doc.get("documentProjectName"))
    payment_code = normalize_non_empty_string(sap_doc.get("paymentProjectCode"))
    payment_name = normalize_non_empty_string(sap_doc.get("paymentProjectName"))

    selected_project_id = normalize_non_empty_string(
        normalized_payload.get("manual_resolved_project_id") or normalized_payload.get("project_id")
    )
    selected_project_code = normalize_non_empty_string(
        normalized_payload.get("manual_resolved_project_code") or normalized_payload.get("project_code")
    )
    selected_project_name = normalize_non_empty_string(
        normalized_payload.get("manual_resolved_project_name") or normalized_payload.get("project_name")
    )

    if resolve_to == "document":
        selected_project_id, selected_project_code, selected_project_name = resolve_project_metadata_from_code_or_name(
            project_code=document_code,
            project_name=document_name,
        )
    elif resolve_to == "payment":
        selected_project_id, selected_project_code, selected_project_name = resolve_project_metadata_from_code_or_name(
            project_code=payment_code,
            project_name=payment_name,
        )
    elif resolve_to in {"custom", "other", "manual", ""}:
        if selected_project_id:
            selected_project_id, selected_project_code, selected_project_name = resolve_project_metadata_from_id(selected_project_id)
        elif selected_project_code or selected_project_name:
            selected_project_id, selected_project_code, selected_project_name = resolve_project_metadata_from_code_or_name(
                project_code=selected_project_code,
                project_name=selected_project_name,
            )
    else:
        raise HTTPException(status_code=400, detail="resolve_to must be document, payment or custom")

    if not selected_project_id:
        raise HTTPException(
            status_code=400,
            detail="Could not derive resolved project id. Provide a valid project_id or a unique project_code/project_name mapping.",
        )

    now_iso = datetime.now(timezone.utc).isoformat()
    resolved_by = normalize_non_empty_string(user.get("displayName") or user.get("username") or user.get("id"))

    suspicious_collection.update_one(
        {"_id": oid_transaction_id},
        {
            "$set": {
                "sap.manualResolvedProjectId": selected_project_id,
                "sap.manualResolvedProjectCode": selected_project_code,
                "sap.manualResolvedProjectName": selected_project_name,
                "sap.manualResolvedBy": resolved_by,
                "sap.manualResolvedAt": now_iso,
                "sap.manualResolutionReason": reason,
                "updated_at": now_iso,
            }
        },
    )

    updated = suspicious_collection.find_one({"_id": oid_transaction_id})
    updated_sap = updated.get("sap") if isinstance(updated.get("sap"), dict) else {}
    persisted_project_id = normalize_non_empty_string(updated_sap.get("manualResolvedProjectId"))
    persisted_project_code = normalize_non_empty_string(updated_sap.get("manualResolvedProjectCode"))
    persisted_project_name = normalize_non_empty_string(updated_sap.get("manualResolvedProjectName"))
    if not (persisted_project_id and persisted_project_code and persisted_project_name):
        raise HTTPException(status_code=500, detail="Manual resolution persisted an incomplete target project")

    return {
        "ok": True,
        "transactionId": normalized_transaction_id,
        "manualResolvedProjectId": updated_sap.get("manualResolvedProjectId"),
        "manualResolvedProjectCode": updated_sap.get("manualResolvedProjectCode"),
        "manualResolvedProjectName": updated_sap.get("manualResolvedProjectName"),
        "isProjectResolutionSuspicious": bool(updated_sap.get("isProjectResolutionSuspicious")),
        "resolvedAt": updated_sap.get("manualResolvedAt"),
    }


@app.post("/api/admin/suspicious-project-resolutions/bulk-resolve-document")
def bulk_resolve_admin_suspicious_project_resolution_to_document(payload: dict, user: dict = Depends(require_admin)):
    transaction_ids = payload.get("transactionIds") if isinstance(payload.get("transactionIds"), list) else []
    reason = normalize_non_empty_string(payload.get("reason") or "bulk_resolve_to_document")
    if not transaction_ids:
        raise HTTPException(status_code=400, detail="transactionIds is required")

    resolved_by = normalize_non_empty_string(user.get("displayName") or user.get("username") or user.get("id"))
    now_iso = datetime.now(timezone.utc).isoformat()
    updated_count = 0

    for raw_id in transaction_ids:
        tx_id = str(raw_id or "").strip()
        if not ObjectId.is_valid(tx_id):
            continue
        tx = db.transactions.find_one({"_id": ObjectId(tx_id), "source": "sap-sbo"}, {"sap.documentProjectCode": 1, "sap.documentProjectName": 1})
        if not tx:
            continue
        sap_doc = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
        selected_project_id, selected_project_code, selected_project_name = resolve_project_metadata_from_code_or_name(
            project_code=normalize_non_empty_string(sap_doc.get("documentProjectCode")),
            project_name=normalize_non_empty_string(sap_doc.get("documentProjectName")),
        )
        if not selected_project_id:
            continue

        db.transactions.update_one(
            {"_id": ObjectId(tx_id)},
            {
                "$set": {
                    "sap.manualResolvedProjectId": selected_project_id,
                    "sap.manualResolvedProjectCode": selected_project_code,
                    "sap.manualResolvedProjectName": selected_project_name,
                    "sap.manualResolvedBy": resolved_by,
                    "sap.manualResolvedAt": now_iso,
                    "sap.manualResolutionReason": reason,
                    "updated_at": now_iso,
                }
            },
        )
        updated_count += 1

    return {"ok": True, "updated": updated_count}


@app.get("/api/supplier-categories")
def list_supplier_categories(_: dict = Depends(require_authenticated)):
    return [serialize(c) for c in db.supplierCategoryCatalog.find({}).sort("name", 1)]


@app.get("/api/projects")
def list_projects(user: dict = Depends(require_authenticated)):
    query: dict = {"visibleInFrontend": {"$ne": False}}
    accessible_project_ids = get_accessible_project_ids(user)
    if accessible_project_ids is not None:
        if not accessible_project_ids:
            return []
        query["_id"] = {"$in": [ObjectId(pid) for pid in accessible_project_ids]}

    rows = db.projects.find(
        query,
        {"name": 1, "displayName": 1, "slug": 1},
    ).sort("name", 1)
    return [
        {
            "_id": str(row["_id"]),
            "name": row.get("name"),
            "displayName": row.get("displayName"),
            "slug": row.get("slug"),
        }
        for row in rows
    ]


@app.post("/api/admin/projects", status_code=201)
def create_project_admin(payload: dict, _: dict = Depends(require_admin)):
    display_name = (payload.get("displayName") or payload.get("name") or "").strip()
    if not display_name:
        raise HTTPException(status_code=400, detail="displayName is required")

    slug = normalize_project_slug(payload.get("slug"))

    raw_project_names = payload.get("sapProjectNames")
    if not isinstance(raw_project_names, list):
        raw_project_names = []
    sap_project_names = [str(item).strip() for item in raw_project_names if str(item).strip()]

    raw_project_name = (payload.get("rawProjectName") or "").strip()
    if raw_project_name and raw_project_name not in sap_project_names:
        sap_project_names.insert(0, raw_project_name)
    if not sap_project_names:
        sap_project_names = [display_name]

    source_sbo = (payload.get("sourceSbo") or "").strip()

    s3_prefix = None
    if payload.get("s3Prefix"):
        s3_prefix = normalize_project_prefix(payload.get("s3Prefix"), slug)

    if db.projects.find_one({"slug": slug}, {"_id": 1}):
        raise HTTPException(status_code=409, detail="Project slug already exists")

    if db.projects.find_one(
        {
            "$or": [
                {"name": {"$regex": f"^{re.escape(display_name)}$", "$options": "i"}},
                {"displayName": {"$regex": f"^{re.escape(display_name)}$", "$options": "i"}},
            ]
        },
        {"_id": 1},
    ):
        raise HTTPException(status_code=409, detail="Project name already exists")

    now_iso = datetime.now(timezone.utc).isoformat()
    sap_payload = {
        "projectNames": sap_project_names,
    }
    if source_sbo:
        sap_payload["sourceSbo"] = source_sbo
    if raw_project_name:
        sap_payload["rawProjectName"] = raw_project_name
    if s3_prefix:
        sap_payload["s3"] = {"bucket": DEFAULT_PROJECT_S3_BUCKET, "prefix": s3_prefix}

    doc = {
        "name": slug,
        "displayName": display_name,
        "slug": slug,
        "sap": sap_payload,
        "created_at": now_iso,
        "updated_at": now_iso,
    }

    try:
        project_id = db.projects.insert_one(doc).inserted_id
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Project name or slug already exists")

    return {
        "ok": True,
        "projectId": str(project_id),
        "name": slug,
        "displayName": display_name,
        "slug": slug,
        "sap": sap_payload,
    }


@app.get("/api/admin/projects")
def list_projects_admin(_: dict = Depends(require_admin)):
    projection = {
        "name": 1,
        "displayName": 1,
        "slug": 1,
        "visibleInFrontend": 1,
        "sap.sourceSbo": 1,
        "sap.rawProjectName": 1,
        "sap.projectNames": 1,
    }
    rows = db.projects.find({}, projection).sort("name", 1)
    response: list[dict] = []
    for row in rows:
        sap = row.get("sap") if isinstance(row.get("sap"), dict) else {}
        response.append(
            {
                "_id": str(row["_id"]),
                "name": row.get("name"),
                "displayName": row.get("displayName"),
                "slug": row.get("slug"),
                "visibleInFrontend": row.get("visibleInFrontend") is not False,
                "sap": {
                    "sourceSbo": sap.get("sourceSbo"),
                    "rawProjectName": sap.get("rawProjectName"),
                    "projectNames": sap.get("projectNames") if isinstance(sap.get("projectNames"), list) else [],
                },
            }
        )
    return response


@app.patch("/api/admin/projects/{project_id}/visibility")
def update_project_visibility_admin(project_id: str, payload: dict, _: dict = Depends(require_admin)):
    if "visibleInFrontend" not in payload or not isinstance(payload.get("visibleInFrontend"), bool):
        raise HTTPException(status_code=400, detail="visibleInFrontend must be a boolean")

    updated = db.projects.find_one_and_update(
        {"_id": oid(project_id)},
        {
            "$set": {
                "visibleInFrontend": payload.get("visibleInFrontend"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Project not found")

    return {
        "ok": True,
        "_id": str(updated.get("_id")),
        "visibleInFrontend": updated.get("visibleInFrontend") is not False,
    }


@app.post("/api/admin/projects/create-from-unmatched")
def create_projects_from_unmatched(_: dict = Depends(require_admin)):
    created_projects: list[dict] = []
    skipped_projects: list[dict] = []

    projection = {"rawProjectName": 1, "sourceSbo": 1}
    for row in db.unmatched_projects.find({}, projection):
        raw_project_name = str(row.get("rawProjectName") or "").strip()
        source_sbo = str(row.get("sourceSbo") or "").strip()

        if not raw_project_name:
            skipped_projects.append(
                {
                    "rawProjectName": raw_project_name,
                    "sourceSbo": source_sbo,
                    "reason": "missing rawProjectName",
                }
            )
            continue

        slug = normalize_slug_from_raw_project_name(raw_project_name)
        if not slug or not re.fullmatch(r"[a-z0-9-]+", slug):
            skipped_projects.append(
                {
                    "rawProjectName": raw_project_name,
                    "sourceSbo": source_sbo,
                    "reason": "invalid_slug",
                }
            )
            continue

        existing_by_sap = db.projects.find_one({"sap.projectNames": raw_project_name}, {"_id": 1})
        if existing_by_sap:
            skipped_projects.append(
                {
                    "displayName": raw_project_name,
                    "slug": slug,
                    "rawProjectName": raw_project_name,
                    "sourceSbo": source_sbo,
                    "reason": "existing sap.projectNames match",
                }
            )
            continue

        existing_by_slug = db.projects.find_one({"slug": slug}, {"_id": 1})
        if existing_by_slug:
            skipped_projects.append(
                {
                    "displayName": raw_project_name,
                    "slug": slug,
                    "rawProjectName": raw_project_name,
                    "sourceSbo": source_sbo,
                    "reason": "existing slug match",
                }
            )
            continue

        now_iso = datetime.now(timezone.utc).isoformat()
        doc = {
            "name": slug,
            "displayName": raw_project_name,
            "slug": slug,
            "sap": {
                "projectNames": [raw_project_name],
                "sourceSbo": source_sbo,
                "rawProjectName": raw_project_name,
            },
            "visibleInFrontend": False,
            "created_at": now_iso,
            "updated_at": now_iso,
        }

        try:
            db.projects.insert_one(doc)
        except DuplicateKeyError:
            skipped_projects.append(
                {
                    "displayName": raw_project_name,
                    "slug": slug,
                    "rawProjectName": raw_project_name,
                    "sourceSbo": source_sbo,
                    "reason": "duplicate key during insert",
                }
            )
            continue

        created_projects.append(
            {
                "displayName": raw_project_name,
                "slug": slug,
                "rawProjectName": raw_project_name,
                "sourceSbo": source_sbo,
            }
        )

    return {
        "ok": True,
        "createdCount": len(created_projects),
        "skippedExistingCount": len(skipped_projects),
        "createdProjects": created_projects,
        "skippedProjects": skipped_projects,
    }


@app.post("/api/admin/s3/create-prefix")
def create_s3_prefix_admin(payload: dict, _: dict = Depends(require_admin)):
    slug = normalize_project_slug(payload.get("slug"))
    prefix = f"exports/{slug}/"

    region = (os.getenv("AWS_REGION") or "").strip()
    s3_client = boto3.client("s3", region_name=region or None)

    try:
        s3_client.put_object(Bucket=DEFAULT_PROJECT_S3_BUCKET, Key=prefix, Body=b"")
        s3_client.put_object(Bucket=DEFAULT_PROJECT_S3_BUCKET, Key=f"{prefix}.keep", Body=b"keep")
    except Exception as exc:
        logger.exception("Failed creating S3 prefix marker bucket=%s prefix=%s", DEFAULT_PROJECT_S3_BUCKET, prefix)
        raise HTTPException(status_code=500, detail=f"Could not create S3 prefix: {exc}")

    return {
        "ok": True,
        "bucket": DEFAULT_PROJECT_S3_BUCKET,
        "prefix": prefix,
    }


@app.post("/api/supplier-categories")
def create_supplier_category(payload: dict, _: dict = Depends(require_admin)):
    name = (payload.get("name") or "").strip()
    if len(name) < 2:
        raise HTTPException(status_code=400, detail="name is required")
    if db.supplierCategoryCatalog.find_one({"name": name}):
        raise HTTPException(status_code=409, detail="Supplier category already exists")
    _id = db.supplierCategoryCatalog.insert_one({"name": name}).inserted_id
    return serialize(db.supplierCategoryCatalog.find_one({"_id": _id}))


@app.get("/api/suppliers")
def list_suppliers(uncategorized: int = 0, request: FastAPIRequest = None, user: dict = Depends(require_authenticated)):
    active_project_id = get_active_project_id(request)
    if not can_access_project(user, active_project_id):
        if is_viewer(user):
            return []
        raise HTTPException(status_code=403, detail="Project access denied")

    query = {"$and": [{"$or": [{"projectId": active_project_id}, {"projectIds": active_project_id}]}]}
    if uncategorized == 1:
        query["$and"].append({"$or": [{"categoryId": None}, {"categoryId": {"$exists": False}}]})
    return [serialize(s) for s in db.suppliers.find(query).sort("name", 1)]


@app.patch("/api/suppliers/{supplier_id}")
def update_supplier(supplier_id: str, payload: dict, request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    supplier_filter = {
        "_id": oid(supplier_id),
        "$or": [{"projectId": active_project_id}, {"projectIds": active_project_id}],
    }
    supplier = db.suppliers.find_one(supplier_filter)
    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    category_id = payload.get("categoryId")
    if category_id is not None:
        if not db.supplierCategoryCatalog.find_one({"_id": oid(category_id)}):
            raise HTTPException(status_code=400, detail="Invalid categoryId")
        db.suppliers.update_one(supplier_filter, {"$set": {"categoryId": category_id}})
    else:
        db.suppliers.update_one(supplier_filter, {"$set": {"categoryId": None}})

    return serialize(db.suppliers.find_one(supplier_filter))


@app.get("/api/projects/{project_id}/supplier-categories")
def list_project_supplier_categories(project_id: str, user: dict = Depends(require_authenticated)):
    resolved_project_id = resolve_project_id(project_id)
    if not can_access_project(user, resolved_project_id):
        if is_viewer(user):
            return []
        raise HTTPException(status_code=403, detail="Project access denied")

    rows = list(db.supplierCategories.find({"projectId": resolved_project_id}).sort("updatedAt", -1))

    supplier_ids = [oid(row.get("supplierId")) for row in rows if ObjectId.is_valid(str(row.get("supplierId") or ""))]
    category_ids = [oid(row.get("categoryId")) for row in rows if ObjectId.is_valid(str(row.get("categoryId") or ""))]

    suppliers_by_id = {
        str(s.get("_id")): s
        for s in db.suppliers.find({"_id": {"$in": supplier_ids}}, {"name": 1, "cardCode": 1})
    } if supplier_ids else {}
    categories_by_id = {
        str(c.get("_id")): c
        for c in db.categories.find({"_id": {"$in": category_ids}}, {"name": 1, "code": 1})
    } if category_ids else {}

    result = []
    for row in rows:
        item = serialize(row)
        supplier = suppliers_by_id.get(str(row.get("supplierId") or ""))
        category = categories_by_id.get(str(row.get("categoryId") or ""))
        if supplier:
            item["supplierName"] = supplier.get("name") or supplier.get("cardCode")
            item["supplierCardCode"] = supplier.get("cardCode")
        if category:
            item["categoryName"] = category.get("name") or category.get("code")
            item["categoryCode"] = category.get("code") or str(category.get("_id"))
        result.append(item)
    return result


@app.put("/api/projects/{project_id}/suppliers/{supplier_id}/category2")
def upsert_project_supplier_category_rule(project_id: str, supplier_id: str, payload: dict, user: dict = Depends(require_admin)):
    resolved_project_id = resolve_project_id(project_id)
    if not ObjectId.is_valid(supplier_id):
        raise HTTPException(status_code=400, detail="Invalid supplierId")

    supplier_filter = {
        "_id": ObjectId(supplier_id),
        "$or": [{"projectId": resolved_project_id}, {"projectIds": resolved_project_id}],
    }
    supplier = db.suppliers.find_one(supplier_filter, {"_id": 1})
    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    category_id = normalize_non_empty_string(payload.get("categoryId"))
    if not category_id or not ObjectId.is_valid(category_id):
        raise HTTPException(status_code=400, detail="categoryId is required")

    category = db.categories.find_one(
        {"_id": ObjectId(category_id), "$or": [{"projectId": resolved_project_id}, {"projectIds": resolved_project_id}]},
        {"name": 1},
    )
    if not category:
        raise HTTPException(status_code=400, detail="Invalid categoryId")

    now_iso = datetime.now(timezone.utc).isoformat()
    updated_by = normalize_non_empty_string(user.get("username") if isinstance(user, dict) else None) or "system"

    db.supplierCategories.update_one(
        {"projectId": resolved_project_id, "supplierId": supplier_id},
        {
            "$set": {
                "projectId": resolved_project_id,
                "supplierId": supplier_id,
                "categoryId": category_id,
                "updatedAt": now_iso,
                "updatedBy": updated_by,
            },
            "$setOnInsert": {"createdAt": now_iso},
        },
        upsert=True,
    )

    apply_to_existing = bool(payload.get("applyToExisting"))
    modified = 0
    if apply_to_existing:
        category_name = normalize_non_empty_string(category.get("name")) or category_id
        manual_absent_query = {
            "$and": [
                {"$or": [{"categoryManualCode": {"$exists": False}}, {"categoryManualCode": None}, {"categoryManualCode": ""}]},
                {"$or": [{"category_override_id": {"$exists": False}}, {"category_override_id": None}, {"category_override_id": ""}]},
                {"$or": [{"category_locked": {"$exists": False}}, {"category_locked": False}, {"category_locked": None}]},
            ]
        }
        update_result = db.transactions.update_many(
            {
                "projectId": resolved_project_id,
                "source": "sap",
                "supplierId": supplier_id,
                **manual_absent_query,
            },
            {
                "$set": {
                    "categoryManualCode": category_id,
                    "categoryManualName": category_name,
                    "categoryEffectiveCode": category_id,
                    "categoryEffectiveName": category_name,
                    "category_source": "vendor_rule",
                    "category_override_id": category_id,
                    "category_id": category_id,
                    "categoryId": category_id,
                    "categoryManualUpdatedAt": now_iso,
                    "categoryManualUpdatedBy": updated_by,
                    "category_locked": False,
                }
            },
        )
        modified = update_result.modified_count or 0

    stored = db.supplierCategories.find_one({"projectId": resolved_project_id, "supplierId": supplier_id})
    response = serialize(stored)
    response["applyToExistingModified"] = modified
    return response


def build_sap_vendor_upsert(card_code: str, beneficiary: str, project_id: str):
    return UpdateOne(
        {"source": "sap", "projectId": project_id, "supplierCardCode": card_code},
        {
            "$set": {
                "name": beneficiary or card_code,
                "source": "sap",
                "externalIds.sapCardCode": card_code,
                "supplierCardCode": card_code,
                "projectId": project_id,
                "active": True,
            },
            "$setOnInsert": {
                "categoryId": None,
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
    project_name = (project or "").strip()
    if not project_name:
        raise HTTPException(status_code=400, detail="project is required")
    default_s3_prefix = {
        "CALDERON DE LA BARCA": "exports/calderon",
        "PENSYLVANIA": "exports/pensylvania",
    }.get(project_name.upper())

    set_on_insert = {"name": project_name}
    if default_s3_prefix:
        set_on_insert["sap.s3.prefix"] = default_s3_prefix

    project_doc = db.projects.find_one_and_update(
        {"name": project_name},
        {"$setOnInsert": set_on_insert},
        upsert=True,
    )
    if not project_doc:
        project_doc = db.projects.find_one({"name": project_name})

    if default_s3_prefix and project_doc and isinstance(project_doc, dict):
        sap_doc = project_doc.get("sap") if isinstance(project_doc.get("sap"), dict) else {}
        s3_doc = sap_doc.get("s3") if isinstance(sap_doc.get("s3"), dict) else {}
        existing_prefix = str(s3_doc.get("prefix") or "").strip()
        if not existing_prefix:
            db.projects.update_one(
                {
                    "_id": project_doc["_id"],
                    "$or": [
                        {"sap.s3.prefix": {"$exists": False}},
                        {"sap.s3.prefix": None},
                        {"sap.s3.prefix": ""},
                    ],
                },
                {"$set": {"sap.s3.prefix": default_s3_prefix}},
            )

    return str(project_doc["_id"])


def build_supplier_rule_cache(project_id: str, supplier_ids: list[str]) -> dict[str, dict]:
    if not supplier_ids:
        return {}

    rules = {}
    for row in db.supplierCategories.find(
        {"projectId": project_id, "supplierId": {"$in": supplier_ids}},
        {"supplierId": 1, "categoryId": 1},
    ):
        supplier_id = normalize_non_empty_string(row.get("supplierId"))
        category_id = normalize_non_empty_string(row.get("categoryId"))
        if not supplier_id or not category_id:
            continue
        rules[supplier_id] = {"categoryId": category_id}
    return rules


def build_category_name_cache_for_ids(project_id: str, category_ids: list[str]) -> dict[str, str]:
    normalized_ids = []
    for category_id in category_ids:
        normalized = normalize_non_empty_string(category_id)
        if not normalized or not ObjectId.is_valid(normalized):
            continue
        normalized_ids.append(ObjectId(normalized))

    if not normalized_ids:
        return {}

    names = {}
    for category in db.categories.find(
        {"_id": {"$in": normalized_ids}, "$or": [{"projectId": project_id}, {"projectIds": project_id}]},
        {"name": 1},
    ):
        names[str(category.get("_id"))] = normalize_non_empty_string(category.get("name")) or str(category.get("_id"))
    return names


def transaction_has_manual_category(tx: dict | None) -> bool:
    tx_doc = tx if isinstance(tx, dict) else {}
    manual_code = normalize_non_empty_string(tx_doc.get("categoryManualCode"))
    override_id = normalize_non_empty_string(tx_doc.get("category_override_id"))
    locked = bool(tx_doc.get("category_locked"))
    return bool(manual_code or override_id or locked)


def build_supplier_auto_category_map(supplier_ids: list[str]) -> dict[str, str]:
    if not supplier_ids:
        return {}

    distinct_categories_by_supplier: dict[str, set[str]] = {}

    query = {
        "supplierId": {"$in": supplier_ids},
        "type": "EXPENSE",
        "$or": [
            {"category_id": {"$exists": True, "$nin": [None, ""]}},
            {"categoryId": {"$exists": True, "$nin": [None, ""]}},
        ],
    }

    for tx in db.transactions.find(query, {"supplierId": 1, "category_id": 1, "categoryId": 1}):
        supplier_id = str(tx.get("supplierId") or "").strip()
        if not supplier_id:
            continue

        category_id = str(tx.get("category_id") or tx.get("categoryId") or "").strip()
        if not category_id:
            continue

        if supplier_id not in distinct_categories_by_supplier:
            distinct_categories_by_supplier[supplier_id] = set()
        distinct_categories_by_supplier[supplier_id].add(category_id)

    return {
        supplier_id: next(iter(categories))
        for supplier_id, categories in distinct_categories_by_supplier.items()
        if len(categories) == 1
    }


def upsert_sap_categories_from_hints(project_id: str, line_records: list[dict]) -> dict[str, str]:
    hints_by_code: dict[str, str | None] = {}
    for record in line_records:
        code = str(record.get("categoryHintCode") or "").strip()
        if not code:
            continue

        name = str(record.get("categoryHintName") or "").strip() or None
        if code not in hints_by_code or (not hints_by_code.get(code) and name):
            hints_by_code[code] = name

    if not hints_by_code:
        return {}

    now = datetime.now(timezone.utc).isoformat()
    category_ops = []
    for code, name in hints_by_code.items():
        category_set_doc = {
            "projectId": project_id,
            "code": code,
            "name": name or code,
            "source": "sap",
            "updatedAt": now,
            "active": True,
            "isActive": True,
        }

        category_ops.append(
            UpdateOne(
                {"projectId": project_id, "code": code},
                {
                    "$set": category_set_doc,
                    "$setOnInsert": {"createdAt": now},
                },
                upsert=True,
            )
        )

    if category_ops:
        db.categories.bulk_write(category_ops, ordered=False)

    return {
        str(category.get("code")): str(category.get("_id"))
        for category in db.categories.find(
            {"projectId": project_id, "code": {"$in": list(hints_by_code.keys())}},
            {"code": 1},
        )
        if category.get("code")
    }


def normalize_category_override_update(
    payload: dict,
    *,
    lock_override: bool,
    updated_by: str | None = None,
) -> dict:
    has_manual_payload = any(
        key in payload
        for key in ("category_id", "categoryId", "categoryManualCode", "categoryManualName")
    )
    if not has_manual_payload:
        return {}

    now_iso = datetime.now(timezone.utc).isoformat()
    raw_category_id = payload.get("category_id", payload.get("categoryId"))

    if raw_category_id is not None:
        normalized_category_id = normalize_non_empty_string(raw_category_id)
        if not normalized_category_id:
            updates = {
                "categoryManualCode": None,
                "categoryManualName": None,
                "categoryManualUpdatedAt": now_iso,
                "categoryManualUpdatedBy": updated_by,
                "category_override_id": None,
                "category_locked": False,
                "category_source": "sap",
                "category_id": None,
                "categoryId": None,
            }
            updates.update(build_effective_category_fields(None, None, payload.get("categoryHintCode"), payload.get("categoryHintName")))
            return updates

        category_doc = db.categories.find_one({"_id": oid(normalized_category_id), "active": True}, {"name": 1})
        if not category_doc:
            raise HTTPException(status_code=400, detail="Invalid category_id")

        manual_name = normalize_non_empty_string(category_doc.get("name")) or normalized_category_id
        updates = {
            "categoryManualCode": normalized_category_id,
            "categoryManualName": manual_name,
            "categoryManualUpdatedAt": now_iso,
            "categoryManualUpdatedBy": updated_by,
            "category_override_id": normalized_category_id if lock_override else None,
            "category_locked": bool(lock_override),
            "category_source": "manual",
            "category_id": normalized_category_id,
            "categoryId": normalized_category_id,
        }
        updates.update(build_effective_category_fields(normalized_category_id, manual_name, payload.get("categoryHintCode"), payload.get("categoryHintName")))
        return updates

    manual_code = normalize_non_empty_string(payload.get("categoryManualCode"))
    manual_name = normalize_non_empty_string(payload.get("categoryManualName"))
    updates = {
        "categoryManualCode": manual_code,
        "categoryManualName": manual_name,
        "categoryManualUpdatedAt": now_iso,
        "categoryManualUpdatedBy": updated_by,
        "category_source": "manual" if (manual_code or manual_name) else "sap",
        "category_locked": bool(lock_override and (manual_code or manual_name)),
        "category_override_id": manual_code if lock_override and manual_code else None,
        "category_id": manual_code,
        "categoryId": manual_code,
    }
    updates.update(build_effective_category_fields(manual_code, manual_name, payload.get("categoryHintCode"), payload.get("categoryHintName")))
    return updates


def downloadFromS3Object(bucket: str, key: str) -> bytes:
    region = (os.getenv("AWS_REGION") or "").strip()
    if not region:
        raise HTTPException(status_code=500, detail="Missing AWS_REGION env var")

    s3_client = boto3.client("s3", region_name=region)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def downloadFromS3(key: str) -> bytes:
    bucket = (os.getenv("S3_BUCKET") or "").strip()
    if not bucket:
        raise HTTPException(status_code=500, detail="Missing S3_BUCKET env var")
    return downloadFromS3Object(bucket=bucket, key=key)


def build_s3_object_fingerprint(bucket: str, key: str) -> dict:
    region = (os.getenv("AWS_REGION") or "").strip()
    s3_client = boto3.client("s3", region_name=region or None)
    response = s3_client.head_object(Bucket=bucket, Key=key)

    etag = str(response.get("ETag") or "").strip().strip('"')
    last_modified = response.get("LastModified")
    last_modified_iso = last_modified.astimezone(timezone.utc).isoformat() if last_modified else None
    content_length = int(response.get("ContentLength") or 0)

    return {
        "etag": etag,
        "lastModified": last_modified_iso,
        "contentLength": content_length,
    }


def build_s3_key(filename: str) -> str:
    prefix = (os.getenv("S3_PREFIX") or "").strip().strip("/")
    if not prefix:
        return filename
    return f"{prefix}/{filename}"


def build_project_s3_key(project_id: str, filename: str) -> str:
    prefix = ""
    try:
        project_doc = db.projects.find_one(
            {"_id": ObjectId(project_id)}, {"sap": 1, "s3Prefix": 1}
        )
    except Exception:
        project_doc = None

    if isinstance(project_doc, dict):
        sap_doc = project_doc.get("sap") if isinstance(project_doc.get("sap"), dict) else {}
        s3_doc = sap_doc.get("s3") if isinstance(sap_doc.get("s3"), dict) else {}
        prefix = str(s3_doc.get("prefix") or "").strip().strip("/")
        if not prefix:
            prefix = str(project_doc.get("s3Prefix") or "").strip().strip("/")

    if not prefix:
        prefix = (os.getenv("S3_PREFIX") or "").strip().strip("/")

    logger.info(
        "Resolved project S3 key prefix project_id=%s prefix=%s filename=%s",
        project_id,
        prefix,
        filename,
    )

    if not prefix:
        return filename

    return f"{prefix}/{filename}"


def normalize_project_name_for_matching(raw_value) -> str:
    return str(raw_value or "").strip().upper()


def pick_first_non_empty_value(row: dict, *keys: str) -> str | None:
    for key in keys:
        value = normalize_non_empty_string(row.get(key))
        if value:
            return value
    return None


def resolve_sbo_project_fields_from_row(row: dict, movement_type: str) -> dict:
    document_project_code = pick_first_non_empty_value(row, "document_project_code", "documentProjectCode")
    document_project_name = pick_first_non_empty_value(row, "document_project_name", "documentProjectName")
    payment_project_code = pick_first_non_empty_value(row, "payment_project_code", "paymentProjectCode")
    payment_project_name = pick_first_non_empty_value(row, "payment_project_name", "paymentProjectName")
    resolved_project_code = pick_first_non_empty_value(row, "resolved_project_code", "resolvedProjectCode")
    resolved_project_name = pick_first_non_empty_value(row, "resolved_project_name", "resolvedProjectName")
    project_resolution_source = pick_first_non_empty_value(
        row,
        "project_resolution_source",
        "projectResolutionSource",
    )
    raw_project_code = pick_first_non_empty_value(row, "raw_project_code", "rawProjectCode")
    raw_project_name = pick_first_non_empty_value(row, "raw_project_name", "rawProjectName")

    # Business decision (SBO V2): outgoing payments now always resolve the automatic
    # project from document-level project. payment_project_* is kept only for
    # diagnostics/suspicious workflow.
    if movement_type == "egreso":
        effective_raw_project_code = document_project_code or raw_project_code or resolved_project_code
        effective_raw_project_name = document_project_name or raw_project_name or resolved_project_name
        project_resolution_source = "document" if document_project_code or document_project_name else "document"
    else:
        effective_raw_project_code = raw_project_code or resolved_project_code
        effective_raw_project_name = raw_project_name or resolved_project_name

    return {
        "raw_project_code": effective_raw_project_code or "",
        "raw_project_name": effective_raw_project_name or "",
        "document_project_code": document_project_code,
        "document_project_name": document_project_name,
        "payment_project_code": payment_project_code,
        "payment_project_name": payment_project_name,
        "resolved_project_code": resolved_project_code,
        "resolved_project_name": resolved_project_name,
        "project_resolution_source": project_resolution_source,
    }


def build_project_resolution_suspicion_fields(movement_type: str, project_fields: dict) -> dict:
    is_outgoing = str(movement_type or "").strip().lower() == "egreso"
    document_project_code = normalize_non_empty_string(project_fields.get("document_project_code"))
    document_project_name = normalize_non_empty_string(project_fields.get("document_project_name"))
    payment_project_code = normalize_non_empty_string(project_fields.get("payment_project_code"))
    payment_project_name = normalize_non_empty_string(project_fields.get("payment_project_name"))

    suspicious = bool(
        is_outgoing
        and document_project_code
        and payment_project_code
        and document_project_code != payment_project_code
    )

    return {
        "isProjectResolutionSuspicious": suspicious,
        "projectResolutionSuspicionReasons": ["document_project_differs_from_payment_project"] if suspicious else [],
        "suggestedProjectCode": document_project_code,
        "suggestedProjectName": document_project_name,
        "conflictingPaymentProjectCode": payment_project_code,
        "conflictingPaymentProjectName": payment_project_name,
    }


def build_sbo_dedupe_key(row: dict) -> str:
    amount_applied = parse_optional_decimal(row.get("amount_applied"))
    dedupe_parts = [
        normalize_source_db_value(row.get("source_db")),
        str(row.get("movement_type") or "").strip().upper(),
        str(row.get("source_type") or "").strip().upper(),
        str(row.get("payment_docentry") or "").strip(),
        str(row.get("invoice_docentry") or "").strip(),
        f"{float(amount_applied or 0):.2f}",
    ]
    return sha256("|".join(dedupe_parts).encode("utf-8")).hexdigest()


def resolve_projects_by_sap_names() -> dict[str, str]:
    project_map: dict[str, str] = {}
    projection = {"sap.projectNames": 1}
    for project_doc in db.projects.find({"sap.projectNames": {"$exists": True}}, projection):
        project_id = str(project_doc.get("_id") or "")
        sap_doc = project_doc.get("sap") if isinstance(project_doc.get("sap"), dict) else {}
        project_names = sap_doc.get("projectNames") if isinstance(sap_doc.get("projectNames"), list) else []
        for project_name in project_names:
            normalized = normalize_project_name_for_matching(project_name)
            if normalized and normalized not in project_map:
                project_map[normalized] = project_id
    return project_map


def import_sap_movements_by_sbo(
    sbo: str,
    mode: str,
    force: int = 0,
    trigger_source: str = "api",
    actor: str | None = None,
) -> dict:
    source_sbo = str(sbo or "").strip()
    if not source_sbo:
        raise HTTPException(status_code=400, detail="sbo is required")

    normalized_mode = str(mode or "latest").strip().lower()
    if normalized_mode not in {"backfill", "latest"}:
        raise HTTPException(status_code=400, detail="Invalid mode. Use 'backfill' or 'latest'.")

    bucket = DEFAULT_PROJECT_S3_BUCKET
    s3_file = "backfill_movements.csv" if normalized_mode == "backfill" else "latest_movements.csv"
    s3_key = f"exports-v2/{source_sbo}/{s3_file}"
    file_name = s3_file
    source_file = s3_key
    file_bytes = downloadFromS3Object(bucket=bucket, key=s3_key)
    file_hash = sha256(file_bytes).hexdigest()

    import_key = f"sap-movements-by-sbo:{source_sbo}:{normalized_mode}:{s3_key}"
    now = datetime.now(timezone.utc).isoformat()
    normalized_trigger_source = _normalize_trigger_source(trigger_source)
    actor_label = _summarize_actor(actor)

    run_scope_filter = {
        "source": "sap-movements-by-sbo",
        "sourceSbo": source_sbo,
        "mode": normalized_mode,
    }
    existing_run = db.importRuns.find_one({**run_scope_filter, "sha256": file_hash})
    if existing_run and existing_run.get("status") == "ok" and force != 1:
        return {"already_imported": True, "importRunId": str(existing_run.get("_id"))}

    import_run_doc = {
        "sha256": file_hash,
        "fileName": file_name,
        "sourceFile": source_file,
        "sourceSbo": source_sbo,
        "sourceDb": "SBO",
        "importKey": import_key,
        "source": "sap-movements-by-sbo",
        "projectId": None,
        "rowsTotal": 0,
        "rowsOk": 0,
        "rowsSkipped": 0,
        "rowsError": 0,
        "status": "processing",
        "startedAt": now,
        "finishedAt": None,
        "errorsSample": [],
        "mode": normalized_mode,
        "bucket": bucket,
        "s3Key": s3_key,
        "triggerSource": normalized_trigger_source,
        "triggerActor": actor_label,
    }

    if existing_run:
        db.importRuns.update_one({"_id": existing_run["_id"]}, {"$set": import_run_doc})
        import_run_id = existing_run["_id"]
    else:
        try:
            import_run_id = db.importRuns.insert_one(import_run_doc).inserted_id
        except DuplicateKeyError:
            reusable_run = db.importRuns.find_one({**run_scope_filter, "sha256": file_hash})
            if reusable_run:
                db.importRuns.update_one({"_id": reusable_run["_id"]}, {"$set": import_run_doc})
                import_run_id = reusable_run["_id"]
            else:
                raise

    rows_total = 0
    rows_ok = 0
    rows_error = 0
    rows_imported = 0
    rows_updated = 0
    rows_unmatched = 0
    errors_sample = []
    vendor_upserts: list[UpdateOne] = []
    vendor_upsert_keys: set[str] = set()

    normalized_projects = resolve_projects_by_sap_names()
    decoded = file_bytes.decode("utf-8-sig")
    reader = csv.DictReader(decoded.splitlines())

    for idx, row in enumerate(reader, start=2):
        rows_total += 1
        try:
            movement_type = str(row.get("movement_type") or row.get("movementType") or "").strip().lower()
            project_fields = resolve_sbo_project_fields_from_row(row, movement_type)
            raw_project_name = project_fields["raw_project_name"]
            raw_project_code = project_fields["raw_project_code"]
            normalized_project_name = normalize_project_name_for_matching(raw_project_name)
            project_id = normalized_projects.get(normalized_project_name)
            category_hint_code = normalize_non_empty_string(
                row.get("CategoryHintCode")
                or row.get("category_hint_code")
                or row.get("categoryHintCode")
            )
            category_hint_name = normalize_non_empty_string(
                row.get("CategoryHintName")
                or row.get("category_hint_name")
                or row.get("categoryHintName")
            )

            if not project_id:
                rows_unmatched += 1
                db.unmatched_projects.update_one(
                    {"sourceSbo": source_sbo, "normalizedProjectName": normalized_project_name},
                    {
                        "$setOnInsert": {
                            "sourceSbo": source_sbo,
                            "normalizedProjectName": normalized_project_name,
                            "firstSeenAt": now,
                        },
                        "$set": {
                            "rawProjectName": raw_project_name,
                            "lastSeenAt": now,
                        },
                        "$inc": {"count": 1},
                    },
                    upsert=True,
                )

            dedupe_key = build_sbo_dedupe_key(row)
            source_db = normalize_source_db_value(row.get("source_db"))
            movement_date = parse_excel_date(row.get("movement_date"))
            invoice_date = parse_excel_date(row.get("invoice_date"))
            amount_applied = parse_optional_decimal(row.get("amount_applied")) or 0.0
            invoice_subtotal = parse_optional_decimal(row.get("invoice_subtotal"))
            invoice_iva = parse_optional_decimal(row.get("invoice_iva"))
            invoice_total = parse_optional_decimal(row.get("invoice_total"))
            tx_type = "INCOME" if movement_type == "ingreso" else "EXPENSE" if movement_type == "egreso" else "EXPENSE"
            suspicion_fields = build_project_resolution_suspicion_fields(movement_type, project_fields)

            tx_doc = {
                "projectId": project_id,
                "source": "sap-sbo",
                "sourceDb": source_db,
                "sourceSbo": source_sbo,
                "date": movement_date or invoice_date,
                "amount": float(amount_applied),
                "subtotal": invoice_subtotal,
                "montoSinIva": invoice_subtotal,
                "iva": invoice_iva,
                "montoIva": invoice_iva,
                "totalFactura": invoice_total,
                "tax": {
                    "subtotal": invoice_subtotal,
                    "iva": invoice_iva,
                    "totalFactura": invoice_total,
                },
                "type": tx_type,
                "description": str(row.get("payment_comments") or "").strip() or str(row.get("invoice_comments") or "").strip(),
                "supplierName": str(row.get("business_partner") or "").strip() or str(row.get("card_code") or "").strip(),
                "dedupeKey": dedupe_key,
                "categoryHintCode": category_hint_code,
                "categoryHintName": category_hint_name,
                "sap": {
                    "movementType": str(row.get("movement_type") or "").strip(),
                    "sourceType": str(row.get("source_type") or "").strip(),
                    "paymentDocEntry": str(row.get("payment_docentry") or "").strip(),
                    "paymentNum": str(row.get("payment_num") or "").strip(),
                    "invoiceDocEntry": str(row.get("invoice_docentry") or "").strip(),
                    "invoiceNum": str(row.get("invoice_num") or "").strip(),
                    "externalDocNum": str(row.get("external_doc_num") or "").strip(),
                    "movementDate": movement_date,
                    "invoiceDate": invoice_date,
                    "montoAplicado": float(amount_applied),
                    "montoAplicadoCents": to_monto_aplicado_cents(amount_applied),
                    "invoiceSubtotal": invoice_subtotal,
                    "invoiceIva": invoice_iva,
                    "invoiceTotal": invoice_total,
                    "paymentCurrency": str(row.get("payment_currency") or "").strip(),
                    "invoiceCurrency": str(row.get("invoice_currency") or "").strip(),
                    "cardCode": str(row.get("card_code") or "").strip(),
                    "businessPartner": str(row.get("business_partner") or "").strip(),
                    "categoryHintCode": category_hint_code,
                    "categoryHintName": category_hint_name,
                    "rawProjectCode": raw_project_code,
                    "rawProjectName": raw_project_name,
                    "documentProjectCode": project_fields["document_project_code"],
                    "documentProjectName": project_fields["document_project_name"],
                    "paymentProjectCode": project_fields["payment_project_code"],
                    "paymentProjectName": project_fields["payment_project_name"],
                    "projectResolutionSource": project_fields["project_resolution_source"],
                    "normalizedProjectName": normalized_project_name,
                    "sourceDb": source_db,
                    "sourceSbo": source_sbo,
                    "sourceSboMode": normalized_mode,
                    "isProjectResolutionSuspicious": suspicion_fields["isProjectResolutionSuspicious"],
                    "projectResolutionSuspicionReasons": suspicion_fields["projectResolutionSuspicionReasons"],
                    "suggestedProjectCode": suspicion_fields["suggestedProjectCode"],
                    "suggestedProjectName": suspicion_fields["suggestedProjectName"],
                    "conflictingPaymentProjectCode": suspicion_fields["conflictingPaymentProjectCode"],
                    "conflictingPaymentProjectName": suspicion_fields["conflictingPaymentProjectName"],
                },
                "updated_at": now,
            }

            supplier_name = str(tx_doc.get("supplierName") or "").strip()
            sap_doc = tx_doc.get("sap") if isinstance(tx_doc.get("sap"), dict) else {}
            card_code = str(sap_doc.get("cardCode") or "").strip()
            if project_id and (card_code or supplier_name):
                vendor_key = card_code or supplier_name.lower()
                upsert_key = f"{project_id}|{vendor_key}"
                if upsert_key not in vendor_upsert_keys:
                    vendor_upsert_keys.add(upsert_key)
                    vendor_filter = {
                        "source": "sap-sbo",
                        "projectId": project_id,
                        "supplierCardCode": card_code,
                    } if card_code else {
                        "source": "sap-sbo",
                        "projectId": project_id,
                        "name": supplier_name,
                    }
                    set_payload = {
                        "source": "sap-sbo",
                        "projectId": project_id,
                        "active": True,
                        "name": supplier_name or str(sap_doc.get("businessPartner") or "").strip() or card_code,
                    }
                    if card_code:
                        set_payload["supplierCardCode"] = card_code
                        set_payload["externalIds.sapCardCode"] = card_code
                    vendor_upserts.append(
                        UpdateOne(
                            vendor_filter,
                            {
                                "$set": set_payload,
                                "$setOnInsert": {
                                    "categoryId": None,
                                    "category_ids": [],
                                    "created_at": now,
                                },
                            },
                            upsert=True,
                        )
                    )

            existing_tx = db.transactions.find_one(
                {"dedupeKey": dedupe_key},
                {
                    "sap.manualResolvedProjectId": 1,
                    "sap.manualResolvedProjectCode": 1,
                    "sap.manualResolvedProjectName": 1,
                    "sap.manualResolvedBy": 1,
                    "sap.manualResolvedAt": 1,
                    "sap.manualResolutionReason": 1,
                },
            )
            existing_sap = existing_tx.get("sap") if isinstance((existing_tx or {}).get("sap"), dict) else {}
            for manual_key in [
                "manualResolvedProjectId",
                "manualResolvedProjectCode",
                "manualResolvedProjectName",
                "manualResolvedBy",
                "manualResolvedAt",
                "manualResolutionReason",
            ]:
                manual_value = normalize_non_empty_string(existing_sap.get(manual_key))
                if manual_value:
                    tx_doc["sap"][manual_key] = manual_value

            update_doc = {"$set": tx_doc, "$setOnInsert": {"created_at": now}}
            update_result = db.transactions.update_one({"dedupeKey": dedupe_key}, update_doc, upsert=True)
            if update_result.upserted_id:
                rows_imported += 1
            else:
                rows_updated += 1
            rows_ok += 1
        except Exception as exc:
            rows_error += 1
            if len(errors_sample) < 50:
                errors_sample.append({"row": idx, "error": str(exc)})

    if vendor_upserts:
        db.vendors.bulk_write(vendor_upserts, ordered=False)

    finished_at = datetime.now(timezone.utc).isoformat()
    status = "ok" if rows_error == 0 else "ok_with_errors"
    db.importRuns.update_one(
        {"_id": import_run_id},
        {
            "$set": {
                "rowsTotal": rows_total,
                "rowsOk": rows_ok,
                "rowsSkipped": rows_unmatched,
                "rowsError": rows_error,
                "status": status,
                "finishedAt": finished_at,
                "errorsSample": errors_sample,
                "summary": {
                    "imported": rows_imported,
                    "updated": rows_updated,
                    "unmatched": rows_unmatched,
                },
            }
        },
    )

    logger.info(
        "sap-movements-by-sbo completed sbo=%s mode=%s source=%s actor=%s imported=%s updated=%s unmatched=%s rows_error=%s",
        source_sbo,
        normalized_mode,
        normalized_trigger_source,
        actor_label,
        rows_imported,
        rows_updated,
        rows_unmatched,
        rows_error,
    )

    return {
        "status": status,
        "importRunId": str(import_run_id),
        "rowsTotal": rows_total,
        "rowsOk": rows_ok,
        "rowsError": rows_error,
        "imported": rows_imported,
        "updated": rows_updated,
        "unmatched": rows_unmatched,
    }


def importCsv(
    file_bytes: bytes,
    sourceDb: str,
    project: str = "CALDERON DE LA BARCA",
    force: int = 0,
    source: str = "sap-latest",
    mode: str = "upsert",
    source_file: str | None = None,
    source_sbo: str | None = None,
):
    normalized_source_db = (sourceDb or "").strip().upper() or "SAP"
    file_name = source_file or f"latest_{normalized_source_db}.csv"
    return run_sap_import(
        file_name=file_name,
        file_bytes=file_bytes,
        project=project,
        force=force,
        source=source,
        mode=mode,
        source_db_override=normalized_source_db,
        source_file=source_file,
        source_sbo=source_sbo,
    )


def run_s3_latest_sap_import(
    project: str = "CALDERON DE LA BARCA",
    force: int = 0,
    mode: str = "upsert",
    source: str = "sap-latest",
):
    project_id = get_or_create_project_id(project)
    project_doc = db.projects.find_one({"_id": ObjectId(project_id)}, {"sap": 1, "s3Prefix": 1}) or {}
    sap_doc = project_doc.get("sap") if isinstance(project_doc.get("sap"), dict) else {}
    sap_s3_doc = sap_doc.get("s3") if isinstance(sap_doc.get("s3"), dict) else {}

    prefijo = str(sap_s3_doc.get("prefix") or "").strip()
    if not prefijo:
        prefijo = str(project_doc.get("s3Prefix") or "").strip()
    if not prefijo:
        prefijo = str(os.getenv("S3_PREFIX") or "").strip()

    prefijo = prefijo.strip("/")
    if not prefijo:
        raise RuntimeError(f"Missing S3 prefix for project '{project}' (project_id={project_id})")

    bucket = str(sap_s3_doc.get("bucket") or "").strip() or DEFAULT_PROJECT_S3_BUCKET

    logger.info(
        "Running latest SAP import project=%s project_id=%s bucket=%s prefix=%s",
        project,
        project_id,
        bucket,
        prefijo,
    )

    source_configs = [
        {"label": "iva", "sourceDb": "IVA", "sourceSbo": "SBO_GMDI", "s3Key": f"{prefijo}/latest_IVA.csv"},
        {
            "label": "efectivo",
            "sourceDb": "EFECTIVO",
            "sourceSbo": "SBO_RAFAEL",
            "s3Key": f"{prefijo}/latest_EFECTIVO.csv",
        },
    ]

    result = {}
    for config in source_configs:
        source_db = config["sourceDb"]
        source_sbo = config["sourceSbo"]
        s3_key = config["s3Key"]

        fingerprint = build_s3_object_fingerprint(bucket=bucket, key=s3_key)
        state_query = {
            "projectId": project_id,
            "sourceDb": source_db,
            "sourceSbo": source_sbo,
        }
        existing_state = db.sap_import_state.find_one(state_query)

        fingerprint_matches = bool(
            existing_state
            and str(existing_state.get("s3Key") or "") == s3_key
            and str(existing_state.get("etag") or "") == str(fingerprint.get("etag") or "")
            and str(existing_state.get("lastModified") or "") == str(fingerprint.get("lastModified") or "")
            and int(existing_state.get("contentLength") or 0) == int(fingerprint.get("contentLength") or 0)
        )

        if fingerprint_matches and force != 1:
            summary = {
                "already_imported": True,
                "importRunId": str(existing_state.get("lastImportRunId") or ""),
                "etag": fingerprint.get("etag"),
                "lastModified": fingerprint.get("lastModified"),
                "contentLength": fingerprint.get("contentLength"),
            }
            result[config["label"]] = summary
            continue

        logger.info("Attempting S3 download for source_db=%s key=%s", source_db, s3_key)
        file_bytes = downloadFromS3Object(bucket=bucket, key=s3_key)
        summary = importCsv(
            file_bytes,
            sourceDb=source_db,
            project=project,
            force=force,
            source=source,
            mode=mode,
            source_file=s3_key,
            source_sbo=source_sbo,
        )

        summary["etag"] = fingerprint.get("etag")
        summary["lastModified"] = fingerprint.get("lastModified")
        summary["contentLength"] = fingerprint.get("contentLength")

        import_run_id = str(summary.get("importRunId") or "").strip()
        if import_run_id:
            db.sap_import_state.update_one(
                state_query,
                {
                    "$set": {
                        "projectId": project_id,
                        "sourceDb": source_db,
                        "sourceSbo": source_sbo,
                        "s3Key": s3_key,
                        "etag": str(fingerprint.get("etag") or ""),
                        "lastModified": fingerprint.get("lastModified"),
                        "contentLength": int(fingerprint.get("contentLength") or 0),
                        "lastImportRunId": import_run_id,
                        "updatedAt": datetime.now(timezone.utc).isoformat(),
                    }
                },
                upsert=True,
            )

        result[config["label"]] = summary

    return result


@app.post("/api/telegram/webhook")
async def telegram_webhook(
    update: dict,
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
):
    configured_secret = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()
    if configured_secret and x_telegram_bot_api_secret_token != configured_secret:
        raise HTTPException(status_code=401, detail="Invalid Telegram webhook secret")

    callback_query = update.get("callback_query") if isinstance(update, dict) else None
    if isinstance(callback_query, dict):
        return _telegram_handle_callback(callback_query)

    message = update.get("message") if isinstance(update, dict) else None
    chat = message.get("chat") if isinstance(message, dict) else None
    chat_id = chat.get("id") if isinstance(chat, dict) else None
    from_user = message.get("from") if isinstance(message, dict) else None
    username, first_name, last_name = _telegram_extract_user_data(from_user)
    text = (message.get("text") or "").strip() if isinstance(message, dict) else ""
    date_value = str(message.get("date") or "") if isinstance(message, dict) else ""

    logger.info(
        "Telegram webhook received update_id=%s chat_id=%s username=%s text=%s",
        update.get("update_id") if isinstance(update, dict) else None,
        chat_id,
        username,
        text,
    )

    if chat_id is None:
        return {"ok": True, "ignored": "no_message_or_chat_id"}

    if _telegram_handle_admin_command(chat_id=chat_id, text=text):
        return {"ok": True, "handled": "admin_command"}

    if not _telegram_is_chat_approved(chat_id):
        send_telegram_to_chat("No autorizado. Envié solicitud al admin.", chat_id=chat_id)
        if not _telegram_has_recent_pending_request(chat_id):
            _telegram_register_access_request(chat_id=chat_id, from_user=from_user)
            _telegram_notify_admin_access_request(
                chat_id=chat_id,
                from_user=from_user,
                text=text,
                date_value=date_value,
            )
        else:
            logger.info("Telegram access request throttled for chat_id=%s", chat_id)
        return {"ok": True, "ignored": "chat_id_not_approved", "username": username, "first_name": first_name, "last_name": last_name}

    if text.startswith("/start") or text.startswith("/help"):
        send_telegram_to_chat(_telegram_help_text(), chat_id=chat_id)
    elif text.startswith("/project"):
        selected_project_id = _telegram_get_selected_project_id(chat_id)
        response_text, keyboard = _telegram_build_projects_keyboard(selected_project_id=selected_project_id)
        tg_send(chat_id=chat_id, text=response_text, reply_markup=keyboard)
    elif text.startswith("/ping"):
        send_telegram_to_chat("pong", chat_id=chat_id)
    elif text.startswith("/import_status"):
        send_telegram_to_chat(_telegram_import_status_summary(), chat_id=chat_id)
    elif text.startswith("/count"):
        if _telegram_require_project_selection(chat_id, pending_command="count", pending_text=text):
            return {"ok": True, "pending": "project_selection"}
        selected_project_id = _telegram_get_selected_project_id(chat_id)
        send_telegram_to_chat(_telegram_count_transactions(selected_project_id), chat_id=chat_id)
    elif text.startswith("/sum"):
        if _telegram_require_project_selection(chat_id, pending_command="sum", pending_text=text):
            return {"ok": True, "pending": "project_selection"}
        selected_project_id = _telegram_get_selected_project_id(chat_id)
        _, _, month_token = text.partition(" ")
        send_telegram_to_chat(_telegram_sum_expenses(project_id=selected_project_id, month_token=month_token.strip(), include_iva=False), chat_id=chat_id)
    elif text.startswith("/prov"):
        if _telegram_require_project_selection(chat_id, pending_command="prov", pending_text=text):
            return {"ok": True, "pending": "project_selection"}
        response_text, keyboard = _telegram_search_suppliers_keyboard(chat_id, text)
        tg_send(chat_id=chat_id, text=response_text, reply_markup=keyboard)
    elif text.startswith("/catid"):
        if _telegram_require_project_selection(chat_id, pending_command="catid", pending_text=text):
            return {"ok": True, "pending": "project_selection"}
        selected_project_id = _telegram_get_selected_project_id(chat_id)
        send_telegram_to_chat(_telegram_search_category_id(selected_project_id, text), chat_id=chat_id)
    elif text.startswith("/cat"):
        if _telegram_require_project_selection(chat_id, pending_command="cat", pending_text=text):
            return {"ok": True, "pending": "project_selection"}
        response_text, keyboard = _telegram_search_categories_keyboard(chat_id, text)
        tg_send(chat_id=chat_id, text=response_text, reply_markup=keyboard)
    elif text.startswith("/find"):
        if _telegram_require_project_selection(chat_id, pending_command="find", pending_text=text):
            return {"ok": True, "pending": "project_selection"}
        selected_project_id = _telegram_get_selected_project_id(chat_id)
        send_telegram_to_chat(_telegram_search_find(selected_project_id, text), chat_id=chat_id)
    elif text.startswith("/ask"):
        if _telegram_require_project_selection(chat_id, pending_command="ask", pending_text=text):
            return {"ok": True, "pending": "project_selection"}
        selected_project_id = _telegram_get_selected_project_id(chat_id)
        _, _, ask_text = text.partition(" ")
        send_telegram_to_chat(_telegram_ask_transactions(selected_project_id, ask_text.strip()), chat_id=chat_id)
    elif text.startswith("/chatid"):
        set_setting(TELEGRAM_SETTINGS_KEY, str(chat_id))
        send_telegram_to_chat(f"chat_id registrado: {chat_id}", chat_id=chat_id)
    elif text and not text.startswith("/"):
        if _telegram_require_project_selection(chat_id, pending_command="ask", pending_text=text):
            return {"ok": True, "pending": "project_selection"}
        selected_project_id = _telegram_get_selected_project_id(chat_id)
        send_telegram_to_chat(_telegram_ask_transactions(selected_project_id, text), chat_id=chat_id)

    return {"ok": True}


def run_sap_import(
    file_name: str,
    file_bytes: bytes,
    project: str | None,
    force: int,
    project_id: str | None = None,
    source: str = "sap-payments",
    mode: str = "upsert",
    confirm_rebuild: int = 0,
    allow_rebuild: bool = False,
    source_db_override: str | None = None,
    source_file: str | None = None,
    source_sbo: str | None = None,
):
    resolved_project_id = (project_id or "").strip()
    if resolved_project_id:
        if not ObjectId.is_valid(resolved_project_id):
            raise HTTPException(status_code=400, detail="Invalid projectId")
        if not db.projects.find_one({"_id": ObjectId(resolved_project_id)}, {"_id": 1}):
            raise HTTPException(status_code=404, detail="Project not found")
    else:
        resolved_project_id = get_or_create_project_id(project or "")

    project_id = resolved_project_id
    source_file_key = normalize_source_file_key(source_file=source_file, file_name=file_name)
    source_file_value = source_file_key or None
    source_sbo_value = (source_sbo or "").strip() or None
    source_db_value = normalize_source_db_value(source_db_override)
    import_key = f"{project_id}:{source_db_value}:{source_file_value or file_name}"
    file_hash = sha256(file_bytes).hexdigest()

    existing_run = db.importRuns.find_one({"projectId": project_id, "sha256": file_hash})
    existing_ok_run = existing_run and existing_run.get("status") == "ok"

    if existing_ok_run and force != 1:
        return {"already_imported": True, "importRunId": str(existing_run["_id"])}

    now = datetime.now(timezone.utc).isoformat()
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
        "sourceFile": source_file_value,
        "sourceSbo": source_sbo_value,
        "sourceDb": source_db_value,
        "importKey": import_key,
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

    reusable_run = existing_run

    should_reuse_existing_run = reusable_run and (
        force == 1 or reusable_run.get("status") != "ok" or (reusable_run.get("rowsOk") or 0) == 0
    )

    if should_reuse_existing_run:
        db.importRuns.update_one({"_id": reusable_run["_id"]}, {"$set": import_run_doc})
        import_run_id = reusable_run["_id"]
    else:
        import_run_id = db.importRuns.insert_one(import_run_doc).inserted_id

    rows = parse_sap_file(file_name, file_bytes)

    suppliers_created = 0
    payments_upserted = 0
    invoices_upserted = 0
    lines_inserted = 0
    sap_expenses_upserted = 0
    sap_expenses_inserted = 0
    sap_expenses_updated = 0
    category_preserved_count = 0
    category_would_have_changed_count = 0
    category_hint_updated_count = 0
    vendor_rule_applied_count = 0
    vendor_rule_skipped_because_manual_count = 0
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
            applied_amount = float(parse_decimal(row.get("MontoAplicado")))
            applied_amount_cents = to_monto_aplicado_cents(applied_amount)

            subtotal = parse_optional_decimal(row.get("subtotal"))
            iva = parse_optional_decimal(row.get("iva"))
            retenciones = parse_optional_decimal(row.get("retenciones"))
            total_factura = parse_optional_decimal(row.get("totalfactura"))
            source_db = (source_db_override or str(row.get("sourceDb") or "").strip() or "SAP").upper()
            category_hint_code = (
                str(
                    row.get("CategoryHintCode")
                    or row.get("category_hint_code")
                    or row.get("categoryHintCode")
                    or row.get("categoryhintcode")
                    or ""
                ).strip()
                or None
            )
            category_hint_name = (
                str(
                    row.get("CategoryHintName")
                    or row.get("category_hint_name")
                    or row.get("categoryHintName")
                    or row.get("categoryhintname")
                    or ""
                ).strip()
                or None
            )
            category_hint_project = str(row.get("categoryhintproject") or "").strip() or None

            if card_code not in existing_cardcodes and card_code not in created_cardcodes:
                suppliers_created += 1
                created_cardcodes.add(card_code)

            suppliers_ops.append(
                UpdateOne(
                    {"cardCode": card_code},
                    {
                        # Suppliers are shared across projects by cardCode, but we track membership per project.
                        "$setOnInsert": {
                            "cardCode": card_code,
                            "categoryId": None,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "projectId": project_id,
                        },
                        "$set": {"name": beneficiary or card_code},
                        "$addToSet": {"projectIds": project_id},
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
                    "paymentNum": str(payment_num).strip(),
                    "invoiceNum": str(invoice_num).strip(),
                    "cardCode": card_code,
                    "paymentKey": payment_key,
                    "invoiceKey": invoice_key,
                    "appliedAmount": applied_amount,
                    "appliedAmountCents": applied_amount_cents,
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
                    "categoryHintCode": category_hint_code,
                    "categoryHintName": category_hint_name,
                    "categoryHintProject": category_hint_project,
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

    supplier_cardcodes = list(suppliers_seen.keys())
    suppliers_map = {
        s["cardCode"]: str(s["_id"])
        for s in db.suppliers.find({"cardCode": {"$in": supplier_cardcodes}}, {"cardCode": 1})
    }

    # Map SAP supplier card codes to vendor ids for this project.
    vendor_map = {
        v.get("supplierCardCode"): str(v.get("_id"))
        for v in db.vendors.find(
            {"projectId": project_id, "supplierCardCode": {"$in": supplier_cardcodes}},
            {"supplierCardCode": 1},
        )
        if v.get("supplierCardCode")
    }

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

    supplier_ids_in_file = sorted(
        {
            supplier_id
            for supplier_id in (suppliers_map.get(record["cardCode"]) for record in line_records)
            if supplier_id
        }
    )
    supplier_auto_category_map = build_supplier_auto_category_map(supplier_ids_in_file)
    sap_category_ids_by_code = upsert_sap_categories_from_hints(project_id, line_records)
    supplier_rule_cache = build_supplier_rule_cache(project_id, supplier_ids_in_file)
    category_name_cache = build_category_name_cache_for_ids(
        project_id,
        [row.get("categoryId") for row in supplier_rule_cache.values() if isinstance(row, dict)],
    )

    lines_ops = []
    sap_expense_ops_by_key = {}
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
            tx_identity_filter = {
                "projectId": project_id,
                "source": "sap",
                "sap.projectId": project_id,
                "sap.pagoNum": record["paymentNum"],
                "sap.facturaNum": record["invoiceNum"],
                "sap.montoAplicadoCents": record["appliedAmountCents"],
            }
            existing_tx = db.transactions.find_one(
                tx_identity_filter,
                {
                    "categoryId": 1,
                    "category_id": 1,
                    "category_auto_id": 1,
                    "category_override_id": 1,
                    "category_locked": 1,
                    "categoryHintCode": 1,
                    "categoryHintName": 1,
                    "categoryManualCode": 1,
                    "categoryManualName": 1,
                },
            )

            hinted_category_id = sap_category_ids_by_code.get(str(record.get("categoryHintCode") or "").strip())
            inferred_category_id = hinted_category_id or supplier_auto_category_map.get(supplier_id)

            supplier_name = record["beneficiary"] or record["cardCode"]
            # For UI/vendor filtering we link to vendors collection when possible.
            vendor_id = vendor_map.get(record["cardCode"]) or supplier_id
            existing_hint_code = normalize_non_empty_string((existing_tx or {}).get("categoryHintCode"))
            existing_hint_name = normalize_non_empty_string((existing_tx or {}).get("categoryHintName"))
            existing_manual_code = normalize_non_empty_string((existing_tx or {}).get("categoryManualCode"))
            existing_manual_name = normalize_non_empty_string((existing_tx or {}).get("categoryManualName"))
            incoming_hint_code = normalize_non_empty_string(record.get("categoryHintCode"))
            incoming_hint_name = normalize_non_empty_string(record.get("categoryHintName"))
            has_manual_category = transaction_has_manual_category(existing_tx)
            vendor_rule = supplier_rule_cache.get(supplier_id) if supplier_id else None

            sap_set_doc = {
                "type": "EXPENSE",
                "projectId": project_id,
                "date": record["paymentDate"] or record["invoiceDate"],
                "amount": record["appliedAmount"],
                "currency": record["currency"],
                "concept": record["concept"],
                "description": record["concept"],
                "supplierId": supplier_id,
                "supplierName": supplier_name,
                "supplierCardCode": record["cardCode"],
                "vendor_id": vendor_id,
                "vendorName": supplier_name,
                "source": "sap",
                "sourceDb": record["sourceDb"],
                "importRunId": str(import_run_id),
                "tax": record["tax"],
                "sap": {
                    "pagoNum": str(record["paymentNum"]).strip(),
                    "facturaNum": str(record["invoiceNum"]).strip(),
                    "montoAplicado": float(record["appliedAmount"]),
                    "montoAplicadoCents": record["appliedAmountCents"],
                    "cardCode": record["cardCode"],
                    "sourceFile": source_file_value,
                    "sourceSbo": record["sourceDb"],
                    "projectId": project_id,
                },
            }

            if incoming_hint_code or incoming_hint_name:
                if incoming_hint_code:
                    sap_set_doc["category_hint_code"] = incoming_hint_code
                    sap_set_doc["categoryHintCode"] = incoming_hint_code
                    sap_set_doc["categorySapCode"] = incoming_hint_code
                    sap_set_doc["sap"]["categoryHintCode"] = incoming_hint_code
                if incoming_hint_name:
                    sap_set_doc["category_hint_name"] = incoming_hint_name
                    sap_set_doc["categoryHintName"] = incoming_hint_name
                    sap_set_doc["categorySapName"] = incoming_hint_name
                    sap_set_doc["category_name"] = incoming_hint_name
                    sap_set_doc["sap"]["categoryHintName"] = incoming_hint_name
                sap_set_doc["category_hint_project"] = record["categoryHintProject"]

                if incoming_hint_code != existing_hint_code or incoming_hint_name != existing_hint_name:
                    category_hint_updated_count += 1

            if has_manual_category:
                category_preserved_count += 1
                vendor_rule_skipped_because_manual_count += 1
                if incoming_hint_code and incoming_hint_code != existing_hint_code:
                    category_would_have_changed_count += 1

            if inferred_category_id:
                sap_set_doc["category_auto_id"] = inferred_category_id
                if not has_manual_category:
                    sap_set_doc["categoryId"] = inferred_category_id
                    sap_set_doc["category_id"] = inferred_category_id
                    sap_set_doc["category_source"] = "sap"
            elif not has_manual_category:
                sap_set_doc["category_auto_id"] = None

            effective_fields = build_effective_category_fields(
                existing_manual_code,
                existing_manual_name,
                incoming_hint_code or existing_hint_code,
                incoming_hint_name or existing_hint_name,
            )

            if (not has_manual_category) and vendor_rule:
                vendor_rule_category_id = normalize_non_empty_string(vendor_rule.get("categoryId"))
                if vendor_rule_category_id:
                    vendor_rule_category_name = category_name_cache.get(vendor_rule_category_id) or vendor_rule_category_id
                    sap_set_doc["categoryManualCode"] = vendor_rule_category_id
                    sap_set_doc["categoryManualName"] = vendor_rule_category_name
                    sap_set_doc["categoryManualUpdatedAt"] = datetime.now(timezone.utc).isoformat()
                    sap_set_doc["categoryManualUpdatedBy"] = "vendor_rule"
                    sap_set_doc["categoryEffectiveCode"] = vendor_rule_category_id
                    sap_set_doc["categoryEffectiveName"] = vendor_rule_category_name
                    sap_set_doc["category_source"] = "vendor_rule"
                    sap_set_doc["category_override_id"] = vendor_rule_category_id
                    sap_set_doc["category_id"] = vendor_rule_category_id
                    sap_set_doc["categoryId"] = vendor_rule_category_id
                    effective_fields = {
                        "categoryEffectiveCode": vendor_rule_category_id,
                        "categoryEffectiveName": vendor_rule_category_name,
                    }
                    vendor_rule_applied_count += 1
            sap_set_doc.update(effective_fields)
            sap_expense_ops_by_key[
                (
                    record["paymentNum"],
                    record["invoiceNum"],
                    record["appliedAmountCents"],
                )
            ] = UpdateOne(
                tx_identity_filter,
                {
                    "$set": sap_set_doc,
                    "$setOnInsert": {
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "sourceFile": source_file_value,
                        "category_locked": False,
                        "category_override_id": None,
                    },
                },
                upsert=True,
            )

    if lines_ops:
        result = db.paymentLines.bulk_write(lines_ops, ordered=False)
        lines_inserted = result.upserted_count or 0
        duplicates_skipped = len(lines_ops) - lines_inserted

    sap_expense_ops = list(sap_expense_ops_by_key.values())
    if sap_expense_ops:
        try:
            result = db.transactions.bulk_write(sap_expense_ops, ordered=False)
            sap_expenses_upserted = (result.upserted_count or 0) + (result.modified_count or 0)
            sap_expenses_inserted = result.upserted_count or 0
            sap_expenses_updated = result.modified_count or 0
        except BulkWriteError as exc:
            details = exc.details or {}
            write_errors = details.get("writeErrors") or []
            non_duplicate_errors = [error for error in write_errors if error.get("code") != 11000]
            duplicate_errors = [error for error in write_errors if error.get("code") == 11000]

            for error in duplicate_errors:
                error_index = error.get("index")
                if error_index is None or error_index < 0 or error_index >= len(sap_expense_ops):
                    continue
                duplicate_op = sap_expense_ops[error_index]
                duplicate_update_doc = duplicate_op._doc if isinstance(duplicate_op._doc, dict) else {}
                duplicate_set_doc = duplicate_update_doc.get("$set")
                if duplicate_set_doc:
                    retry_result = db.transactions.update_one(duplicate_op._filter, {"$set": duplicate_set_doc})
                    sap_expenses_updated += retry_result.modified_count or 0

            if non_duplicate_errors:
                rows_error += len(non_duplicate_errors)
                for error in non_duplicate_errors[: max(0, 50 - len(errors_sample))]:
                    errors_sample.append(
                        {
                            "row": error.get("index"),
                            "code": error.get("code"),
                            "error": error.get("errmsg") or "Bulk write error",
                        }
                    )

            upserted_count = len(details.get("upserted") or [])
            modified_count = details.get("nModified") or 0
            sap_expenses_inserted = upserted_count
            sap_expenses_updated += modified_count
            sap_expenses_upserted = sap_expenses_inserted + sap_expenses_updated

    rows_total = len(rows)
    category_hints_rows = sum(1 for record in line_records if record.get("categoryHintCode") or record.get("categoryHintName"))
    print(
        f"sap_import category_hints project={project_id} rows_with_hints={category_hints_rows} total_rows={rows_total}"
    )
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
                "categoryPreservedCount": category_preserved_count,
                "categoryWouldHaveChangedCount": category_would_have_changed_count,
                "categoryHintUpdatedCount": category_hint_updated_count,
                "vendorRuleAppliedCount": vendor_rule_applied_count,
                "vendorRuleSkippedBecauseManualCount": vendor_rule_skipped_because_manual_count,
            }
        },
    )

    summary = {
        "already_imported": False,
        "rowsTotal": rows_total,
        "rowsOk": rows_ok,
        "suppliersCreated": suppliers_created,
        "vendorsSynced": vendors_synced,
        "paymentsUpserted": payments_upserted,
        "invoicesUpserted": invoices_upserted,
        "linesInserted": lines_inserted,
        "sapExpensesUpserted": sap_expenses_upserted,
        "insertedCount": sap_expenses_inserted,
        "updatedCount": sap_expenses_updated,
        "categoryPreservedCount": category_preserved_count,
        "categoryWouldHaveChangedCount": category_would_have_changed_count,
        "categoryHintUpdatedCount": category_hint_updated_count,
        "vendorRuleAppliedCount": vendor_rule_applied_count,
        "vendorRuleSkippedBecauseManualCount": vendor_rule_skipped_because_manual_count,
        "duplicatesSkipped": duplicates_skipped,
        "rowsError": rows_error,
        "errorsSample": errors_sample[:50],
        "importRunId": str(import_run_id),
    }
    notify_import(
        {
            "rowsOk": summary.get("rowsOk", 0),
            "duplicates": summary.get("duplicatesSkipped", 0),
            "errors": summary.get("rowsError", 0),
        }
    )
    return summary


def resolve_sap_import_project_id(
    request: FastAPIRequest,
    project_id_query: str | None,
    project_name_query: str | None,
) -> str:
    header_project_id = (request.headers.get("X-Project-Id") or "").strip()
    query_project_id = (project_id_query or "").strip()
    query_project_name = (project_name_query or "").strip()

    candidate_project_id = header_project_id or query_project_id
    if candidate_project_id:
        if not ObjectId.is_valid(candidate_project_id):
            raise HTTPException(status_code=400, detail="Invalid projectId")
        if not db.projects.find_one({"_id": ObjectId(candidate_project_id)}, {"_id": 1}):
            raise HTTPException(status_code=404, detail="Project not found")
        return candidate_project_id

    if query_project_name:
        project_doc = db.projects.find_one(
            {"name": {"$regex": f"^{re.escape(query_project_name)}$", "$options": "i"}},
            {"_id": 1},
        )
        if not project_doc:
            raise HTTPException(status_code=404, detail="Project not found")
        return str(project_doc["_id"])

    raise HTTPException(
        status_code=400,
        detail="Missing target project. Provide X-Project-Id header, query projectId, or query project.",
    )


def _normalize_guardrail_token(value: str | None) -> str:
    token = str(value or "").strip().lower()
    token = token.replace("_", "-").replace(" ", "-")
    return re.sub(r"[^a-z0-9-]", "", token)


def _normalize_project_code_for_guardrail(value: str | None) -> str:
    cleaned = str(value or "").strip().upper()
    return re.sub(r"\s+", " ", cleaned)


def _extract_filename_slug_match(file_name: str, expected_slug: str, all_project_slugs: list[str]) -> str | None:
    normalized_file_name = _normalize_guardrail_token(file_name)
    if not normalized_file_name:
        return None

    expected_token = _normalize_guardrail_token(expected_slug)
    compact_file_name = normalized_file_name.replace("-", "")
    expected_present = bool(
        expected_token
        and (expected_token in normalized_file_name or expected_token.replace("-", "") in compact_file_name)
    )
    if expected_present:
        return None

    for slug in all_project_slugs:
        candidate = _normalize_guardrail_token(slug)
        if not candidate:
            continue
        if candidate in normalized_file_name or candidate.replace("-", "") in compact_file_name:
            if candidate != expected_token:
                return slug
    return None


def _detect_project_code_from_csv_sample(file_name: str, file_bytes: bytes, sample_limit: int = 200) -> dict:
    if not str(file_name or "").lower().endswith(".csv"):
        return {
            "topProjectCode": None,
            "topShare": 0.0,
            "sampleSize": 0,
            "totalNonEmpty": 0,
            "isMixed": False,
            "insufficientEvidence": True,
            "error": None,
        }

    header_aliases = {
        "pagoprjcode": "projectcode",
        "projectcode": "projectcode",
    }

    def normalize_header(header_value: str | None) -> str:
        return re.sub(r"[^a-z0-9]", "", str(header_value or "").replace("\ufeff", "").strip().lower())

    try:
        decoded = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return {
            "topProjectCode": None,
            "topShare": 0.0,
            "sampleSize": 0,
            "totalNonEmpty": 0,
            "isMixed": False,
            "insufficientEvidence": True,
            "error": "decode_error",
        }

    reader = csv.reader(decoded.splitlines())
    rows = list(reader)
    if not rows:
        return {
            "topProjectCode": None,
            "topShare": 0.0,
            "sampleSize": 0,
            "totalNonEmpty": 0,
            "isMixed": False,
            "insufficientEvidence": True,
            "error": None,
        }

    header_row = rows[0]
    project_code_idx = None
    for idx, header in enumerate(header_row):
        canonical = header_aliases.get(normalize_header(header))
        if canonical == "projectcode":
            project_code_idx = idx
            break

    if project_code_idx is None:
        return {
            "topProjectCode": None,
            "topShare": 0.0,
            "sampleSize": min(sample_limit, max(0, len(rows) - 1)),
            "totalNonEmpty": 0,
            "isMixed": False,
            "insufficientEvidence": True,
            "error": None,
        }

    frequencies: dict[str, int] = {}
    sample_size = 0
    for row in rows[1 : sample_limit + 1]:
        sample_size += 1
        value = row[project_code_idx] if project_code_idx < len(row) else ""
        normalized = _normalize_project_code_for_guardrail(value)
        if not normalized:
            continue
        frequencies[normalized] = frequencies.get(normalized, 0) + 1

    total_non_empty = sum(frequencies.values())
    if total_non_empty == 0:
        return {
            "topProjectCode": None,
            "topShare": 0.0,
            "sampleSize": sample_size,
            "totalNonEmpty": 0,
            "isMixed": False,
            "insufficientEvidence": True,
            "error": None,
        }

    top_code, top_count = max(frequencies.items(), key=lambda item: item[1])
    top_share = top_count / total_non_empty
    return {
        "topProjectCode": top_code,
        "topShare": top_share,
        "sampleSize": sample_size,
        "totalNonEmpty": total_non_empty,
        "isMixed": top_share < 0.5,
        "insufficientEvidence": total_non_empty < 10,
        "error": None,
    }


def evaluate_manual_import_project_guardrail(project_id: str, file_name: str, file_bytes: bytes) -> dict:
    project_doc = db.projects.find_one({"_id": ObjectId(project_id)}, {"name": 1, "slug": 1, "sap": 1}) or {}
    expected_name = str(project_doc.get("name") or "").strip()
    expected_slug = str(project_doc.get("slug") or "").strip()
    sap_doc = project_doc.get("sap") if isinstance(project_doc.get("sap"), dict) else {}
    expected_project_code = _normalize_project_code_for_guardrail(
        sap_doc.get("projectCode") or sap_doc.get("sapName") or expected_name
    )

    all_project_slugs = [
        str(row.get("slug") or "").strip()
        for row in db.projects.find({}, {"slug": 1})
        if str(row.get("slug") or "").strip()
    ]
    by_filename_slug = _extract_filename_slug_match(file_name=file_name, expected_slug=expected_slug, all_project_slugs=all_project_slugs)
    content_detection = _detect_project_code_from_csv_sample(file_name=file_name, file_bytes=file_bytes)
    top_project_code = _normalize_project_code_for_guardrail(content_detection.get("topProjectCode"))
    top_share = float(content_detection.get("topShare") or 0.0)
    insufficient_evidence = bool(content_detection.get("insufficientEvidence"))

    mismatch_level = "none"
    reason = "none"
    if top_project_code and expected_project_code and top_project_code != expected_project_code:
        if not insufficient_evidence and top_share >= 0.8:
            mismatch_level = "strong"
            reason = "content_top_share"
        elif not insufficient_evidence and top_share >= 0.5:
            mismatch_level = "moderate"
            reason = "content_mixed_majority"
        elif by_filename_slug:
            mismatch_level = "moderate"
            reason = "filename_plus_weak_content"
    elif by_filename_slug:
        mismatch_level = "moderate"
        reason = "filename_slug"

    should_block = mismatch_level in {"strong", "moderate"}
    warning_soft = bool(by_filename_slug or content_detection.get("isMixed") or insufficient_evidence)

    return {
        "shouldBlock": should_block,
        "mismatchLevel": mismatch_level,
        "reason": reason,
        "warningSoft": warning_soft,
        "expectedProject": {
            "projectId": project_id,
            "name": expected_name,
            "slug": expected_slug,
            "sapProjectCode": expected_project_code,
        },
        "detected": {
            "byFilename": by_filename_slug,
            "byContentTopCode": top_project_code or None,
            "topShare": round(top_share, 4),
            "sampleSize": int(content_detection.get("sampleSize") or 0),
            "totalNonEmpty": int(content_detection.get("totalNonEmpty") or 0),
            "insufficientEvidence": insufficient_evidence,
            "mixed": bool(content_detection.get("isMixed")),
            "parseError": content_detection.get("error"),
        },
    }


@app.post("/api/import/sap-payments")
async def import_sap_payments(
    request: FastAPIRequest,
    file: UploadFile = File(...),
    project: str | None = None,
    projectId: str | None = None,
    force: int = 0,
    mode: str = "upsert",
    confirm_rebuild: int = 0,
    admin_user: dict = Depends(require_admin),
):
    file_name = file.filename or ""
    file_bytes = await file.read()
    resolved_project_id = resolve_sap_import_project_id(request, project_id_query=projectId, project_name_query=project)

    guardrail = evaluate_manual_import_project_guardrail(
        project_id=resolved_project_id,
        file_name=file_name,
        file_bytes=file_bytes,
    )

    if guardrail.get("shouldBlock") and force != 1:
        expected = guardrail.get("expectedProject") if isinstance(guardrail.get("expectedProject"), dict) else {}
        detected = guardrail.get("detected") if isinstance(guardrail.get("detected"), dict) else {}
        detected_project = detected.get("byContentTopCode") or detected.get("byFilename") or "otro proyecto"
        expected_project = expected.get("name") or expected.get("slug") or "proyecto actual"
        raise HTTPException(
            status_code=409,
            detail={
                "error": "PROJECT_MISMATCH",
                "message": f"Este archivo parece ser del proyecto {detected_project}, pero estás en {expected_project}.",
                "details": {
                    "expectedProject": expected,
                    "detected": detected,
                    "mismatchLevel": guardrail.get("mismatchLevel"),
                    "reason": guardrail.get("reason"),
                },
            },
        )

    summary = run_sap_import(
        file_name,
        file_bytes,
        project,
        force,
        project_id=resolved_project_id,
        source="sap-payments",
        mode=mode,
        confirm_rebuild=confirm_rebuild,
        allow_rebuild=admin_user["role"] == "SUPERADMIN",
    )

    import_run_id = str((summary or {}).get("importRunId") or "").strip()
    if import_run_id and ObjectId.is_valid(import_run_id):
        db.importRuns.update_one(
            {"_id": ObjectId(import_run_id)},
            {
                "$set": {
                    "uploadedFileName": file_name,
                    "guardrail": {
                        "expectedProjectCode": guardrail.get("expectedProject", {}).get("sapProjectCode"),
                        "detectedByFilenameSlug": guardrail.get("detected", {}).get("byFilename"),
                        "detectedTopProjectCode": guardrail.get("detected", {}).get("byContentTopCode"),
                        "topShare": guardrail.get("detected", {}).get("topShare"),
                        "sampleSize": guardrail.get("detected", {}).get("sampleSize"),
                        "forceUsed": bool(force == 1),
                        "mismatchDetected": bool(guardrail.get("shouldBlock")),
                        "mismatchLevel": guardrail.get("mismatchLevel"),
                        "reason": guardrail.get("reason"),
                    },
                    "mismatchOverride": bool(force == 1 and guardrail.get("shouldBlock")),
                    "mismatchDetected": {
                        "expectedProject": guardrail.get("expectedProject"),
                        "detected": guardrail.get("detected"),
                        "mismatchLevel": guardrail.get("mismatchLevel"),
                        "reason": guardrail.get("reason"),
                    },
                }
            },
        )

    return summary


@app.post("/api/cron/import/sap-payments")
def cron_import_sap_payments(
    project: str = "CALDERON DE LA BARCA",
    force: int = 0,
    mode: str = "upsert",
    _: dict = Depends(require_admin),
):
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


@app.post("/api/cron/import/sap-latest")
def cron_import_sap_latest(
    project: str = "CALDERON DE LA BARCA",
    force: int = 0,
    mode: str = "upsert",
    _: dict = Depends(require_admin),
):
    return handle_sap_latest_import(project=project, force=force, mode=mode, source="sap-latest-cron")


@app.post("/api/cron/import/sap-movements-by-sbo")
def cron_import_sap_movements_by_sbo(
    sbo: str,
    mode: str = "latest",
    force: int = 0,
    x_trigger_source: str | None = Header(default=None, alias="X-Trigger-Source"),
    user: dict = Depends(require_admin),
):
    trigger_source = _normalize_trigger_source(x_trigger_source)
    actor = str(user.get("displayName") or user.get("username") or "system").strip() or "system"
    try:
        result = import_sap_movements_by_sbo(
            sbo=sbo,
            mode=mode,
            force=force,
            trigger_source=trigger_source,
            actor=actor,
        )
        try:
            notify_sap_movements_by_sbo_result(
                sbo=sbo,
                mode=mode,
                trigger_source=trigger_source,
                actor=actor,
                result=result,
            )
        except Exception:
            logger.exception("SAP movements Telegram success notification failed sbo=%s mode=%s", sbo, mode)
        return result
    except Exception as exc:
        try:
            notify_sap_movements_by_sbo_error(
                sbo=sbo,
                mode=mode,
                trigger_source=trigger_source,
                actor=actor,
                exc=exc,
            )
        except Exception:
            logger.exception("SAP movements Telegram error notification failed sbo=%s mode=%s", sbo, mode)
        raise


def handle_sap_latest_import(
    project: str,
    force: int = 0,
    mode: str = "upsert",
    source: str = "sap-latest-cron",
    sources: list[str] | None = None,
):
    del sources
    now = datetime.now(timezone.utc).isoformat()
    try:
        result = run_s3_latest_sap_import(project=project, force=force, mode=mode, source=source)
        notify_sap_latest_import_success(project=project, result=result)
        print(f"sap_latest_import ok source={source} iva={result['iva']} efectivo={result['efectivo']}")
        return result
    except Exception as exc:
        notify_sap_latest_import_failure(project=project, exc=exc)
        error_hash = sha256(f"sap_latest_import_error:{source}:{now}:{str(exc)}".encode("utf-8")).hexdigest()
        import_run_id = db.importRuns.insert_one(
            {
                "sha256": error_hash,
                "fileName": None,
                "source": source,
                "projectId": None,
                "rowsTotal": 0,
                "rowsOk": 0,
                "rowsSkipped": 0,
                "rowsError": 1,
                "status": "failed",
                "startedAt": now,
                "finishedAt": datetime.now(timezone.utc).isoformat(),
                "errorsSample": [{"error": str(exc)}],
            }
        ).inserted_id
        print(f"sap_latest_import failed source={source} importRunId={import_run_id} error={str(exc)}")
        raise


def _normalize_sap_latest_sources(sources: list[str] | None) -> list[str]:
    if not isinstance(sources, list):
        return []
    normalized = []
    for source in sources:
        source_value = str(source or "").strip().upper()
        if source_value in ("IVA", "EFECTIVO") and source_value not in normalized:
            normalized.append(source_value)
    return normalized


def _sap_latest_result_summary(result: dict | None) -> dict:
    if not isinstance(result, dict):
        return {"already_imported": None, "importRunIds": []}

    import_run_ids = []
    already_imported = {}
    for label in ("iva", "efectivo"):
        bucket = result.get(label) if isinstance(result.get(label), dict) else {}
        run_id = str(bucket.get("importRunId") or "").strip()
        if run_id:
            import_run_ids.append(run_id)
        already_imported[label] = bool(bucket.get("already_imported"))

    return {"already_imported": already_imported, "importRunIds": import_run_ids}


def _build_supplier_summary_bucket_key(tx: dict, trusted_id_to_supplier_key: dict[str, str]) -> str:
    supplier_id = normalize_non_empty_string(tx.get("supplierId") or tx.get("supplier_id"))
    vendor_id = normalize_non_empty_string(tx.get("vendor_id"))
    identity = build_supplier_identity_from_transaction(tx)
    supplier_key = normalize_non_empty_string(identity.get("supplierKey"))

    if supplier_key:
        return supplier_key

    for candidate_id in (supplier_id, vendor_id):
        if candidate_id and trusted_id_to_supplier_key.get(candidate_id):
            return str(trusted_id_to_supplier_key.get(candidate_id))

    # When canonical supplier identity is unavailable, avoid collapsing distinct
    # vendors that share a legacy supplierId by keeping both IDs in the bucket key.
    if supplier_id and vendor_id:
        return f"supplier:{supplier_id}|vendor:{vendor_id}"
    if vendor_id:
        return f"vendor:{vendor_id}"
    if supplier_id:
        return f"supplier:{supplier_id}"
    return f"tx:{str(tx.get('_id') or '').strip()}"


def _build_trusted_id_supplier_key_map(movements: list[dict]) -> dict[str, str]:
    trusted_id_candidates: dict[str, set[str]] = {}
    for tx in movements:
        identity = build_supplier_identity_from_transaction(tx)
        supplier_key = normalize_non_empty_string(identity.get("supplierKey"))
        if not supplier_key or supplier_key.startswith("name:"):
            continue

        supplier_id = normalize_non_empty_string(tx.get("supplierId") or tx.get("supplier_id"))
        vendor_id = normalize_non_empty_string(tx.get("vendor_id"))
        for candidate_id in (supplier_id, vendor_id):
            if not candidate_id:
                continue
            trusted_id_candidates.setdefault(candidate_id, set()).add(supplier_key)

    return {
        supplier_id: next(iter(keys))
        for supplier_id, keys in trusted_id_candidates.items()
        if len(keys) == 1
    }


def _resolve_supplier_summary_display_name(tx: dict, identity: dict, suppliers_by_id: dict[str, dict]) -> str:
    supplier_id = normalize_non_empty_string(tx.get("supplierId") or tx.get("supplier_id"))
    supplier_doc = suppliers_by_id.get(supplier_id or "") if supplier_id else None
    catalog_name = normalize_non_empty_string((supplier_doc or {}).get("name"))
    supplier_name = normalize_non_empty_string(identity.get("supplierName"))
    sap_business_partner = normalize_non_empty_string(identity.get("businessPartner"))
    sap_card_code = normalize_non_empty_string(identity.get("supplierCardCode"))
    return catalog_name or supplier_name or sap_business_partner or sap_card_code or ""


@app.post("/api/admin/import/sap-latest")
def admin_import_sap_latest(payload: dict, user: dict = Depends(require_admin)):
    project_id = str((payload or {}).get("projectId") or "").strip()
    if not project_id:
        raise HTTPException(status_code=400, detail="projectId is required")
    if not ObjectId.is_valid(project_id):
        raise HTTPException(status_code=400, detail="Invalid projectId")

    project_doc = db.projects.find_one({"_id": ObjectId(project_id)}, {"name": 1})
    if not project_doc:
        raise HTTPException(status_code=404, detail="Project not found")

    sources = _normalize_sap_latest_sources((payload or {}).get("sources"))
    now = datetime.now(timezone.utc)

    with sap_latest_admin_guard:
        lock_is_active = bool(sap_latest_admin_locks.get(project_id))
        if lock_is_active:
            raise HTTPException(status_code=409, detail="Import already running")

        last_requested_at = sap_latest_admin_last_request_at.get(project_id)
        if isinstance(last_requested_at, datetime):
            elapsed = (now - last_requested_at).total_seconds()
            if elapsed < SAP_LATEST_ADMIN_RATE_LIMIT_SECONDS:
                raise HTTPException(status_code=429, detail="Too Many Requests")

        sap_latest_admin_locks[project_id] = True
        sap_latest_admin_last_request_at[project_id] = now

    requested_by = {
        "userId": user.get("username"),
        "email": user.get("username"),
        "displayName": user.get("displayName"),
    }
    audit_id = db.adminActions.insert_one(
        {
            "action": "sap_latest_import",
            "projectId": project_id,
            "sources": sources,
            "requestedBy": requested_by,
            "requestedAt": now.isoformat(),
            "status": "running",
        }
    ).inserted_id

    try:
        result = handle_sap_latest_import(
            project=str(project_doc.get("name") or ""),
            force=0,
            mode="upsert",
            source="sap-latest-admin",
            sources=sources,
        )
        db.adminActions.update_one(
            {"_id": audit_id},
            {
                "$set": {
                    "status": "success",
                    "finishedAt": datetime.now(timezone.utc).isoformat(),
                    "resultSummary": _sap_latest_result_summary(result),
                }
            },
        )
        return result
    except Exception as exc:
        db.adminActions.update_one(
            {"_id": audit_id},
            {
                "$set": {
                    "status": "error",
                    "finishedAt": datetime.now(timezone.utc).isoformat(),
                    "errorMessage": str(exc),
                }
            },
        )
        raise
    finally:
        with sap_latest_admin_guard:
            sap_latest_admin_locks.pop(project_id, None)


@app.get("/api/expenses/summary-by-supplier")
def summary_expenses_by_supplier(
    projectId: str | None = None,
    project: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    source: str | None = None,
    sourceDb: str | None = None,
    include_iva: bool = False,
    user: dict = Depends(require_authenticated),
):
    project_id = resolve_project_id(projectId or project)
    if not can_access_project(user, project_id):
        if is_viewer(user):
            return []
        raise HTTPException(status_code=403, detail="Project access denied")

    normalized_from_date = from_date if isinstance(from_date, str) else None
    normalized_to_date = to_date if isinstance(to_date, str) else None
    effective_date_from = normalized_from_date or date_from
    effective_date_to = normalized_to_date or date_to
    movements_query = build_transactions_query(
        type_value="EXPENSE",
        date_from=effective_date_from,
        date_to=effective_date_to,
        source=source,
        source_db=sourceDb,
    )
    movements_query = with_legacy_project_filter(movements_query, project_id)
    movements = list(
        db.transactions.find(
            movements_query,
            {
                "_id": 1,
                "source": 1,
                "supplierId": 1,
                "supplier_id": 1,
                "vendor_id": 1,
                "supplierName": 1,
                "sap.cardCode": 1,
                "sap.businessPartner": 1,
                "proveedorNombre": 1,
                "beneficiario": 1,
                "proveedor.name": 1,
                "amount": 1,
                "tax": 1,
            },
        )
    )

    supplier_ids = []
    for tx in movements:
        supplier_id = normalize_non_empty_string(tx.get("supplierId") or tx.get("supplier_id"))
        if supplier_id and ObjectId.is_valid(supplier_id):
            supplier_ids.append(ObjectId(supplier_id))

    suppliers_by_id = {}
    if supplier_ids:
        for supplier in db.suppliers.find({"_id": {"$in": supplier_ids}}, {"name": 1}):
            suppliers_by_id[str(supplier.get("_id"))] = supplier

    trusted_id_to_supplier_key = _build_trusted_id_supplier_key_map(movements)
    supplier_totals = {}
    for tx in movements:
        source = str(tx.get("source") or "").strip().lower()
        supplier_id = tx.get("supplierId") or tx.get("supplier_id")
        vendor_id = tx.get("vendor_id")
        identity = build_supplier_identity_from_transaction(tx)
        supplier_key = str(identity.get("supplierKey") or "").strip()
        supplier_name = str(identity.get("supplierName") or "").strip()
        sap_card_code = str(identity.get("supplierCardCode") or "").strip()
        sap_business_partner = str(identity.get("businessPartner") or "").strip()

        # Canonical key is preferred, but when canonical data is missing we bridge
        # through IDs only if that ID maps to exactly one non-name supplier key.
        provider_key = _build_supplier_summary_bucket_key(tx, trusted_id_to_supplier_key)

        display_name = _resolve_supplier_summary_display_name(tx, identity, suppliers_by_id)
        bucket = supplier_totals.setdefault(
            provider_key,
            {
                "supplierId": str(supplier_id).strip() if supplier_id else None,
                "vendorId": str(vendor_id).strip() if vendor_id else None,
                "supplierName": display_name,
                "supplierKey": supplier_key,
                "sapCardCode": sap_card_code,
                "sapBusinessPartner": sap_business_partner,
                "source": source,
                "totalAmount": 0.0,
                "count": 0,
            },
        )

        stable_name = _resolve_supplier_summary_display_name(tx, identity, suppliers_by_id)
        if stable_name and (not bucket.get("supplierName") or bucket.get("supplierName") == bucket.get("sapCardCode")):
            bucket["supplierName"] = stable_name
        if not bucket.get("sapBusinessPartner") and sap_business_partner:
            bucket["sapBusinessPartner"] = sap_business_partner
        if not bucket.get("sapCardCode") and sap_card_code:
            bucket["sapCardCode"] = sap_card_code

        # Keep original IDs only when missing; identity key is canonical for grouping.
        if not bucket.get("supplierId") and supplier_id:
            bucket["supplierId"] = str(supplier_id).strip()
        if not bucket.get("vendorId") and vendor_id:
            bucket["vendorId"] = str(vendor_id).strip()

        amount_value = float(tx.get("amount") or 0)
        bucket["totalAmount"] += amount_value if include_iva else compute_monto_sin_iva(tx)
        bucket["count"] += 1

    rows = [
        {
            "_id": provider_id,
            "supplierId": values.get("supplierId"),
            "vendorId": values.get("vendorId"),
            "supplierName": values.get("supplierName") or "",
            "supplierKey": values.get("supplierKey") or "",
            "sapCardCode": values.get("sapCardCode") or "",
            "sapBusinessPartner": values.get("sapBusinessPartner") or "",
            "source": values.get("source") or "",
            "totalAmount": round(values["totalAmount"], 2),
            "count": values["count"],
        }
        for provider_id, values in supplier_totals.items()
    ]

    supplier_ids = [oid(row.get("supplierId")) for row in rows if row.get("supplierId") and ObjectId.is_valid(str(row.get("supplierId") or ""))]
    vendor_ids = [oid(row.get("vendorId")) for row in rows if row.get("vendorId") and ObjectId.is_valid(str(row.get("vendorId") or ""))]
    supplier_card_codes = [str(row.get("sapCardCode") or "").strip() for row in rows if str(row.get("sapCardCode") or "").strip()]

    supplier_names_by_id = {}
    if supplier_ids:
        for supplier in db.suppliers.find({"_id": {"$in": supplier_ids}}, {"name": 1}):
            supplier_names_by_id[str(supplier["_id"])] = supplier.get("name") or "(Sin proveedor)"

    supplier_names_by_card_code = {}
    if supplier_card_codes:
        for supplier in db.suppliers.find({"cardCode": {"$in": supplier_card_codes}}, {"name": 1, "cardCode": 1}):
            card_code = str(supplier.get("cardCode") or "").strip()
            if card_code and card_code not in supplier_names_by_card_code:
                supplier_names_by_card_code[card_code] = supplier.get("name") or card_code

    vendor_names = {}
    if vendor_ids:
        for vendor in db.vendors.find({"_id": {"$in": vendor_ids}, "projectId": project_id}, {"name": 1}):
            vendor_names[str(vendor["_id"])] = vendor.get("name") or "(Sin proveedor)"

    output = []
    for row in rows:
        provider_key = str(row.get("_id") or "")
        supplier_name = row.get("supplierName") or ""
        card_code = str(row.get("sapCardCode") or "").strip()

        # For canonical keys (cardcode/bp/name), prefer identity-based naming so
        # legacy supplierId defaults do not relabel providers (e.g. default vendor debt).
        if provider_key.startswith(("bpcc:", "cardcode:", "bp:", "name:")):
            resolved_name = (
                supplier_name
                or row.get("sapBusinessPartner")
                or supplier_names_by_card_code.get(card_code)
                or card_code
                or "(Sin proveedor)"
            )
            resolved_supplier_id = row.get("supplierId")
        else:
            resolved_name = (
                supplier_names_by_id.get(row.get("supplierId"))
                or vendor_names.get(row.get("vendorId"))
                or supplier_names_by_card_code.get(card_code)
                or supplier_name
                or row.get("sapBusinessPartner")
                or card_code
                or "(Sin proveedor)"
            )
            resolved_supplier_id = row.get("supplierId") or row.get("vendorId")

        output.append(
            {
                "supplierId": resolved_supplier_id,
                "supplierName": resolved_name,
                "totalAmount": round(float(row.get("totalAmount") or 0), 2),
                "count": int(row.get("count") or 0),
            }
        )

    for row, item in zip(rows, output):
        logger.info(
            "[summary-by-supplier][debug] provider_key=%s supplierName=%s source=%s count=%s total=%s",
            row.get("_id"),
            item.get("supplierName"),
            row.get("source"),
            item.get("count"),
            item.get("totalAmount"),
        )

    output.sort(key=lambda item: (item["supplierName"] or "").lower())
    return output


@app.get("/api/admin/trabajos-especiales/suppliers")
def admin_trabajos_especiales_suppliers(_: dict = Depends(require_admin)):
    tx_query = {
        "$or": [
            {"categoryEffectiveName": {"$exists": True, "$nin": [None, ""]}},
            {"categoryManualName": {"$exists": True, "$nin": [None, ""]}},
            {"categoryHintName": {"$exists": True, "$nin": [None, ""]}},
        ]
    }
    projection = {
        "description": 1,
        "categoryEffectiveName": 1,
        "categoryManualName": 1,
        "categoryHintName": 1,
        "supplierId": 1,
        "supplierName": 1,
        "supplierCardCode": 1,
        "sap.cardCode": 1,
        "sap.businessPartner": 1,
        "projectId": 1,
        "project": 1,
        "date": 1,
    }

    projects_by_id = {
        str(project.get("_id")): project
        for project in db.projects.find({}, {"name": 1, "displayName": 1, "sap.sourceSbo": 1})
    }
    supplier_rules = list(db.supplierCategory2Rules.find({"isActive": {"$ne": False}}, {"supplierKey": 1, "category2Id": 1, "category2Name": 1}))
    rules_by_key = {str(rule.get("supplierKey") or "").strip(): rule for rule in supplier_rules if str(rule.get("supplierKey") or "").strip()}

    grouped = {}
    for tx in db.transactions.find(tx_query, projection):
        category_manual_name = normalize_non_empty_string(tx.get("categoryManualName"))
        category_hint_name = normalize_non_empty_string(tx.get("categoryHintName"))
        category_effective_name = normalize_non_empty_string(tx.get("categoryEffectiveName")) or build_effective_category_fields(
            None,
            category_manual_name,
            None,
            category_hint_name,
        ).get("categoryEffectiveName")
        if not category_effective_name:
            continue

        normalized_effective_category = normalize_text_for_matching(category_effective_name)
        if not normalized_effective_category.startswith(TRABAJOS_ESPECIALES_PREFIX):
            continue

        description = str(tx.get("description") or "").strip()
        identity = build_supplier_identity_from_transaction(tx)
        supplier_key = identity.get("supplierKey") or f"tx:{str(tx.get('_id') or '')}"

        project_id = str(tx.get("projectId") or "").strip()
        project_doc = projects_by_id.get(project_id) if project_id else None
        project_name = (
            str((project_doc or {}).get("displayName") or (project_doc or {}).get("name") or tx.get("project") or project_id or "")
            .strip()
        )
        source_sbo = str(((project_doc or {}).get("sap") or {}).get("sourceSbo") or "").strip()

        bucket = grouped.setdefault(
            supplier_key,
            {
                "supplierKey": supplier_key,
                "supplierId": str(tx.get("supplierId") or "").strip(),
                "supplierName": identity.get("supplierName") or "(Sin proveedor)",
                "supplierCardCode": identity.get("supplierCardCode") or "",
                "businessPartner": identity.get("businessPartner") or "",
                "transactionCount": 0,
                "_projectKeys": set(),
                "projects": [],
                "sampleDescriptions": [],
                "_sampleSeen": set(),
                "matchedCategories": [],
                "_matchedCategorySeen": set(),
                "_lastSeenDate": None,
            },
        )

        bucket["transactionCount"] += 1

        project_key = project_id or project_name
        if project_key and project_key not in bucket["_projectKeys"]:
            bucket["_projectKeys"].add(project_key)
            bucket["projects"].append(
                {
                    "projectId": project_id,
                    "projectName": project_name,
                    "sourceSbo": source_sbo,
                }
            )

        normalized_category_sample = normalize_text_for_matching(category_effective_name)
        if (
            normalized_category_sample
            and normalized_category_sample not in bucket["_matchedCategorySeen"]
            and len(bucket["matchedCategories"]) < 5
        ):
            bucket["_matchedCategorySeen"].add(normalized_category_sample)
            bucket["matchedCategories"].append(category_effective_name)

        normalized_sample = normalize_text_for_matching(description)
        if normalized_sample and normalized_sample not in bucket["_sampleSeen"] and len(bucket["sampleDescriptions"]) < 5:
            bucket["_sampleSeen"].add(normalized_sample)
            bucket["sampleDescriptions"].append(description)

        tx_date = normalizeDate(tx.get("date"))
        if tx_date and (bucket["_lastSeenDate"] is None or tx_date > bucket["_lastSeenDate"]):
            bucket["_lastSeenDate"] = tx_date

    suppliers = []
    for values in grouped.values():
        matching_rule = rules_by_key.get(values.get("supplierKey") or "")
        assigned_category2_name = normalize_non_empty_string((matching_rule or {}).get("category2Name"))
        suppliers.append(
            {
                "supplierKey": values.get("supplierKey") or "",
                "supplierId": values.get("supplierId") or "",
                "supplierName": values.get("supplierName") or "(Sin proveedor)",
                "supplierCardCode": values.get("supplierCardCode") or "",
                "businessPartner": values.get("businessPartner") or "",
                "transactionCount": int(values.get("transactionCount") or 0),
                "projectCount": len(values.get("_projectKeys") or set()),
                "projects": sorted(
                    values.get("projects") or [],
                    key=lambda item: (str(item.get("projectName") or "").lower(), str(item.get("projectId") or "")),
                ),
                "matchedCategories": values.get("matchedCategories") or [],
                "sampleDescriptions": values.get("sampleDescriptions") or [],
                "lastSeenAt": values.get("_lastSeenDate").isoformat() if values.get("_lastSeenDate") else None,
                "category2Rule": {
                    "category2Id": normalize_non_empty_string((matching_rule or {}).get("category2Id")),
                    "category2Name": assigned_category2_name,
                    "status": "assigned" if assigned_category2_name else "unclassified",
                },
            }
        )

    suppliers.sort(key=lambda item: ((item.get("supplierName") or "").lower(), item.get("supplierCardCode") or "", item.get("supplierKey") or ""))
    return {
        "items": suppliers,
        "totalSuppliers": len(suppliers),
        "matchingPrefix": TRABAJOS_ESPECIALES_PREFIX,
    }


@app.get("/api/admin/trabajos-especiales/supplier-category2-rules")
def list_admin_trabajos_especiales_supplier_category2_rules(_: dict = Depends(require_admin)):
    rows = list(db.supplierCategory2Rules.find({}).sort([("updatedAt", -1), ("supplierName", 1)]))
    return {"items": [serialize_raw_doc(row) for row in rows]}


@app.get("/api/admin/categories/global")
def list_admin_global_categories(_: dict = Depends(require_admin)):
    ensure_global_categories_catalog()
    rows = list(db.categories.find({"active": {"$ne": False}}))
    deduped_by_key: dict[str, dict] = {}

    for row in rows:
        name = clean_global_category_name(row.get("name"))
        key = normalize_global_category_key(name)
        if not key:
            continue
        current = deduped_by_key.get(key)
        if current is None or str(row.get("_id")) < str(current.get("_id")):
            deduped_by_key[key] = row

    ordered_rows = sorted(
        deduped_by_key.values(),
        key=lambda row: (normalize_global_category_key(row.get("name")), str(row.get("_id") or "")),
    )
    return [serialize(row) for row in ordered_rows]


@app.put("/api/admin/trabajos-especiales/supplier-category2-rules")
def upsert_admin_trabajos_especiales_supplier_category2_rule(payload: dict, user: dict = Depends(require_admin)):
    category2_id = normalize_non_empty_string(payload.get("category2Id"))
    if not category2_id or not ObjectId.is_valid(category2_id):
        raise HTTPException(status_code=400, detail="category2Id is required")

    category = db.categories.find_one({"_id": ObjectId(category2_id), "active": {"$ne": False}}, {"name": 1})
    if not category:
        raise HTTPException(status_code=400, detail="Invalid category2Id")

    supplier_key = build_supplier_key(
        payload.get("supplierCardCode"),
        payload.get("businessPartner"),
        payload.get("supplierName"),
    )
    if not supplier_key:
        raise HTTPException(status_code=400, detail="supplierKey could not be resolved from supplierCardCode/businessPartner/supplierName")

    now_iso = datetime.now(timezone.utc).isoformat()
    actor = normalize_non_empty_string((user or {}).get("username")) or normalize_non_empty_string((user or {}).get("email")) or "system"
    updates = {
        "supplierKey": supplier_key,
        "supplierName": normalize_non_empty_string(payload.get("supplierName")) or "(Sin proveedor)",
        "supplierCardCode": normalize_non_empty_string(payload.get("supplierCardCode")),
        "businessPartner": normalize_non_empty_string(payload.get("businessPartner")),
        "category2Id": str(category.get("_id")),
        "category2Name": normalize_non_empty_string(category.get("name")) or category2_id,
        "updatedAt": now_iso,
        "updatedBy": actor,
        "isActive": bool(payload.get("isActive", True)),
    }

    db.supplierCategory2Rules.update_one(
        {"supplierKey": supplier_key},
        {
            "$set": updates,
            "$setOnInsert": {
                "createdAt": now_iso,
                "createdBy": actor,
            },
        },
        upsert=True,
    )

    saved = db.supplierCategory2Rules.find_one({"supplierKey": supplier_key})
    return serialize_raw_doc(saved) if saved else {"ok": True, "supplierKey": supplier_key}


@app.delete("/api/admin/trabajos-especiales/supplier-category2-rules/{supplier_key}")
def deactivate_admin_trabajos_especiales_supplier_category2_rule(supplier_key: str, user: dict = Depends(require_admin)):
    normalized_key = str(supplier_key or "").strip()
    if not normalized_key:
        raise HTTPException(status_code=400, detail="supplier_key is required")

    now_iso = datetime.now(timezone.utc).isoformat()
    actor = normalize_non_empty_string((user or {}).get("username")) or normalize_non_empty_string((user or {}).get("email")) or "system"

    result = db.supplierCategory2Rules.update_one(
        {"supplierKey": normalized_key},
        {"$set": {"isActive": False, "updatedAt": now_iso, "updatedBy": actor}},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"ok": True, "supplierKey": normalized_key}


@app.post("/api/admin/migrate/projectId")
def migrate_transactions_project_id(_: dict = Depends(require_admin)):
    default_project_id = resolve_project_id()

    copied_result = db.transactions.update_many(
        {
            "projectId": {"$exists": False},
            "sap.projectId": {"$exists": True, "$nin": [None, ""]},
        },
        [{"$set": {"projectId": "$sap.projectId"}}],
    )

    defaulted_result = db.transactions.update_many(
        {
            "$and": [
                {"$or": [{"projectId": {"$exists": False}}, {"projectId": None}, {"projectId": ""}]},
                {"$or": [{"sap.projectId": {"$exists": False}}, {"sap.projectId": None}, {"sap.projectId": ""}]},
            ]
        },
        {"$set": {"projectId": default_project_id}},
    )

    return {
        "defaultProjectId": default_project_id,
        "copiedFromSapProjectId": copied_result.modified_count,
        "defaultedProjectId": defaulted_result.modified_count,
    }


# Manual test (cURL):
# curl -X POST "http://localhost:8000/api/admin/migrate/projectId" -H "Authorization: Bearer <ADMIN_TOKEN>"
@app.post("/api/admin/sap/dedupe-migration")
def admin_run_sap_dedupe_migration(_: dict = Depends(require_admin)):
    return run_sap_manual_migration_and_dedupe()


@app.post("/api/admin/backfill/suppliers-to-vendors")
def backfill_suppliers_to_vendors(project: str = "CALDERON DE LA BARCA", _: dict = Depends(require_admin)):
    project_id = get_or_create_project_id(project)
    suppliers = list(
        db.suppliers.find(
            {"$or": [{"projectId": project_id}, {"projectIds": project_id}]},
            {"cardCode": 1, "name": 1},
        )
    )
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


@app.post("/api/admin/dedupe/sap-transactions")
def dedupe_sap_transactions_endpoint(_: dict = Depends(require_admin)):
    return dedupe_sap_transactions(dry_run=False)


@app.post("/api/admin/sap/dedupe")
def admin_dedupe_sap_transactions(
    projectId: str = Query(..., min_length=1),
    mode: str = "bySapKey",
    dryRun: bool = True,
    _: dict = Depends(require_admin),
):
    normalized_mode = (mode or "").strip()
    if normalized_mode != "bySapKey":
        raise HTTPException(status_code=400, detail="Invalid mode. Use 'bySapKey'.")
    return dedupe_sap_transactions(project_id=projectId, dry_run=dryRun)


@app.post("/api/admin/sap/cleanup-iva-duplicates")
def admin_cleanup_sap_iva_duplicates(
    projectId: str = Query(..., min_length=1),
    dryRun: bool = True,
    _: dict = Depends(require_admin),
):
    return cleanup_sap_iva_duplicates(project_id=projectId, dry_run=dryRun)


@app.post("/api/admin/telegram/test")
def admin_test_telegram(message: str = "✅ test telegram desde backend", user: dict = Depends(require_admin)):
    delivery = send_telegram_broadcast(message)
    logger.info(
        "Admin telegram test requested by %s sent=%s/%s failed=%s",
        user.get("username"),
        delivery.get("sent"),
        delivery.get("total"),
        delivery.get("failed"),
    )
    return {"ok": delivery.get("failed", 0) == 0, **delivery}


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
    created_categories = 0

    for category_name in DEFAULT_CATEGORIES:
        normalized_name = normalize_category_name(category_name)
        result = cats.update_one(
            {
                "$or": [
                    {"nameKey": normalized_name},
                    {"name": {"$regex": f"^{re.escape(category_name)}$", "$options": "i"}},
                ]
            },
            {
                "$set": {"nameKey": normalized_name},
                "$setOnInsert": {
                    "name": category_name,
                    "active": True,
                },
            },
            upsert=True,
        )
        if result.upserted_id:
            created_categories += 1

    return {"created_categories": created_categories}


# ---------- categories ----------
@app.get("/api/categories")
@app.get("/categories")
def list_categories(active_only: bool = True, request: FastAPIRequest = None, user: dict = Depends(require_authenticated)):
    active_project_id = get_active_project_id(request)
    if not can_access_project(user, active_project_id):
        if is_viewer(user):
            return []
        raise HTTPException(status_code=403, detail="Project access denied")

    q = {"$or": [{"projectId": active_project_id}, {"projectIds": active_project_id}]}
    if active_only:
        q["active"] = True

    categories = [serialize(c) for c in db.categories.find(q).sort("name", 1)]
    combined = []
    seen_keys = set()

    for category in categories:
        cat_id = str(category.get("id") or "").strip()
        name = normalize_non_empty_string(category.get("name"))
        code = normalize_non_empty_string(category.get("code")) or cat_id
        if not name:
            continue
        key = f"code:{code}" if code else f"name:{normalize_category_name(name)}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        combined.append({
            **category,
            "code": code,
            "name": name,
            "source": category.get("source") or "catalog",
            "displayLabel": f"{name} ({code})" if code and code != name else name,
        })

    tx_query = {"projectId": active_project_id}
    projection = {
        "categoryManualCode": 1,
        "categoryManualName": 1,
        "categoryHintCode": 1,
        "categoryHintName": 1,
    }
    for tx in db.transactions.find(tx_query, projection):
        for source, code_key, name_key in (
            ("manual", "categoryManualCode", "categoryManualName"),
            ("sap", "categoryHintCode", "categoryHintName"),
        ):
            code = normalize_non_empty_string(tx.get(code_key))
            name = normalize_non_empty_string(tx.get(name_key))
            if not code and not name:
                continue
            normalized_name = normalize_category_name(name or code or "")
            key = f"code:{code}" if code else f"name:{normalized_name}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            option_value = code or name
            combined.append({
                "id": f"{source}:{option_value}",
                "name": name or code,
                "code": code,
                "source": source,
                "displayLabel": f"{name or code} ({code})" if code and name and code != name else (name or code),
            })

    combined.sort(key=lambda c: normalize_category_name(str(c.get("name") or "")))
    return combined


@app.post("/api/categories")
@app.post("/categories")
def create_category(payload: dict, request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    name = (payload.get("name") or "").strip()
    if len(name) < 2:
        raise HTTPException(status_code=400, detail="name is required")

    # Categoría 2 manual: si ya existe globalmente por nombre, reutilizarla y
    # asociarla al proyecto activo para que quede compartida entre proyectos.
    existing = db.categories.find_one({"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}})
    if existing:
        update_ops = {
            "$set": {
                "active": True,
                "projectId": existing.get("projectId") or active_project_id,
            },
            "$addToSet": {"projectIds": active_project_id},
        }
        db.categories.update_one({"_id": existing["_id"]}, update_ops)
        return serialize(db.categories.find_one({"_id": existing["_id"]}))

    all_project_ids = [str(project.get("_id")) for project in db.projects.find({}, {"_id": 1}) if project.get("_id")]
    if active_project_id not in all_project_ids:
        all_project_ids.append(active_project_id)

    _id = db.categories.insert_one({
        "name": name,
        "active": True,
        "projectId": active_project_id,
        "projectIds": all_project_ids,
    }).inserted_id
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
    request: FastAPIRequest = None,
    user: dict = Depends(require_authenticated),
):
    active_project_id = get_active_project_id(request)
    if not can_access_project(user, active_project_id):
        if is_viewer(user):
            return []
        raise HTTPException(status_code=403, detail="Project access denied")

    q = {"projectId": active_project_id, "active": True} if active_only else {"projectId": active_project_id}
    if category_id:
        q["category_ids"] = category_id
    if include_sap:
        q["$or"] = [{"source": {"$exists": False}}, {"source": "manual"}, {"source": "sap"}]
    else:
        q["$or"] = [{"source": {"$exists": False}}, {"source": "manual"}]

    vendors = [serialize(v) for v in db.vendors.find(q).sort("name", 1)]
    if not include_sap or category_id:
        return vendors

    seen_keys = set()
    for vendor in vendors:
        source = str(vendor.get("source") or "").strip().lower()
        card_code = str(vendor.get("supplierCardCode") or vendor.get("cardCode") or "").strip().upper()
        name = str(vendor.get("name") or "").strip().lower()
        if card_code:
            seen_keys.add(f"card:{card_code}")
        if name:
            seen_keys.add(f"name:{name}")
        if source == "sap-sbo":
            project_key = str(vendor.get("projectId") or "").strip()
            if card_code:
                seen_keys.add(f"sap-sbo:{project_key}:card:{card_code}")
            elif name:
                seen_keys.add(f"sap-sbo:{project_key}:name:{name}")

    projection = {"supplierName": 1, "sap.cardCode": 1, "sap.businessPartner": 1, "projectId": 1}
    tx_query = {"projectId": active_project_id, "source": "sap-sbo"}
    for tx in db.transactions.find(tx_query, projection):
        project_id = str(tx.get("projectId") or "").strip()
        sap_doc = tx.get("sap") if isinstance(tx.get("sap"), dict) else {}
        card_code = str(sap_doc.get("cardCode") or "").strip()
        supplier_name = str(tx.get("supplierName") or "").strip() or str(sap_doc.get("businessPartner") or "").strip()
        if not supplier_name and not card_code:
            continue

        stable_key = card_code or supplier_name.lower()
        seen_key = f"sap-sbo:{project_id}:{stable_key}"
        if seen_key in seen_keys:
            continue

        vendors.append(
            {
                "id": f"sap-sbo:{project_id}:{stable_key}",
                "projectId": project_id,
                "name": supplier_name or card_code,
                "cardCode": card_code or None,
                "supplierCardCode": card_code or None,
                "source": "sap-sbo",
                "active": True,
                "category_ids": [],
            }
        )
        seen_keys.add(seen_key)
        if card_code:
            seen_keys.add(f"card:{card_code.upper()}")
        if supplier_name:
            seen_keys.add(f"name:{supplier_name.lower()}")

    vendors.sort(key=lambda v: normalize_category_name(str(v.get("name") or "")))
    return vendors


@app.post("/vendors")
def create_vendor(payload: dict, request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    name = (payload.get("name") or "").strip()
    if len(name) < 2:
        raise HTTPException(status_code=400, detail="name is required")
    if db.vendors.find_one({"name": name, "projectId": active_project_id}):
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
        "projectId": active_project_id,
    }
    _id = db.vendors.insert_one(doc).inserted_id
    return serialize(db.vendors.find_one({"_id": _id}))


@app.patch("/vendors/{vendor_id}")
def update_vendor(vendor_id: str, payload: dict, request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    vendor_filter = {"_id": oid(vendor_id), "projectId": active_project_id}
    vendor = db.vendors.find_one(vendor_filter)
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")

    updates = {}
    if "name" in payload:
        name = (payload.get("name") or "").strip()
        if len(name) < 2:
            raise HTTPException(status_code=400, detail="name is required")
        dup = db.vendors.find_one({"name": name, "projectId": active_project_id, "_id": {"$ne": oid(vendor_id)}})
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

    db.vendors.update_one(vendor_filter, {"$set": updates})
    return serialize(db.vendors.find_one(vendor_filter))


@app.delete("/vendors/{vendor_id}")
def delete_vendor(vendor_id: str, request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    vendor_filter = {"_id": oid(vendor_id), "projectId": active_project_id}
    result = db.vendors.delete_one(vendor_filter)
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Vendor not found")
    db.transactions.update_many({"vendor_id": vendor_id, "projectId": active_project_id}, {"$set": {"vendor_id": None}})
    return {"ok": True}


@app.post("/vendors/{vendor_id}/categories")
def set_vendor_categories(vendor_id: str, category_ids: list[str], request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    vendor_filter = {"_id": oid(vendor_id), "projectId": active_project_id}
    if not db.vendors.find_one(vendor_filter):
        raise HTTPException(status_code=404, detail="Vendor not found")
    for cid in category_ids:
        if not db.categories.find_one({"_id": oid(cid), "active": True}):
            raise HTTPException(status_code=400, detail=f"Invalid category_id: {cid}")
    db.vendors.update_one(vendor_filter, {"$set": {"category_ids": category_ids}})
    return serialize(db.vendors.find_one(vendor_filter))


# ---------- transactions ----------
@app.post("/transactions")
def create_transaction(
    payload: dict,
    request: FastAPIRequest,
    projectId: str | None = None,
    _: dict = Depends(require_admin),
):
    active_project_id = resolve_project_id(projectId)
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
    source_db = payload.get("sourceDb")
    if source_db is not None:
        source_db = str(source_db).strip().upper() or None
        if source_db not in ("IVA", "EFECTIVO"):
            raise HTTPException(status_code=400, detail="sourceDb must be IVA or EFECTIVO")

    if ttype == "EXPENSE":
        if not category_id or not vendor_id:
            raise HTTPException(status_code=400, detail="EXPENSE requires category_id and vendor_id")
        if not db.categories.find_one({"_id": oid(category_id), "active": True}):
            raise HTTPException(status_code=400, detail="Invalid category_id")
        if not db.vendors.find_one({"_id": oid(vendor_id), "active": True, "projectId": active_project_id}):
            raise HTTPException(status_code=400, detail="Invalid vendor_id")

    doc = {
        "type": ttype,
        "date": d,
        "amount": amount,
        "category_id": category_id,
        "categoryId": category_id,
        "category_auto_id": category_id,
        "category_override_id": category_id,
        "category_locked": bool(category_id),
        "category_source": "manual" if category_id else None,
        "categoryManualCode": category_id,
        "categoryManualName": None,
        "categoryManualUpdatedAt": datetime.now(timezone.utc).isoformat(),
        "categoryManualUpdatedBy": str(_.get("username") or _.get("email") or "system"),
        "vendor_id": vendor_id,
        "description": payload.get("description"),
        "reference": payload.get("reference"),
        "sourceDb": source_db,
        "created_at": datetime.utcnow().isoformat(),
        "projectId": active_project_id,
    }
    doc.update(build_effective_category_fields(doc.get("categoryManualCode"), doc.get("categoryManualName"), None, None))
    _id = db.transactions.insert_one(doc).inserted_id
    return serialize(db.transactions.find_one({"_id": _id}))


@app.patch("/api/transactions/{transaction_id}")
@app.patch("/transactions/{transaction_id}")
def update_transaction(transaction_id: str, payload: dict, request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    transaction_filter = {"_id": oid(transaction_id), "projectId": active_project_id}
    tx = db.transactions.find_one(transaction_filter)
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

    category_override_updates = normalize_category_override_update(
        payload,
        lock_override=("category_id" in payload or "categoryId" in payload),
        updated_by=str(_.get("username") or _.get("email") or "system"),
    )
    if category_override_updates:
        updates.update(category_override_updates)

    if "vendor_id" in payload:
        vid = payload.get("vendor_id")
        if vid and not db.vendors.find_one({"_id": oid(vid), "active": True, "projectId": active_project_id}):
            raise HTTPException(status_code=400, detail="Invalid vendor_id")
        updates["vendor_id"] = vid

    for field in ("description", "reference"):
        if field in payload:
            updates[field] = payload.get(field)

    if "sourceDb" in payload:
        source_db = payload.get("sourceDb")
        if source_db is None:
            updates["sourceDb"] = None
        else:
            normalized_source_db = str(source_db).strip().upper()
            if normalized_source_db not in ("IVA", "EFECTIVO"):
                raise HTTPException(status_code=400, detail="sourceDb must be IVA or EFECTIVO")
            updates["sourceDb"] = normalized_source_db

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    merged = dict(tx)
    merged.update(updates)
    if merged.get("type") == "EXPENSE" and (not merged.get("category_id") or not merged.get("vendor_id")):
        if tx.get("source") != "sap":
            raise HTTPException(status_code=400, detail="EXPENSE requires category_id and vendor_id")

    db.transactions.update_one(transaction_filter, {"$set": updates})

    category_id = updates.get("category_id")
    existing_category_id = tx.get("category_id") or tx.get("categoryId") or tx.get("categoryManualCode")
    should_apply_category_to_related = bool(category_id and not existing_category_id)

    if should_apply_category_to_related:
        propagation_updates = {
            "category_id": category_override_updates.get("category_id"),
            "categoryId": category_override_updates.get("categoryId"),
            "categoryManualCode": category_override_updates.get("categoryManualCode"),
            "categoryManualName": category_override_updates.get("categoryManualName"),
            "categoryManualUpdatedAt": category_override_updates.get("categoryManualUpdatedAt"),
            "categoryManualUpdatedBy": category_override_updates.get("categoryManualUpdatedBy"),
            "category_source": category_override_updates.get("category_source"),
            "category_locked": category_override_updates.get("category_locked"),
            "category_override_id": category_override_updates.get("category_override_id"),
            "categoryEffectiveCode": category_override_updates.get("categoryEffectiveCode"),
            "categoryEffectiveName": category_override_updates.get("categoryEffectiveName"),
        }
        uncategorized_filter = {
            "$and": [
                {
                    "$or": [
                        {"category_id": {"$exists": False}},
                        {"category_id": None},
                        {"category_id": ""},
                    ]
                },
                {
                    "$or": [
                        {"categoryManualCode": {"$exists": False}},
                        {"categoryManualCode": None},
                        {"categoryManualCode": ""},
                    ]
                },
            ]
        }

        supplier_id = tx.get("supplierId")
        if supplier_id:
            db.transactions.update_many(
                {
                    "supplierId": supplier_id,
                    "type": "EXPENSE",
                    "projectId": active_project_id,
                    **uncategorized_filter,
                },
                {"$set": propagation_updates},
            )
        elif tx.get("vendor_id"):
            db.transactions.update_many(
                {
                    "vendor_id": tx.get("vendor_id"),
                    "type": "EXPENSE",
                    "projectId": active_project_id,
                    **uncategorized_filter,
                },
                {"$set": propagation_updates},
            )

    return serialize(db.transactions.find_one(transaction_filter))


@app.patch("/api/projects/{project_id}/transactions/{transaction_id}")
def update_project_transaction(project_id: str, transaction_id: str, payload: dict, _: dict = Depends(require_admin)):
    transaction_filter = {"_id": oid(transaction_id), "projectId": project_id}
    tx = db.transactions.find_one(transaction_filter)
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")

    updates = normalize_category_override_update(
        payload,
        lock_override=True,
        updated_by=str(_.get("username") or _.get("email") or "system"),
    )
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    db.transactions.update_one(transaction_filter, {"$set": updates})

    category_id = updates.get("category_id")
    existing_category_id = tx.get("category_id") or tx.get("categoryId") or tx.get("categoryManualCode")
    should_apply_category_to_related = bool(category_id and not existing_category_id)

    if should_apply_category_to_related:
        propagation_updates = {
            "category_id": updates.get("category_id"),
            "categoryId": updates.get("categoryId"),
            "categoryManualCode": updates.get("categoryManualCode"),
            "categoryManualName": updates.get("categoryManualName"),
            "categoryManualUpdatedAt": updates.get("categoryManualUpdatedAt"),
            "categoryManualUpdatedBy": updates.get("categoryManualUpdatedBy"),
            "category_source": updates.get("category_source"),
            "category_locked": updates.get("category_locked"),
            "category_override_id": updates.get("category_override_id"),
            "categoryEffectiveCode": updates.get("categoryEffectiveCode"),
            "categoryEffectiveName": updates.get("categoryEffectiveName"),
        }
        uncategorized_filter = {
            "$and": [
                {
                    "$or": [
                        {"category_id": {"$exists": False}},
                        {"category_id": None},
                        {"category_id": ""},
                    ]
                },
                {
                    "$or": [
                        {"categoryManualCode": {"$exists": False}},
                        {"categoryManualCode": None},
                        {"categoryManualCode": ""},
                    ]
                },
            ]
        }

        supplier_id = tx.get("supplierId")
        if supplier_id:
            db.transactions.update_many(
                {
                    "supplierId": supplier_id,
                    "type": "EXPENSE",
                    "projectId": project_id,
                    **uncategorized_filter,
                },
                {"$set": propagation_updates},
            )
        elif tx.get("vendor_id"):
            db.transactions.update_many(
                {
                    "vendor_id": tx.get("vendor_id"),
                    "type": "EXPENSE",
                    "projectId": project_id,
                    **uncategorized_filter,
                },
                {"$set": propagation_updates},
            )

    return serialize(db.transactions.find_one(transaction_filter))


@app.post("/api/projects/{project_id}/transactions/bulk-update-category")
def bulk_update_project_transactions_category(project_id: str, payload: dict, _: dict = Depends(require_admin)):
    ids = payload.get("ids") or []
    raw_filter = payload.get("filter") or {}
    category_payload = {
        "category_id": payload.get("category_id", payload.get("categoryId")),
        "categoryManualCode": payload.get("categoryManualCode"),
        "categoryManualName": payload.get("categoryManualName"),
    }
    updates = normalize_category_override_update(
        category_payload,
        lock_override=True,
        updated_by=str(_.get("username") or _.get("email") or "system"),
    )
    if not updates:
        raise HTTPException(status_code=400, detail="categoryId/category_id is required")

    query = {"projectId": project_id}
    if ids:
        normalized_ids = []
        for tx_id in ids:
            if not ObjectId.is_valid(str(tx_id)):
                raise HTTPException(status_code=400, detail=f"Invalid transaction id: {tx_id}")
            normalized_ids.append(ObjectId(str(tx_id)))
        query["_id"] = {"$in": normalized_ids}
    elif isinstance(raw_filter, dict) and raw_filter:
        allowed_filters = ("sourceDb", "source", "supplierId", "vendor_id", "type")
        for key in allowed_filters:
            if key in raw_filter:
                query[key] = raw_filter.get(key)
    else:
        raise HTTPException(status_code=400, detail="Provide ids or filter")

    result = db.transactions.update_many(query, {"$set": updates})
    return {"matched": result.matched_count, "modified": result.modified_count}


@app.delete("/transactions/{transaction_id}")
def delete_transaction(transaction_id: str, request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    result = db.transactions.delete_one({"_id": oid(transaction_id), "projectId": active_project_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return {"ok": True}


# Manual test (cURL):
# curl -G "http://localhost:8000/api/transactions" --data-urlencode "projectId=<PROJECT_OBJECT_ID>" -H "Authorization: Bearer <TOKEN>"
# curl -G "http://localhost:8000/api/transactions" --data-urlencode "projectId=000000000000000000000000" -H "Authorization: Bearer <TOKEN>"  # debe responder 404 si no existe
# curl -G "http://localhost:8000/api/transactions" --data-urlencode "projectId=000" -H "Authorization: Bearer <TOKEN>"  # debe responder 400 Invalid projectId
# curl -G "http://localhost:8000/api/transactions" --data-urlencode "projectId=<PENSYLVANIA_PROJECT_OBJECT_ID>" -H "Authorization: Bearer <TOKEN>"  # puede responder 200 con items=[] si no hay import
# curl -G "http://localhost:8000/api/movimientos" -H "Authorization: Bearer <TOKEN>"  # usa DEFAULT_PROJECT_ID si no se envía projectId
# curl -G "http://localhost:8000/api/expenses/summary-by-supplier" --data-urlencode "projectId=<PROJECT_OBJECT_ID>" -H "Authorization: Bearer <TOKEN>"
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
    request: FastAPIRequest = None,
    project_id: str | None = Query(default=None, alias="projectId"),
    origen: str | None = None,
    source: str | None = None,
    sourceDb: str | None = None,
    q: str | None = None,
    page: int = 1,
    limit: int = 50,
    user: dict = Depends(require_authenticated),
):
    normalized_page = max(page, 1)
    resolved_project_id = resolve_project_id(project_id)
    logger.info("transactions projectId=%s resolved=%s", project_id, resolved_project_id)

    if not can_access_project(user, resolved_project_id):
        if is_viewer(user):
            return {
                "items": [],
                "page": normalized_page,
                "limit": min(max(limit, 1), 500),
                "totalCount": 0,
                "totals": {
                    "expensesGross": 0.0,
                    "expensesTax": 0.0,
                    "expensesWithoutTax": 0.0,
                    "incomeGross": 0.0,
                    "net": 0.0,
                },
            }
        raise HTTPException(status_code=403, detail="Project access denied")
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
        project_id=None,
        origen=origen,
        source=source,
        source_db=sourceDb,
        search_query=q,
    )
    match_query = with_legacy_project_filter(match_query, resolved_project_id)

    total_count = db.transactions.count_documents(match_query)
    totals = build_transaction_totals(match_query, search_query=q)
    logger.info("/transactions resolved projectId=%s totalCount=%s", resolved_project_id, total_count)

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

    supplier_rules = list(
        db.supplierCategory2Rules.find(
            {"isActive": {"$ne": False}},
            {
                "supplierKey": 1,
                "supplierCardCode": 1,
                "businessPartner": 1,
                "supplierName": 1,
                "category2Id": 1,
                "category2Name": 1,
            },
        )
    )
    supplier_rule_indexes = build_supplier_rule_indexes(supplier_rules)
    supplier_rules_by_key = supplier_rule_indexes.get("by_key", {})

    items = []
    for tx in txs:
        tx_doc = serialize_transaction_with_supplier(
            tx,
            suppliers_by_id,
            supplier_rules_by_key,
            supplier_rule_indexes=supplier_rule_indexes,
        )

        subtotal, iva, total_factura = resolve_transaction_tax_components(tx_doc)
        amount = parse_optional_decimal(tx_doc.get("amount")) or 0
        sign = -1 if amount < 0 else 1

        tx_doc["subtotal"] = sign * subtotal if subtotal is not None else None
        tx_doc["montoSinIva"] = tx_doc["subtotal"]
        tx_doc["iva"] = sign * iva if iva is not None else None
        tx_doc["montoIva"] = tx_doc["iva"]
        tx_doc["totalFactura"] = sign * total_factura if total_factura is not None else None
        tx_doc["tax"] = {
            "subtotal": tx_doc["subtotal"],
            "iva": tx_doc["iva"],
            "totalFactura": tx_doc["totalFactura"],
        }
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
    include_iva: bool = False,
    projectId: str | None = None,
    project: str | None = None,
    user: dict = Depends(require_authenticated),
):
    active_project_id = resolve_project_id(projectId or project)
    if not can_access_project(user, active_project_id):
        if is_viewer(user):
            return {"total_expenses": 0.0, "rows": []}
        raise HTTPException(status_code=403, detail="Project access denied")

    match = {"type": "EXPENSE", "projectId": active_project_id}
    if vendor_id:
        match["vendor_id"] = vendor_id
    if date_from or date_to:
        match["date"] = {}
        if date_from:
            match["date"]["$gte"] = date_from
        if date_to:
            match["date"]["$lte"] = date_to

    transactions = list(
        db.transactions.find(
            match,
            {
                "category_id": 1,
                "categoryId": 1,
                "categoryManualCode": 1,
                "categoryManualName": 1,
                "categoryHintCode": 1,
                "categoryHintName": 1,
                "categoryEffectiveCode": 1,
                "categoryEffectiveName": 1,
                "resolvedCategory2Id": 1,
                "resolvedCategory2Name": 1,
                "resolvedCategory2Source": 1,
                "supplierName": 1,
                "supplierCardCode": 1,
                "businessPartner": 1,
                "sap": 1,
                "amount": 1,
                "tax": 1,
            },
        )
    )

    supplier_rules = list(
        db.supplierCategory2Rules.find(
            {"isActive": {"$ne": False}},
            {
                "supplierKey": 1,
                "supplierCardCode": 1,
                "businessPartner": 1,
                "supplierName": 1,
                "category2Id": 1,
                "category2Name": 1,
            },
        )
    )
    supplier_rule_indexes = build_supplier_rule_indexes(supplier_rules)
    supplier_rules_by_key = supplier_rule_indexes.get("by_key", {})

    totals_by_category = {}
    for tx in transactions:
        category_manual_code = normalize_non_empty_string(tx.get("categoryManualCode"))
        category_manual_name = normalize_non_empty_string(tx.get("categoryManualName"))
        category_hint_code = normalize_non_empty_string(tx.get("categoryHintCode"))
        category_hint_name = normalize_non_empty_string(tx.get("categoryHintName"))
        effective = build_effective_category_fields(
            category_manual_code,
            category_manual_name,
            category_hint_code,
            category_hint_name,
        )
        category_effective_code = normalize_non_empty_string(tx.get("categoryEffectiveCode")) or effective.get("categoryEffectiveCode")
        category_effective_name = normalize_non_empty_string(tx.get("categoryEffectiveName")) or effective.get("categoryEffectiveName")
        if category_effective_code and not normalize_non_empty_string(tx.get("categoryEffectiveCode")):
            tx["categoryEffectiveCode"] = category_effective_code
        if category_effective_name and not normalize_non_empty_string(tx.get("categoryEffectiveName")):
            tx["categoryEffectiveName"] = category_effective_name

        resolved_category2_id = normalize_non_empty_string(tx.get("resolvedCategory2Id"))
        resolved_category2_name = normalize_non_empty_string(tx.get("resolvedCategory2Name"))
        resolved_category2_source = normalize_non_empty_string(tx.get("resolvedCategory2Source"))

        if not resolved_category2_id and not resolved_category2_name:
            tx.update(
                resolve_transaction_category2(
                    tx,
                    supplier_rules_by_key=supplier_rules_by_key,
                    supplier_rule_indexes=supplier_rule_indexes,
                )
            )
            resolved_category2_id = normalize_non_empty_string(tx.get("resolvedCategory2Id"))
            resolved_category2_name = normalize_non_empty_string(tx.get("resolvedCategory2Name"))
            resolved_category2_source = normalize_non_empty_string(tx.get("resolvedCategory2Source"))
        category_key = resolved_category2_id or resolved_category2_name or UNRESOLVED_CATEGORY2_ID
        category_display_name = resolved_category2_name or UNRESOLVED_CATEGORY2_NAME

        amount_value = float(tx.get("amount") or 0)
        movement_amount = amount_value if include_iva else compute_monto_sin_iva(tx)
        if category_key not in totals_by_category:
            totals_by_category[category_key] = {
                "amount": 0.0,
                "display_name": category_display_name,
                "resolved_category2_source": resolved_category2_source,
            }

        totals_by_category[category_key]["amount"] = round(totals_by_category[category_key]["amount"] + movement_amount, 2)
        if not totals_by_category[category_key].get("display_name") and category_display_name:
            totals_by_category[category_key]["display_name"] = category_display_name
        if not totals_by_category[category_key].get("resolved_category2_source") and resolved_category2_source:
            totals_by_category[category_key]["resolved_category2_source"] = resolved_category2_source

    rows = [
        {
            "_id": category_id,
            "amount": values.get("amount", 0.0),
            "display_name": values.get("display_name"),
            "resolved_category2_source": values.get("resolved_category2_source"),
        }
        for category_id, values in totals_by_category.items()
    ]
    rows.sort(key=lambda row: row["amount"], reverse=True)
    total = round(sum(float(r["amount"]) for r in rows), 2) if rows else 0.0

    cat_object_ids = []
    cat_codes = []
    for row in rows:
        raw_category_id = normalize_non_empty_string(row.get("_id"))
        if not raw_category_id:
            continue
        if ObjectId.is_valid(raw_category_id):
            cat_object_ids.append(ObjectId(raw_category_id))
        else:
            cat_codes.append(raw_category_id)

    cats = {}
    category_query = {"$or": []}
    if cat_object_ids:
        category_query["$or"].append({"_id": {"$in": cat_object_ids}})
    if cat_codes:
        category_query["$or"].append({"code": {"$in": cat_codes}})

    if category_query["$or"]:
        for c in db.categories.find(category_query, {"name": 1, "code": 1}):
            category_name = c.get("name") or c.get("code")
            if not category_name:
                continue

            category_id = c.get("_id")
            if category_id is not None:
                cats[str(category_id)] = category_name

            category_code = normalize_non_empty_string(c.get("code"))
            if category_code:
                cats[category_code] = category_name

    out = []
    for r in rows:
        cid = r["_id"]
        amt = float(r["amount"])
        out.append(
            {
                "category_id": cid,
                "category_name": r.get("display_name") or cats.get(cid, "(Sin categoría)"),
                "amount": round(amt, 2),
                "percent": round((amt / total * 100.0), 2) if total > 0 else 0.0,
                "resolvedCategory2Source": r.get("resolved_category2_source"),
            }
        )
    return {"total_expenses": round(total, 2), "rows": out}


if os.getenv("SKIP_STARTUP_INIT") != "1":
    ensure_indexes()
    ensure_default_users()
    ensure_telegram_admin_user()
