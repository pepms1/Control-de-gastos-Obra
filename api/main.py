from fastapi import FastAPI, HTTPException, Response, Depends, UploadFile, File, Query, Request as FastAPIRequest, Security, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, OperationFailure
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
logger = logging.getLogger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)


TELEGRAM_SETTINGS_KEY = "telegram_default_chat_id"
TELEGRAM_ACCESS_REQUEST_WINDOW_HOURS = 24


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
        default_admin_user: {"password": default_admin_pass, "role": "ADMIN", "displayName": default_admin_name},
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
        if not user.get("active", True):
            raise HTTPException(status_code=401, detail="User inactive or not found")

        role = user.get("role")
        if role not in ("ADMIN", "VIEWER"):
            raise HTTPException(status_code=401, detail="Invalid role")

        env_user = get_env_auth_users().get(username)
        display_name = (
            user.get("displayName")
            or (env_user or {}).get("displayName")
            or payload.get("displayName")
            or payload.get("name")
            or user["username"]
        )
        return {
            "username": user["username"],
            "role": role,
            "displayName": display_name,
            "active": user.get("active", True),
        }

    token_role = payload.get("role")
    env_user = get_env_auth_users().get(username)
    if not env_user:
        raise HTTPException(status_code=401, detail="User inactive or not found")

    role = env_user.get("role")
    if role not in ("ADMIN", "VIEWER") or token_role != role:
        raise HTTPException(status_code=401, detail="Invalid role")
    display_name = payload.get("displayName") or payload.get("name") or env_user.get("displayName") or username
    return {"username": username, "role": role, "displayName": display_name, "active": True}


def require_admin(user=Depends(role_from_token)):
    if user["role"] != "ADMIN":
        raise HTTPException(status_code=403, detail="ADMIN role required")
    return user


def require_authenticated(user=Depends(role_from_token)):
    return user


def get_active_project_id(request: FastAPIRequest) -> str:
    header_project_id = (request.headers.get("X-Project-Id") or "").strip()
    default_project_id = (os.getenv("DEFAULT_PROJECT_ID") or "").strip()

    active_project_id = header_project_id or default_project_id
    if not active_project_id:
        raise HTTPException(
            status_code=400,
            detail="Missing active project. Provide X-Project-Id header or configure DEFAULT_PROJECT_ID env var.",
        )

    logger.info(
        "Resolved active projectId=%s via %s",
        active_project_id,
        "X-Project-Id" if header_project_id else "DEFAULT_PROJECT_ID",
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
    legacy_filter = {"$or": [{"projectId": project_id}, {"sap.projectId": project_id}]}
    if not query:
        return legacy_filter
    return {"$and": [legacy_filter, query]}


def ensure_default_users():
    users = db.users
    users.create_index("username", unique=True)

    default_admin_user = env_get("DEFAULT_ADMIN_USERNAME", "default_admin_username", "admin")
    default_admin_pass = env_get("DEFAULT_ADMIN_PASSWORD", "default_admin_password", "admin123")
    default_admin_name = env_get("DEFAULT_ADMIN_NAME", "default_admin_name", default_admin_user)
    defaults = [(default_admin_user, default_admin_pass, "ADMIN", default_admin_name)]

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
            users.update_one(
                {"_id": existing["_id"]},
                {"$set": {"displayName": existing.get("displayName") or display_name}},
            )
            continue
        users.insert_one(
            {
                "username": username,
                "password_hash": pwd_context.hash(plain_password),
                "role": role,
                "displayName": display_name,
                "active": True,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    users.update_many({"active": {"$exists": False}}, {"$set": {"active": True}})


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
        "supplierId": payload.get("supplierId"),
        "categoryId": payload.get("categoryId"),
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


def _telegram_build_suppliers_keyboard(search_text: str, page: int = 1, limit: int = 10) -> tuple[str, dict | None, dict]:
    clean_search = str(search_text or "").strip()
    current_page = _telegram_parse_page(str(page))
    effective_limit = _telegram_parse_limit(str(limit), default=10, maximum=25)

    query: dict = {}
    if clean_search:
        escaped = re.escape(clean_search)
        query = {"$or": [{"name": {"$regex": escaped, "$options": "i"}}, {"cardCode": {"$regex": escaped, "$options": "i"}}]}

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


def _telegram_build_categories_keyboard(search_text: str, page: int = 1, limit: int = 10) -> tuple[str, dict | None, dict]:
    clean_search = str(search_text or "").strip()
    current_page = _telegram_parse_page(str(page))
    effective_limit = _telegram_parse_limit(str(limit), default=10, maximum=25)

    query: dict = {}
    if clean_search:
        escaped = re.escape(clean_search)
        query = {"name": {"$regex": escaped, "$options": "i"}}

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

    response_text, keyboard, picker = _telegram_build_suppliers_keyboard(
        search_text=picker.get("searchText") or "", page=picker.get("page") or 1, limit=picker.get("limit") or 10
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

    response_text, keyboard, picker = _telegram_build_categories_keyboard(
        search_text=picker.get("searchText") or "", page=picker.get("page") or 1, limit=picker.get("limit") or 10
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
    project_id = _resolve_default_project_id() or "699f9b894678d62c8d69f86d"

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
    project_id = _resolve_default_project_id() or "699f9b894678d62c8d69f86d"
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

    if data in {"provPick:prev", "provPick:next", "catPick:prev", "catPick:next", "noop"}:
        mode = str(picker_state.get("mode") or "").strip()
        search_text = str(picker_state.get("searchText") or "").strip()
        page = _telegram_parse_page(str(picker_state.get("page") or 1))
        limit = _telegram_parse_limit(str(picker_state.get("limit") or 10), default=10, maximum=25)

        if data.endswith(":prev"):
            page = max(1, page - 1)
        elif data.endswith(":next"):
            page = page + 1

        if mode == "prov":
            response_text, keyboard, picker = _telegram_build_suppliers_keyboard(search_text=search_text, page=page, limit=limit)
        elif mode == "cat":
            response_text, keyboard, picker = _telegram_build_categories_keyboard(search_text=search_text, page=page, limit=limit)
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
        state = {"mode": "prov", "supplierId": supplier_id, "categoryId": None, "page": 1, "limit": 25}
        _telegram_save_state(normalized_chat_id, state)
        text, keyboard = _telegram_build_transaction_message(state)
        tg_edit_message(chat_id=chat_id, message_id=message_id, text=text, reply_markup=keyboard)
        tg_answer_callback_query(callback_query_id)
        return {"ok": True}

    if data.startswith("catSel:"):
        category_id = data.split(":", 1)[1].strip()
        state = {"mode": "cat", "supplierId": None, "categoryId": category_id, "page": 1, "limit": 25}
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


def _telegram_count_transactions() -> str:
    project_id = _resolve_default_project_id()
    if not project_id:
        return "No encontré el proyecto por defecto"

    total = db.transactions.count_documents({"projectId": project_id})
    return f"Total transactions projectId={project_id}: {total}"


def _telegram_sum_expenses(month_token: str, include_iva: bool = False) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", month_token or ""):
        return "Uso: /sum YYYY-MM"

    project_id = _resolve_default_project_id()
    if not project_id:
        return "No encontré el proyecto por defecto"

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
        "/count - contar transacciones del proyecto por defecto\n"
        "/sum YYYY-MM - sumar egresos del mes (sin IVA)\n"
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


def _telegram_query_transactions(query: dict, limit: int, page: int) -> str:
    project_id = _resolve_default_project_id() or "699f9b894678d62c8d69f86d"
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


def _telegram_search_supplier(raw_text: str) -> str:
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
    return _telegram_query_transactions(query=query, limit=limit, page=page)


def _telegram_search_find(raw_text: str) -> str:
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
    return _telegram_query_transactions(query=query, limit=limit, page=page)


def _telegram_search_category_name(raw_text: str) -> str:
    text, limit, _ = _telegram_parse_search_params(raw_text)
    if not text:
        return "Uso: /cat <texto> [limit]"
    escaped = re.escape(text)
    matches = list(
        db.categories.find({"name": {"$regex": escaped, "$options": "i"}}, {"name": 1}).sort([("name", 1)]).limit(20)
    )

    if not matches:
        return "No encontré categorías con ese nombre"
    if len(matches) == 1:
        category_id = str(matches[0]["_id"])
        return _telegram_query_transactions(
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


def _telegram_search_category_id(raw_text: str) -> str:
    args = _telegram_parse_command_args(raw_text)
    if len(args) < 2:
        return "Uso: /catid <categoryId> [limit] [page]"

    category_id = args[1]
    limit = _telegram_parse_limit(args[2] if len(args) > 2 else None)
    page = _telegram_parse_page(args[3] if len(args) > 3 else None)
    return _telegram_query_transactions(
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


def _telegram_ask_transactions(text: str) -> str:
    action = _telegram_classify_ask_action(text)
    raw_keyword = _telegram_detect_ask_keyword(text)
    keyword, synonyms = _telegram_keyword_synonyms(raw_keyword)
    project_id = _resolve_default_project_id()
    if not project_id:
        return "No encontré el proyecto por defecto"

    regex_terms = [re.escape(term) for term in synonyms if term]
    if not regex_terms:
        regex_terms = [re.escape(keyword)]
    keyword_regex = "|".join(regex_terms)

    category_matches = list(
        db.categories.find(
            {"name": {"$regex": keyword_regex, "$options": "i"}},
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
    db.supplierCategories.create_index("name", unique=True)
    db.projects.create_index("name", unique=True)
    db.payments.create_index([("projectId", 1), ("sapPaymentNum", 1)], unique=True)
    db.apInvoices.create_index([("projectId", 1), ("sapInvoiceNum", 1)], unique=True)
    db.paymentLines.create_index([("paymentId", 1), ("apInvoiceId", 1), ("appliedAmount", 1)], unique=True)
    db.importRuns.create_index("sha256", unique=True)
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
    db.transactions.create_index([("projectId", 1), ("date", -1)])
    db.transactions.create_index([("projectId", 1)], name="transactions_project_id_idx")
    db.transactions.create_index([("sap.projectId", 1)], name="transactions_sap_project_id_idx")
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
        if category_id == "__UNCATEGORIZED__":
            q["$or"] = [
                {"category_id": None},
                {"category_id": ""},
                {"category_id": {"$exists": False}},
                {"categoryId": None},
                {"categoryId": ""},
                {"categoryId": {"$exists": False}},
            ]
        else:
            q["$or"] = [
                {"category_id": category_id},
                {
                    "$and": [
                        {"categoryId": category_id},
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
        search_conditions = [
            {"description": {"$regex": escaped_search, "$options": "i"}},
            {"concept": {"$regex": escaped_search, "$options": "i"}},
            {"supplierName": {"$regex": escaped_search, "$options": "i"}},
            {"beneficiario": {"$regex": escaped_search, "$options": "i"}},
        ]

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

            exact_category_match = any((category.get("normalizedName") or "") == normalized_search for category in matching_categories)
            if exact_category_match:
                search_conditions = category_search_conditions
            else:
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
        # Explicit aliases requested for SAP exports with variant spacing/casing.
        # normalize_header removes spaces/underscores/BOM before this lookup.
        "subtotal": "subtotal",  # Subtotal | Sub total | SubTotal
        "iva": "iva",  # IVA | Iva
        "totalfactura": "totalfactura",  # TotalFactura | Total Factura | Total_Factura
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
        role = env_user.get("role", "VIEWER")
        display_name = env_user.get("displayName") or username
    else:
        user = db.users.find_one({"username": username})
        if not user or not pwd_context.verify(password, user.get("password_hash", "")):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if not user.get("active", True):
            raise HTTPException(status_code=403, detail="User is inactive")
        role = user.get("role", "VIEWER")
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


@app.get("/api/supplier-categories")
def list_supplier_categories(_: dict = Depends(require_authenticated)):
    return [serialize(c) for c in db.supplierCategories.find({}).sort("name", 1)]


@app.get("/api/projects")
def list_projects(_: dict = Depends(require_authenticated)):
    rows = db.projects.find({}, {"name": 1, "slug": 1}).sort("name", 1)
    return [{"_id": str(row["_id"]), "name": row.get("name"), "slug": row.get("slug")} for row in rows]


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
def list_suppliers(uncategorized: int = 0, request: FastAPIRequest = None, _: dict = Depends(require_authenticated)):
    active_project_id = get_active_project_id(request)
    query = {"projectId": active_project_id}
    if uncategorized == 1:
        query["$or"] = [{"categoryId": None}, {"categoryId": {"$exists": False}}]
    return [serialize(s) for s in db.suppliers.find(query).sort("name", 1)]


@app.patch("/api/suppliers/{supplier_id}")
def update_supplier(supplier_id: str, payload: dict, request: FastAPIRequest, _: dict = Depends(require_admin)):
    active_project_id = get_active_project_id(request)
    supplier_filter = {"_id": oid(supplier_id), "projectId": active_project_id}
    supplier = db.suppliers.find_one(supplier_filter)
    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    category_id = payload.get("categoryId")
    if category_id is not None:
        if not db.supplierCategories.find_one({"_id": oid(category_id)}):
            raise HTTPException(status_code=400, detail="Invalid categoryId")
        db.suppliers.update_one(supplier_filter, {"$set": {"categoryId": category_id}})
    else:
        db.suppliers.update_one(supplier_filter, {"$set": {"categoryId": None}})

    return serialize(db.suppliers.find_one(supplier_filter))


def build_sap_vendor_upsert(card_code: str, beneficiary: str, project_id: str):
    return UpdateOne(
        {"source": "sap", "externalIds.sapCardCode": card_code},
        {
            "$set": {
                "name": beneficiary or card_code,
                "source": "sap",
                "externalIds.sapCardCode": card_code,
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
    project_name = project.strip() or "CALDERON DE LA BARCA"
    project_doc = db.projects.find_one_and_update(
        {"name": project_name},
        {"$setOnInsert": {"name": project_name}},
        upsert=True,
    )
    if not project_doc:
        project_doc = db.projects.find_one({"name": project_name})
    return str(project_doc["_id"])


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


def downloadFromS3(key: str) -> bytes:
    region = (os.getenv("AWS_REGION") or "").strip()
    bucket = (os.getenv("S3_BUCKET") or "").strip()

    if not region:
        raise HTTPException(status_code=500, detail="Missing AWS_REGION env var")
    if not bucket:
        raise HTTPException(status_code=500, detail="Missing S3_BUCKET env var")

    s3_client = boto3.client("s3", region_name=region)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def build_s3_key(filename: str) -> str:
    prefix = (os.getenv("S3_PREFIX") or "").strip().strip("/")
    if not prefix:
        return filename
    return f"{prefix}/{filename}"


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
    iva_key = build_s3_key("latest_IVA.csv")
    efectivo_key = build_s3_key("latest_EFECTIVO.csv")

    iva_bytes = downloadFromS3(iva_key)
    efectivo_bytes = downloadFromS3(efectivo_key)

    iva_summary = importCsv(
        iva_bytes,
        sourceDb="IVA",
        project=project,
        force=force,
        source=source,
        mode=mode,
        source_file="latest_IVA.csv",
        source_sbo="SBO_GMDI",
    )
    efectivo_summary = importCsv(
        efectivo_bytes,
        sourceDb="EFECTIVO",
        project=project,
        force=force,
        source=source,
        mode=mode,
        source_file="latest_EFECTIVO.csv",
        source_sbo="SBO_RAFAEL",
    )

    return {"iva": iva_summary, "efectivo": efectivo_summary}


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
    elif text.startswith("/ping"):
        send_telegram_to_chat("pong", chat_id=chat_id)
    elif text.startswith("/import_status"):
        send_telegram_to_chat(_telegram_import_status_summary(), chat_id=chat_id)
    elif text.startswith("/count"):
        send_telegram_to_chat(_telegram_count_transactions(), chat_id=chat_id)
    elif text.startswith("/sum"):
        _, _, month_token = text.partition(" ")
        send_telegram_to_chat(_telegram_sum_expenses(month_token=month_token.strip(), include_iva=False), chat_id=chat_id)
    elif text.startswith("/prov"):
        response_text, keyboard = _telegram_search_suppliers_keyboard(chat_id, text)
        tg_send(chat_id=chat_id, text=response_text, reply_markup=keyboard)
    elif text.startswith("/catid"):
        send_telegram_to_chat(_telegram_search_category_id(text), chat_id=chat_id)
    elif text.startswith("/cat"):
        response_text, keyboard = _telegram_search_categories_keyboard(chat_id, text)
        tg_send(chat_id=chat_id, text=response_text, reply_markup=keyboard)
    elif text.startswith("/find"):
        send_telegram_to_chat(_telegram_search_find(text), chat_id=chat_id)
    elif text.startswith("/ask"):
        _, _, ask_text = text.partition(" ")
        send_telegram_to_chat(_telegram_ask_transactions(ask_text.strip()), chat_id=chat_id)
    elif text.startswith("/chatid"):
        set_setting(TELEGRAM_SETTINGS_KEY, str(chat_id))
        send_telegram_to_chat(f"chat_id registrado: {chat_id}", chat_id=chat_id)
    elif text and not text.startswith("/"):
        send_telegram_to_chat(_telegram_ask_transactions(text), chat_id=chat_id)

    return {"ok": True}


def run_sap_import(
    file_name: str,
    file_bytes: bytes,
    project: str,
    force: int,
    source: str = "sap-payments",
    mode: str = "upsert",
    confirm_rebuild: int = 0,
    allow_rebuild: bool = False,
    source_db_override: str | None = None,
    source_file: str | None = None,
    source_sbo: str | None = None,
):
    project_id = get_or_create_project_id(project)
    source_file_key = normalize_source_file_key(source_file=source_file, file_name=file_name)
    source_file_value = source_file_key or None
    source_sbo_value = (source_sbo or "").strip() or None
    source_db_value = normalize_source_db_value(source_db_override)
    file_hash = sha256(file_bytes).hexdigest()

    existing_run = db.importRuns.find_one({"sha256": file_hash})
    existing_ok_run = existing_run and existing_run.get("status") == "ok"

    existing_source_key_run = None
    is_automated_import_source = source in {"sap-latest", "sap-latest-admin", "sap-latest-cron", "sap-payments-cron"}
    if is_automated_import_source and source_file_value and source_db_value:
        existing_source_key_run = db.importRuns.find_one(
            {
                "projectId": project_id,
                "sourceDb": source_db_value,
                "sourceFile": source_file_value,
                "status": "ok",
            }
        )

    if (existing_ok_run or existing_source_key_run) and force != 1:
        run_doc = existing_source_key_run or existing_run
        return {"already_imported": True, "importRunId": str(run_doc["_id"])}

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
        "importKey": f"{source_db_value}:{source_file_value or file_name}",
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
    if existing_source_key_run and (
        force == 1
        or existing_source_key_run.get("status") != "ok"
        or (existing_source_key_run.get("rowsOk") or 0) == 0
    ):
        reusable_run = existing_source_key_run

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

    supplier_ids_in_file = sorted(
        {
            supplier_id
            for supplier_id in (suppliers_map.get(record["cardCode"]) for record in line_records)
            if supplier_id
        }
    )
    supplier_auto_category_map = build_supplier_auto_category_map(supplier_ids_in_file)

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
            tx_filter = {
                "projectId": project_id,
                "source": "sap",
                "sourceDb": record["sourceDb"],
                "sap.pagoNum": record["paymentNum"],
                "sap.facturaNum": record["invoiceNum"],
                "sap.montoAplicadoCents": record["appliedAmountCents"],
            }
            existing_tx = db.transactions.find_one(tx_filter, {"categoryId": 1, "category_id": 1})
            existing_category_id = None
            if existing_tx:
                existing_category_id = existing_tx.get("categoryId") or existing_tx.get("category_id")
                if existing_category_id:
                    category_preserved_count += 1
                    category_would_have_changed_count += 1

            inferred_category_id = supplier_auto_category_map.get(supplier_id)

            sap_set_doc = {
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
                "vendor_id": None,
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
                },
            }
            sap_expense_ops.append(
                UpdateOne(
                    tx_filter,
                    {
                        "$set": sap_set_doc,
                        "$setOnInsert": {
                            "categoryId": inferred_category_id,
                            "category_id": inferred_category_id,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "sourceFile": source_file_value,
                        },
                    },
                    upsert=True,
                )
            )

    if lines_ops:
        result = db.paymentLines.bulk_write(lines_ops, ordered=False)
        lines_inserted = result.upserted_count or 0
        duplicates_skipped = len(lines_ops) - lines_inserted

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
            duplicate_errors_count = len(write_errors) - len(non_duplicate_errors)
            duplicates_skipped += duplicate_errors_count

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
            sap_expenses_updated = modified_count
            sap_expenses_upserted = upserted_count + modified_count

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
                "categoryPreservedCount": category_preserved_count,
                "categoryWouldHaveChangedCount": category_would_have_changed_count,
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


@app.post("/api/admin/import/sap-latest")
def admin_import_sap_latest(
    project: str = "CALDERON DE LA BARCA",
    force: int = 0,
    mode: str = "upsert",
    _: dict = Depends(require_admin),
):
    try:
        result = run_s3_latest_sap_import(project=project, force=force, mode=mode, source="sap-latest-admin")
        notify_sap_latest_import_success(project=project, result=result)
        return result
    except Exception as exc:
        notify_sap_latest_import_failure(project=project, exc=exc)
        raise


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
    now = datetime.now(timezone.utc).isoformat()
    try:
        result = run_s3_latest_sap_import(project=project, force=force, mode=mode, source="sap-latest-cron")
        notify_sap_latest_import_success(project=project, result=result)
        print(f"sap_latest_cron ok iva={result['iva']} efectivo={result['efectivo']}")
        return result
    except Exception as exc:
        notify_sap_latest_import_failure(project=project, exc=exc)
        error_hash = sha256(f"sap_latest_cron_error:{now}:{str(exc)}".encode("utf-8")).hexdigest()
        import_run_id = db.importRuns.insert_one(
            {
                "sha256": error_hash,
                "fileName": None,
                "source": "sap-latest-cron",
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
        print(f"sap_latest_cron failed importRunId={import_run_id} error={str(exc)}")
        raise


@app.get("/api/expenses/summary-by-supplier")
def summary_expenses_by_supplier(
    projectId: str | None = None,
    project: str | None = None,
    include_iva: bool = False,
    _: dict = Depends(require_authenticated),
):
    project_id = resolve_project_id(projectId or project)
    movements_query = {"type": "EXPENSE", "projectId": project_id}
    movements = list(
        db.transactions.find(
            movements_query,
            {"supplierId": 1, "amount": 1, "tax": 1},
        )
    )

    supplier_totals = {}
    for tx in movements:
        supplier_id = tx.get("supplierId")
        bucket = supplier_totals.setdefault(supplier_id, {"totalAmount": 0.0, "count": 0})
        amount_value = float(tx.get("amount") or 0)
        bucket["totalAmount"] += amount_value if include_iva else compute_monto_sin_iva(tx)
        bucket["count"] += 1

    rows = [
        {
            "_id": supplier_id,
            "totalAmount": round(values["totalAmount"], 2),
            "count": values["count"],
        }
        for supplier_id, values in supplier_totals.items()
    ]

    supplier_ids = [oid(row["_id"]) for row in rows if row.get("_id")]
    supplier_names = {}
    if supplier_ids:
        for supplier in db.suppliers.find({"_id": {"$in": supplier_ids}, "projectId": project_id}, {"name": 1}):
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
    suppliers = list(db.suppliers.find({"projectId": project_id}, {"cardCode": 1, "name": 1}))
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
def admin_test_telegram(user: dict = Depends(require_admin)):
    sent = send_telegram("✅ test telegram desde backend")
    logger.info("Admin telegram test requested by %s sent=%s", user.get("username"), sent)
    return {"sent": sent}


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
    request: FastAPIRequest = None,
    _: dict = Depends(require_authenticated),
):
    active_project_id = get_active_project_id(request)
    q = {"projectId": active_project_id, "active": True} if active_only else {"projectId": active_project_id}
    if category_id:
        q["category_ids"] = category_id
    if include_sap:
        q["$or"] = [{"source": {"$exists": False}}, {"source": "manual"}, {"source": "sap"}]
    else:
        q["$or"] = [{"source": {"$exists": False}}, {"source": "manual"}]
    return [serialize(v) for v in db.vendors.find(q).sort("name", 1)]


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
        "vendor_id": vendor_id,
        "description": payload.get("description"),
        "reference": payload.get("reference"),
        "sourceDb": source_db,
        "created_at": datetime.utcnow().isoformat(),
        "projectId": active_project_id,
    }
    _id = db.transactions.insert_one(doc).inserted_id
    return serialize(db.transactions.find_one({"_id": _id}))


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

    if "category_id" in payload:
        cid = payload.get("category_id")
        if cid and not db.categories.find_one({"_id": oid(cid), "active": True}):
            raise HTTPException(status_code=400, detail="Invalid category_id")
        updates["category_id"] = cid
        updates["categoryId"] = cid

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
    existing_category_id = tx.get("category_id") or tx.get("categoryId")
    should_apply_category_to_related = bool(category_id and not existing_category_id)

    if should_apply_category_to_related:
        supplier_id = tx.get("supplierId")
        if supplier_id:
            db.transactions.update_many(
                {"supplierId": supplier_id, "type": "EXPENSE", "projectId": active_project_id},
                {"$set": {"category_id": category_id, "categoryId": category_id}},
            )
        elif tx.get("vendor_id"):
            db.transactions.update_many(
                {"vendor_id": tx.get("vendor_id"), "type": "EXPENSE", "projectId": active_project_id},
                {"$set": {"category_id": category_id}},
            )

    return serialize(db.transactions.find_one(transaction_filter))


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
    _: dict = Depends(require_authenticated),
):
    normalized_page = max(page, 1)
    resolved_project_id = resolve_project_id(project_id)
    logger.info("transactions projectId=%s resolved=%s", project_id, resolved_project_id)
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
        project_id=resolved_project_id,
        origen=origen,
        source=source,
        source_db=sourceDb,
        search_query=q,
    )
    match_query["projectId"] = resolved_project_id

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
        for supplier in db.suppliers.find({"_id": {"$in": supplier_ids}, "projectId": resolved_project_id}, {"name": 1, "cardCode": 1}):
            suppliers_by_id[str(supplier["_id"])] = supplier

    items = []
    for tx in txs:
        tx_doc = serialize_transaction_with_supplier(tx, suppliers_by_id)

        tax_doc = tx_doc.get("tax") if isinstance(tx_doc.get("tax"), dict) else {}
        subtotal = parse_optional_decimal(tax_doc.get("subtotal"))
        iva = parse_optional_decimal(tax_doc.get("iva"))
        total_factura = parse_optional_decimal(tax_doc.get("totalFactura"))

        tx_doc["subtotal"] = subtotal if subtotal is not None else None
        tx_doc["iva"] = iva if iva is not None else None
        tx_doc["totalFactura"] = total_factura if total_factura is not None else None
        amount = parse_optional_decimal(tx_doc.get("amount")) or 0
        sign = -1 if amount < 0 else 1

        tx_doc["subtotal"] = sign * subtotal if subtotal is not None else None
        tx_doc["iva"] = sign * iva if iva is not None else None
        tx_doc["totalFactura"] = sign * total_factura if total_factura is not None else None
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
    _: dict = Depends(require_authenticated),
):
    active_project_id = resolve_project_id(projectId or project)
    match = {"type": "EXPENSE", "projectId": active_project_id}
    if vendor_id:
        match["vendor_id"] = vendor_id
    if date_from or date_to:
        match["date"] = {}
        if date_from:
            match["date"]["$gte"] = date_from
        if date_to:
            match["date"]["$lte"] = date_to

    transactions = list(db.transactions.find(match, {"category_id": 1, "amount": 1, "tax": 1}))

    totals_by_category = {}
    for tx in transactions:
        category_id = tx.get("category_id")
        amount_value = float(tx.get("amount") or 0)
        movement_amount = amount_value if include_iva else compute_monto_sin_iva(tx)
        totals_by_category[category_id] = round(totals_by_category.get(category_id, 0.0) + movement_amount, 2)

    rows = [{"_id": category_id, "amount": amount} for category_id, amount in totals_by_category.items()]
    rows.sort(key=lambda row: row["amount"], reverse=True)
    total = round(sum(float(r["amount"]) for r in rows), 2) if rows else 0.0

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


ensure_indexes()
ensure_default_users()
