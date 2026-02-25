from fastapi import FastAPI, HTTPException, Response, Depends
from fastapi import Header
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
from passlib.context import CryptContext
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


def create_token(username: str, role: str):
    exp = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS)
    payload = {"sub": username, "role": role, "exp": exp}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


ensure_default_users()

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
