from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from bson import ObjectId
from datetime import date, datetime
import os

app = FastAPI(title="Control de Obra API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # en producción puedes restringir a tu dominio Vercel
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URL = os.getenv("MONGO_URL")
if not MONGO_URL:
    raise RuntimeError("MONGO_URL env var is required")

DB_NAME = os.getenv("DB_NAME", "obra")
client = MongoClient(MONGO_URL)
db = client[DB_NAME]

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

# ---------- seed categories ----------
DEFAULT_CATEGORIES = [
    "Albañilería", "Cimentación / Estructura", "Plomería / Hidrosanitario", "Eléctrico",
    "Tablaroca / Plafones", "Cancelería / Vidrio", "Carpintería", "Herrería",
    "Impermeabilización", "Pisos / Azulejos", "Yesos / Aplanados", "Pintura",
    "Acabados / Detalles", "Materiales (Generales)", "Renta de maquinaria",
    "Fletes / Acarreos", "Permisos / Gestoría", "Mano de obra (General)",
    "Seguridad / Limpieza", "Imprevistos",
]

@app.post("/seed")
def seed():
    cats = db.categories
    existing = {c["name"] for c in cats.find({}, {"name": 1})}
    to_insert = [{"name": n, "active": True} for n in DEFAULT_CATEGORIES if n not in existing]
    if to_insert:
        cats.insert_many(to_insert)
    return {"created_categories": len(to_insert)}

# ---------- categories ----------
@app.get("/categories")
def list_categories(active_only: bool = True):
    q = {"active": True} if active_only else {}
    return [serialize(c) for c in db.categories.find(q).sort("name", 1)]

@app.post("/categories")
def create_category(payload: dict):
    name = (payload.get("name") or "").strip()
    if len(name) < 2:
        raise HTTPException(status_code=400, detail="name is required")
    if db.categories.find_one({"name": name}):
        raise HTTPException(status_code=409, detail="Category already exists")
    _id = db.categories.insert_one({"name": name, "active": True}).inserted_id
    return serialize(db.categories.find_one({"_id": _id}))

# ---------- vendors ----------
@app.get("/vendors")
def list_vendors(active_only: bool = True, category_id: str | None = None):
    q = {"active": True} if active_only else {}
    if category_id:
        q["category_ids"] = category_id
    return [serialize(v) for v in db.vendors.find(q).sort("name", 1)]

@app.post("/vendors")
def create_vendor(payload: dict):
    name = (payload.get("name") or "").strip()
    if len(name) < 2:
        raise HTTPException(status_code=400, detail="name is required")
    if db.vendors.find_one({"name": name}):
        raise HTTPException(status_code=409, detail="Vendor already exists")

    category_ids = payload.get("category_ids") or []
    # validate categories exist
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

@app.post("/vendors/{vendor_id}/categories")
def set_vendor_categories(vendor_id: str, category_ids: list[str]):
    if not db.vendors.find_one({"_id": oid(vendor_id)}):
        raise HTTPException(status_code=404, detail="Vendor not found")
    for cid in category_ids:
        if not db.categories.find_one({"_id": oid(cid), "active": True}):
            raise HTTPException(status_code=400, detail=f"Invalid category_id: {cid}")
    db.vendors.update_one({"_id": oid(vendor_id)}, {"$set": {"category_ids": category_ids}})
    return serialize(db.vendors.find_one({"_id": oid(vendor_id)}))

# ---------- transactions ----------
@app.post("/transactions")
def create_transaction(payload: dict):
    ttype = payload.get("type")
    if ttype not in ("INCOME", "EXPENSE"):
        raise HTTPException(status_code=400, detail="type must be INCOME or EXPENSE")

    # date: accept YYYY-MM-DD string
    d = payload.get("date")
    if not d:
        raise HTTPException(status_code=400, detail="date is required")
    if isinstance(d, str):
        try:
            # store as ISO string for easy range filters
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

@app.get("/transactions")
def list_transactions(
    type: str | None = None,
    category_id: str | None = None,
    vendor_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 200
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
def spend_by_category(date_from: str | None = None, date_to: str | None = None, vendor_id: str | None = None):
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

    # map category names
    cat_ids = [oid(r["_id"]) for r in rows if r.get("_id")]
    cats = {}
    if cat_ids:
        for c in db.categories.find({"_id": {"$in": cat_ids}}, {"name": 1}):
            cats[str(c["_id"])] = c["name"]

    out = []
    for r in rows:
        cid = r["_id"]
        amt = float(r["amount"])
        out.append({
            "category_id": cid,
            "category_name": cats.get(cid, "(Sin categoría)"),
            "amount": round(amt, 2),
            "percent": round((amt / total * 100.0), 2) if total > 0 else 0.0
        })
    return {"total_expenses": round(total, 2), "rows": out}
