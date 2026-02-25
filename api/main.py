
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "obra")

client = MongoClient(MONGO_URL)
db = client[DB_NAME]

@app.get("/")
def root():
    return {"status": "API running"}

from fastapi import Response

@app.head("/")
def root_head():
    return Response(status_code=200)

@app.get("/docs-test")
def docs_test():
    return {"message": "If you see this, backend works."}
