import os

# Las pruebas no deben compartir la caché corta de egresos entre casos.
os.environ.setdefault("EXPENSE_CACHE_TTL_SECONDS", "0")
