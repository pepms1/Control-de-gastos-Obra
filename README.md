
# Control de Obra

Backend: FastAPI + MongoDB
Frontend: Placeholder

Deploy backend from api/ folder.

## Guardrail de imports

- Ejecutar `./scripts/check_no_import_metadata.sh` para verificar que no se agreguen `metadata=` en llamadas de import.

## Frontend multiproyecto

- El frontend guarda el proyecto seleccionado en `localStorage` usando la llave `selectedProjectId`.
- Al iniciar sesión, la app consulta `GET /api/projects` y usa el `selectedProjectId` guardado si aún existe; si no, toma el primer proyecto disponible y lo persiste.
- El cliente API añade automáticamente `projectId=<selectedProjectId>` en requests `GET` de reportes y listados relevantes (`/transactions`, `/api/transactions`, `/api/movimientos`, `/api/expenses/summary-by-supplier`, `/stats/spend-by-category`) cuando el caller no lo envía explícitamente.
- El token se sigue enviando igual que antes mediante `Authorization: Bearer <token>`.
