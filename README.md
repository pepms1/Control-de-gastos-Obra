
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

## Cron V2: import latest por SBO

Script: `scripts/render_cron_import_sbo_v2.py`

Variables requeridas/soportadas:
- `CRON_BASE_URL`
- `CRON_LOGIN_PATH` (default: `/api/auth/login`)
- `CRON_IMPORT_PATH` (default: `/api/cron/import/sap-movements-by-sbo`)
- `CRON_USERNAME`
- `CRON_PASSWORD`
- `CRON_SBO_LIST` (default: `SBO_GMDI,SBO_Rafael,SBO_Colima334,SBO_CPSantaFE,SBOCitySur,SBOIndiana,SBO_Mazatlan`)
- `CRON_MODE` (default: `latest`)
- `CRON_FORCE` (default: `0`)
- `CRON_TIMEOUT_SEC` (default: `120`)

Comportamiento:
- Hace login una sola vez y reutiliza el token para todas las SBOs.
- Ejecuta `POST /api/cron/import/sap-movements-by-sbo?sbo=<SBO>&mode=<...>&force=<...>` por cada SBO.
- Continúa aunque falle una SBO.
- Imprime salida JSON por SBO y un resumen final JSON.
- Exit code `1` si hubo errores parciales, `0` si todo salió bien.

Comando sugerido para Render Cron:
- `python3 scripts/render_cron_import_sbo_v2.py`

Opcional: puedes usar `render.yaml` incluido como base para crear el servicio `type: cron`.


## Telegram + imports SAP (V2)

- Backend centraliza notificaciones Telegram para `POST /api/cron/import/sap-movements-by-sbo`.
- Destinatario para imports SAP por SBO: `TELEGRAM_IMPORTS_CHAT_ID`; si falta, usa fallback `TELEGRAM_ADMIN_CHAT_ID` y luego `TELEGRAM_CHAT_ID`/default.
- Las notificaciones de imports SAP por SBO no leen `telegram_users` ni envían broadcast.
- Otras funciones del bot (por ejemplo tests/admin) pueden seguir usando `telegram_users`/fallback existentes.
- Si Telegram falla, el import SAP **no** se cae (best-effort; solo log).
- Webhook disponible en `POST /api/telegram/webhook`.
- Test manual admin: `POST /api/admin/telegram/test?message=<texto>`.

Etiquetado de origen (`X-Trigger-Source`):
- Frontend V2 envía `X-Trigger-Source: frontend`.
- Cron `scripts/render_cron_import_sbo_v2.py` envía `X-Trigger-Source: cron`.
- Si no se envía header, backend usa `api`.

Pruebas rápidas:
1. Test manual Telegram:
   - `POST /api/admin/telegram/test?message=hola`
2. Import SAP desde frontend:
   - Usar botón **Import SAP** (ya manda `X-Trigger-Source: frontend`).
3. Import SAP desde cron:
   - `python3 scripts/render_cron_import_sbo_v2.py` (manda `X-Trigger-Source: cron`).
