#!/usr/bin/env python3
"""Cron job para importar movimientos SAP latest por SBO (Control de Gastos Obra V2)."""

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_SBO_LIST = "SBO_GMDI,SBO_Rafael,SBO_Colima334,SBO_CPSantaFE,SBOCitySur,SBOIndiana,SBO_Mazatlan"


@dataclass
class CronConfig:
    base_url: str
    login_path: str
    import_path: str
    username: str
    password: str
    sbo_list: list[str]
    mode: str
    force: int
    timeout_sec: int


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def load_config() -> CronConfig:
    base_url = _env("CRON_BASE_URL")
    login_path = _env("CRON_LOGIN_PATH", "/api/auth/login")
    import_path = _env("CRON_IMPORT_PATH", "/api/cron/import/sap-movements-by-sbo")
    username = _env("CRON_USERNAME")
    password = _env("CRON_PASSWORD")
    raw_sbo_list = _env("CRON_SBO_LIST", DEFAULT_SBO_LIST)
    mode = _env("CRON_MODE", "latest")
    force_raw = _env("CRON_FORCE", "0")
    timeout_raw = _env("CRON_TIMEOUT_SEC", "120")

    if not base_url:
        raise ValueError("Missing required env var CRON_BASE_URL")
    if not username or not password:
        raise ValueError("Missing required env vars CRON_USERNAME / CRON_PASSWORD")

    sbo_list = [item.strip() for item in raw_sbo_list.split(",") if item.strip()]
    if not sbo_list:
        raise ValueError("CRON_SBO_LIST is empty after parsing")

    if mode not in {"latest", "backfill"}:
        raise ValueError("CRON_MODE must be 'latest' or 'backfill'")

    try:
        force = int(force_raw)
    except ValueError as exc:
        raise ValueError("CRON_FORCE must be 0 or 1") from exc
    if force not in {0, 1}:
        raise ValueError("CRON_FORCE must be 0 or 1")

    try:
        timeout_sec = int(timeout_raw)
    except ValueError as exc:
        raise ValueError("CRON_TIMEOUT_SEC must be an integer") from exc
    if timeout_sec <= 0:
        raise ValueError("CRON_TIMEOUT_SEC must be > 0")

    return CronConfig(
        base_url=base_url.rstrip("/"),
        login_path=login_path,
        import_path=import_path,
        username=username,
        password=password,
        sbo_list=sbo_list,
        mode=mode,
        force=force,
        timeout_sec=timeout_sec,
    )


def build_url(base_url: str, path: str) -> str:
    path = path if path.startswith("/") else f"/{path}"
    return f"{base_url}{path}"


def parse_json_response(body: bytes) -> Any:
    raw = body.decode("utf-8", "replace")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def login(config: CronConfig) -> str:
    login_url = build_url(config.base_url, config.login_path)
    payload = json.dumps({"username": config.username, "password": config.password}).encode("utf-8")
    req = urllib.request.Request(
        login_url,
        method="POST",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=config.timeout_sec) as response:
        data = parse_json_response(response.read())
        token = data.get("access_token") if isinstance(data, dict) else None
        if not token:
            raise RuntimeError(f"Login succeeded but access_token missing. response={data}")
        return token


def import_sbo(config: CronConfig, token: str, sbo: str) -> dict[str, Any]:
    import_url = build_url(config.base_url, config.import_path)
    query = urllib.parse.urlencode(
        {
            "sbo": sbo,
            "mode": config.mode,
            "force": str(config.force),
        }
    )
    url = f"{import_url}?{query}"

    req = urllib.request.Request(
        url,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Trigger-Source": "cron",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=config.timeout_sec) as response:
            body = parse_json_response(response.read())
            return {
                "sbo": sbo,
                "ok": True,
                "status": response.status,
                "mode": config.mode,
                "force": config.force,
                "response": body,
            }
    except urllib.error.HTTPError as exc:
        error_body = parse_json_response(exc.read())
        return {
            "sbo": sbo,
            "ok": False,
            "status": exc.code,
            "mode": config.mode,
            "force": config.force,
            "error": "http_error",
            "response": error_body,
        }
    except Exception as exc:
        return {
            "sbo": sbo,
            "ok": False,
            "status": None,
            "mode": config.mode,
            "force": config.force,
            "error": "request_error",
            "message": str(exc),
        }


def main() -> int:
    try:
        config = load_config()
    except Exception as exc:
        print(json.dumps({"ok": False, "error": "invalid_config", "message": str(exc)}, ensure_ascii=False))
        return 1

    print(
        json.dumps(
            {
                "event": "cron_start",
                "baseUrl": config.base_url,
                "loginPath": config.login_path,
                "importPath": config.import_path,
                "mode": config.mode,
                "force": config.force,
                "timeoutSec": config.timeout_sec,
                "sboCount": len(config.sbo_list),
            },
            ensure_ascii=False,
        )
    )

    try:
        token = login(config)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": "login_failed", "message": str(exc)}, ensure_ascii=False))
        return 1

    results: list[dict[str, Any]] = []
    for sbo in config.sbo_list:
        result = import_sbo(config, token, sbo)
        results.append(result)
        print(json.dumps(result, ensure_ascii=False))

    has_errors = any(not item.get("ok") for item in results)
    summary = {
        "ok": not has_errors,
        "total": len(results),
        "success": sum(1 for item in results if item.get("ok")),
        "failed": sum(1 for item in results if not item.get("ok")),
        "mode": config.mode,
        "force": config.force,
        "results": results,
    }
    print(json.dumps({"event": "cron_summary", **summary}, ensure_ascii=False))
    return 1 if has_errors else 0


if __name__ == "__main__":
    sys.exit(main())
