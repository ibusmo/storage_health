from __future__ import annotations

import asyncio
import json
import re
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError

from sd_health.db import list_devices, list_runs, migrate_from_jsonl, set_device_user_prefs
from sd_health.info import eject_removable_bsd, list_removable_candidates
from sd_health.storage_identity import enrich_candidate_row
from sd_health.models import RunRecord
from sd_health.persist import persist_run
from sd_health.quick_test import cam_stress_record, quick_test_record
from sd_health.macos_verify import repair_volume_record, verify_volume_record
from sd_health.report import write_report
from sd_health.safety import is_likely_system_disk, validate_mount_point

_TBody = TypeVar("_TBody", bound=BaseModel)


# Pydantic body models must live at module scope: defining them inside `create_app()` leaves
# `route.body_field` unset, so FastAPI treats them as query params and JSON POSTs fail (422).
class DevicePrefsBody(BaseModel):
    fingerprint: str = Field(..., min_length=16, max_length=128)
    user_label: str = ""
    user_notes: str = ""
    user_brand: str = ""
    user_series: str = ""


class QuickBody(BaseModel):
    mount: str = Field(..., min_length=1, description="Mounted volume path")
    notes: str | None = None
    sample_mib: int = Field(
        4,
        ge=1,
        le=512,
        description="Temp file size (MiB) for integrity check and speed estimate",
    )


class CamStressBody(BaseModel):
    mount: str = Field(..., min_length=1, description="Mounted volume path")
    notes: str | None = None
    duration_min: int = Field(
        1,
        ge=1,
        le=240,
        description="Sustained write/read duration in minutes",
    )
    chunk_mib: int = Field(
        16,
        ge=1,
        le=64,
        description="Sequential write chunk size in MiB",
    )
    profile: str | None = Field(
        None,
        description="Target camera mode: hd|fhd|2k|4k|8k",
    )
    min_write_mib_s: float | None = Field(
        None,
        ge=0,
        description="Optional override minimum sustained write MiB/s",
    )


class VerifyVolumeBody(BaseModel):
    mount: str = Field(..., min_length=1, description="Mounted volume path")


class EjectBody(BaseModel):
    device: str = Field(
        ...,
        min_length=1,
        description="BSD disk path from removable list, e.g. disk5s1 or /dev/disk5",
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _read_json_model(request: Request, model: type[_TBody]) -> _TBody:
    """Parse JSON body explicitly (avoids FastAPI body_field quirks on some deployments)."""
    raw = await request.body()
    if not raw.strip():
        raise HTTPException(
            status_code=400,
            detail='Empty body: send JSON, e.g. {"mount":"/Volumes/NAME","notes":null,"sample_mib":4}',
        )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")
    try:
        return model.model_validate(data)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors()) from e


def _load_dashboard_html() -> str:
    """Inline CSS so the dashboard is styled even when `/static/*` mount fails (FastAPI mount order)."""
    root = Path(__file__).resolve().parent
    html = (root / "dashboard.html").read_text(encoding="utf-8")
    css_path = root / "static" / "theme.css"
    if not css_path.is_file():
        return html
    css = css_path.read_text(encoding="utf-8")
    html = re.sub(
        r'<link\s+rel="stylesheet"\s+href="/static/theme\.css(?:\?[^"]*)?"\s*/>',
        f"<style>\n{css}\n</style>",
        html,
        count=1,
    )
    return html


def create_app(jsonl_path: Path, report_path: Path, db_path: Path) -> FastAPI:
    jsonl_path = jsonl_path.resolve()
    report_path = report_path.resolve()
    db_path = db_path.resolve()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        migrate_from_jsonl(db_path, jsonl_path)
        yield

    app = FastAPI(title="Storage Health", version="0.1.0", lifespan=lifespan)
    app.state.start_time = time.time()
    app.state.test_status = {"running": False}

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return _load_dashboard_html()

    @app.get("/api/health")
    async def health(request: Request) -> dict[str, Any]:
        # Lets the dashboard detect a stale server (older builds returned 422 for POST JSON).
        started = getattr(request.app.state, "start_time", None)
        now = time.time()
        uptime = int(now - started) if started is not None else 0
        return {
            "status": "ok",
            "post_json": "read_body",
            "server_time_utc": datetime.now(timezone.utc).isoformat(),
            "uptime_sec": uptime,
            # Dashboard uses this to avoid 404 "Not Found" on older server processes.
            "has_repair_volume_post": True,
            "has_eject_post": True,
            "has_test_status": True,
            # Bumped when fingerprint rules change; if this stays old, the server binary is stale.
            "identity_fingerprint_scheme": "volume_uuid_v1",
        }

    @app.get("/api/platform")
    def api_platform() -> dict[str, str | bool]:
        return {
            "host_os": sys.platform,
            "diskutil_verify_available": sys.platform == "darwin",
        }

    @app.get("/api/test/status")
    def api_test_status() -> dict[str, Any]:
        st = getattr(app.state, "test_status", {"running": False})
        return st if isinstance(st, dict) else {"running": False}

    @app.get("/api/runs")
    def api_runs() -> JSONResponse:
        return JSONResponse(list_runs(db_path))

    @app.get("/api/devices")
    def api_devices() -> JSONResponse:
        return JSONResponse(list_devices(db_path))

    @app.post("/api/eject")
    async def api_eject(request: Request) -> JSONResponse:
        body = await _read_json_model(request, EjectBody)
        out = eject_removable_bsd(body.device)
        return JSONResponse(out)

    @app.get("/api/candidates")
    def api_candidates() -> JSONResponse:
        raw = list_removable_candidates()
        devices = list_devices(db_path)
        prefs: dict[str, dict[str, str]] = {}
        for d in devices:
            fp = d.get("fingerprint")
            if fp:
                prefs[fp] = {
                    "user_label": (d.get("user_label") or ""),
                    "user_notes": (d.get("user_notes") or ""),
                    "user_brand": (d.get("user_brand") or ""),
                    "user_series": (d.get("user_series") or ""),
                }
        enriched = [enrich_candidate_row(r, prefs) for r in raw]
        return JSONResponse(enriched)

    @app.patch("/api/devices/prefs")
    async def api_device_prefs(request: Request) -> dict[str, str]:
        prefs = await _read_json_model(request, DevicePrefsBody)
        fp = prefs.fingerprint.strip()
        if not fp:
            raise HTTPException(status_code=400, detail="fingerprint required")
        set_device_user_prefs(
            db_path,
            fp,
            user_label=prefs.user_label,
            user_notes=prefs.user_notes,
            user_brand=prefs.user_brand,
            user_series=prefs.user_series,
        )
        return {"ok": "true"}

    @app.post("/api/report/build")
    def api_report_build() -> dict[str, str]:
        if not jsonl_path.is_file() and not db_path.is_file():
            jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            jsonl_path.write_text("", encoding="utf-8")
        write_report(report_path, jsonl_path=jsonl_path if jsonl_path.is_file() else None, db_path=db_path)
        return {"ok": "true", "report": str(report_path)}

    @app.post("/api/test/quick")
    async def api_test_quick(request: Request) -> JSONResponse:
        quick = await _read_json_model(request, QuickBody)
        try:
            mp = validate_mount_point(quick.mount)
        except SystemExit as e:
            d = e.args[0] if e.args else str(e)
            raise HTTPException(status_code=400, detail=str(d)) from e
        bad, reason = is_likely_system_disk(None, str(mp))
        if bad:
            raise HTTPException(status_code=400, detail=f"Safety: {reason}")
        started = _now_iso()
        run_id = str(uuid.uuid4())
        try:
            out = quick_test_record(
                mount_point=str(mp),
                device_path=None,
                raw_read=False,
                i_know=True,
                sample_mib=quick.sample_mib,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        finished = _now_iso()
        rec = RunRecord(
            run_id=run_id,
            started_at=started,
            finished_at=finished,
            host_os=sys.platform,
            test_mode="quick",
            device_path=None,
            mount_point=str(mp),
            identity=out["identity"],
            result=out["result"],  # type: ignore[arg-type]
            summary=out["summary"],
            error_detail=out.get("error_detail"),
            operator_notes=quick.notes,
        )
        payload = rec.to_json()
        persist_run(payload, db_path=db_path, jsonl_path=jsonl_path)
        write_report(report_path, jsonl_path=jsonl_path if jsonl_path.is_file() else None, db_path=db_path)
        return JSONResponse(payload)

    @app.post("/api/test/cam-stress")
    async def api_test_cam_stress(request: Request) -> JSONResponse:
        req = await _read_json_model(request, CamStressBody)
        try:
            mp = validate_mount_point(req.mount)
        except SystemExit as e:
            d = e.args[0] if e.args else str(e)
            raise HTTPException(status_code=400, detail=str(d)) from e
        bad, reason = is_likely_system_disk(None, str(mp))
        if bad:
            raise HTTPException(status_code=400, detail=f"Safety: {reason}")
        started = _now_iso()
        run_id = str(uuid.uuid4())
        app.state.test_status = {
            "running": True,
            "mode": "cam_stress",
            "run_id": run_id,
            "phase": "starting",
            "started_at": started,
            "duration_target_sec": req.duration_min * 60,
        }

        def _progress_update(payload: dict[str, object]) -> None:
            base = getattr(app.state, "test_status", {}) or {}
            if not isinstance(base, dict):
                base = {}
            merged = dict(base)
            merged.update(payload)
            cw = merged.get("current_write_mib_s")
            if isinstance(cw, (int, float)):
                old_pw = merged.get("peak_write_mib_s")
                merged["peak_write_mib_s"] = round(
                    max(float(cw), float(old_pw) if isinstance(old_pw, (int, float)) else float(cw)),
                    2,
                )
            cr = merged.get("current_read_mib_s")
            if isinstance(cr, (int, float)):
                old_pr = merged.get("peak_read_mib_s")
                merged["peak_read_mib_s"] = round(
                    max(float(cr), float(old_pr) if isinstance(old_pr, (int, float)) else float(cr)),
                    2,
                )
            merged["running"] = True
            merged["mode"] = "cam_stress"
            merged["run_id"] = run_id
            app.state.test_status = merged
        try:
            out = await asyncio.to_thread(
                cam_stress_record,
                mount_point=str(mp),
                device_path=None,
                duration_min=req.duration_min,
                chunk_mib=req.chunk_mib,
                profile=req.profile,
                min_write_mib_s=req.min_write_mib_s,
                progress_cb=_progress_update,
            )
        except ValueError as e:
            app.state.test_status = {
                "running": False,
                "mode": "cam_stress",
                "run_id": run_id,
                "phase": "error",
                "error": str(e),
            }
            raise HTTPException(status_code=400, detail=str(e)) from e
        finished = _now_iso()
        rec = RunRecord(
            run_id=run_id,
            started_at=started,
            finished_at=finished,
            host_os=sys.platform,
            test_mode="cam_stress",
            device_path=None,
            mount_point=str(mp),
            identity=out["identity"],
            result=out["result"],  # type: ignore[arg-type]
            summary=out["summary"],
            error_detail=out.get("error_detail"),
            operator_notes=req.notes,
        )
        payload = rec.to_json()
        persist_run(payload, db_path=db_path, jsonl_path=jsonl_path)
        write_report(report_path, jsonl_path=jsonl_path if jsonl_path.is_file() else None, db_path=db_path)
        app.state.test_status = {
            "running": False,
            "mode": "cam_stress",
            "run_id": run_id,
            "phase": "done",
            "result": payload.get("result"),
            "summary": payload.get("summary"),
            "finished_at": finished,
        }
        return JSONResponse(payload)

    @app.post("/api/test/verify-volume")
    async def api_verify_volume(request: Request) -> JSONResponse:
        req = await _read_json_model(request, VerifyVolumeBody)
        if sys.platform != "darwin":
            raise HTTPException(
                status_code=400,
                detail="Filesystem verify uses macOS diskutil. On other OSes use built-in disk tools.",
            )
        try:
            mp = validate_mount_point(req.mount)
        except SystemExit as e:
            d = e.args[0] if e.args else str(e)
            raise HTTPException(status_code=400, detail=str(d)) from e
        bad, reason = is_likely_system_disk(None, str(mp))
        if bad:
            raise HTTPException(status_code=400, detail=f"Safety: {reason}")
        started = _now_iso()
        run_id = str(uuid.uuid4())
        out = verify_volume_record(str(mp))
        finished = _now_iso()
        rec = RunRecord(
            run_id=run_id,
            started_at=started,
            finished_at=finished,
            host_os=sys.platform,
            test_mode="verify",
            device_path=None,
            mount_point=str(mp),
            identity=out["identity"],
            result=out["result"],  # type: ignore[arg-type]
            summary=out["summary"],
            error_detail=out.get("error_detail"),
            operator_notes=None,
        )
        payload = rec.to_json()
        persist_run(payload, db_path=db_path, jsonl_path=jsonl_path)
        write_report(report_path, jsonl_path=jsonl_path if jsonl_path.is_file() else None, db_path=db_path)
        return JSONResponse(payload)

    @app.post("/api/test/repair-volume")
    async def api_repair_volume(request: Request) -> JSONResponse:
        req = await _read_json_model(request, VerifyVolumeBody)
        if sys.platform != "darwin":
            raise HTTPException(
                status_code=400,
                detail="Filesystem repair uses macOS diskutil. On other OSes use Disk Utility or built-in tools.",
            )
        try:
            mp = validate_mount_point(req.mount)
        except SystemExit as e:
            d = e.args[0] if e.args else str(e)
            raise HTTPException(status_code=400, detail=str(d)) from e
        bad, reason = is_likely_system_disk(None, str(mp))
        if bad:
            raise HTTPException(status_code=400, detail=f"Safety: {reason}")
        started = _now_iso()
        run_id = str(uuid.uuid4())
        out = repair_volume_record(str(mp))
        finished = _now_iso()
        rec = RunRecord(
            run_id=run_id,
            started_at=started,
            finished_at=finished,
            host_os=sys.platform,
            test_mode="repair",
            device_path=None,
            mount_point=str(mp),
            identity=out["identity"],
            result=out["result"],  # type: ignore[arg-type]
            summary=out["summary"],
            error_detail=out.get("error_detail"),
            operator_notes=None,
        )
        payload = rec.to_json()
        persist_run(payload, db_path=db_path, jsonl_path=jsonl_path)
        write_report(report_path, jsonl_path=jsonl_path if jsonl_path.is_file() else None, db_path=db_path)
        return JSONResponse(payload)

    # Mount static files last (catch-all); theme is also inlined in HTML for reliability
    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app
