from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sd_health.log import read_jsonl


_FINGERPRINT_SCHEME = "volume_uuid_v1"


def _ensure_runs_fingerprint_column(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(runs)")}
    if "fingerprint" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN fingerprint TEXT")


def _ensure_device_user_columns(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(devices)")}
    if "user_label" not in cols:
        conn.execute("ALTER TABLE devices ADD COLUMN user_label TEXT")
    if "user_notes" not in cols:
        conn.execute("ALTER TABLE devices ADD COLUMN user_notes TEXT")
    if "user_brand" not in cols:
        conn.execute("ALTER TABLE devices ADD COLUMN user_brand TEXT")
    if "user_series" not in cols:
        conn.execute("ALTER TABLE devices ADD COLUMN user_series TEXT")


def _migrate_legacy_db_name(db_path: Path) -> None:
    """If default DB was renamed to storage_health.db, move sd_health.db → storage_health.db once."""
    try:
        resolved = db_path.resolve()
    except OSError:
        return
    if resolved.name != "storage_health.db":
        return
    if resolved.is_file():
        return
    legacy = resolved.parent / "sd_health.db"
    if legacy.is_file():
        try:
            legacy.rename(resolved)
        except OSError:
            pass


def get_conn(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path) -> None:
    _migrate_legacy_db_name(db_path)
    with get_conn(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id         TEXT PRIMARY KEY,
                started_at     TEXT NOT NULL,
                finished_at    TEXT NOT NULL,
                host_os        TEXT,
                test_mode      TEXT NOT NULL,
                device_path    TEXT,
                mount_point    TEXT,
                identity_json  TEXT NOT NULL,
                result         TEXT NOT NULL,
                summary        TEXT NOT NULL,
                error_detail   TEXT,
                operator_notes TEXT,
                fingerprint    TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_runs_finished ON runs(finished_at DESC);

            CREATE TABLE IF NOT EXISTS devices (
                fingerprint     TEXT PRIMARY KEY,
                first_seen      TEXT NOT NULL,
                last_seen       TEXT NOT NULL,
                run_count       INTEGER NOT NULL DEFAULT 0,
                vendor          TEXT,
                model           TEXT,
                serial          TEXT,
                capacity_bytes  INTEGER,
                media_type      TEXT,
                brand           TEXT,
                series          TEXT,
                identity_json   TEXT,
                user_label      TEXT,
                user_notes      TEXT,
                user_brand      TEXT,
                user_series     TEXT
            );
            CREATE TABLE IF NOT EXISTS app_meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        _ensure_runs_fingerprint_column(conn)
        _ensure_device_user_columns(conn)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_runs_fingerprint ON runs(fingerprint)"
        )
        _migrate_fingerprint_scheme(conn)
        conn.commit()


def _merge_pref_text(old: str | None, new: str | None) -> str:
    a = (old or "").strip()
    b = (new or "").strip()
    return b or a


def _parse_identity_from_run_row(row: sqlite3.Row) -> dict[str, Any]:
    try:
        ident = json.loads(row["identity_json"] or "{}")
    except json.JSONDecodeError:
        ident = {}
    if not isinstance(ident, dict):
        ident = {}
    ident = dict(ident)
    ex0 = ident.get("extra")
    if isinstance(ex0, str):
        try:
            ex0 = json.loads(ex0)
        except json.JSONDecodeError:
            ex0 = {}
    if not isinstance(ex0, dict):
        ex0 = {}
    else:
        ex0 = dict(ex0)
    mp = row["mount_point"]
    if mp and not ex0.get("mount_point"):
        ex0["mount_point"] = mp
    dp = row["device_path"]
    if dp and not ex0.get("device_identifier"):
        s = str(dp).replace("/dev/", "").strip()
        if s:
            ex0["device_identifier"] = s
    ident["extra"] = ex0
    return ident


def _migrate_fingerprint_scheme(conn: sqlite3.Connection) -> None:
    from sd_health.storage_identity import enrich_identity

    got = conn.execute("SELECT value FROM app_meta WHERE key='fingerprint_scheme'").fetchone()
    if got and (got[0] or "") == _FINGERPRINT_SCHEME:
        return

    run_rows = conn.execute(
        "SELECT run_id, started_at, finished_at, device_path, mount_point, identity_json FROM runs"
    ).fetchall()

    # Keep user labels/notes/brand/series and remap them to the new fingerprint.
    old_prefs: dict[str, dict[str, str]] = {}
    old_pref_identity: dict[str, dict[str, Any]] = {}
    old_device_rows = conn.execute(
        "SELECT fingerprint, user_label, user_notes, user_brand, user_series, identity_json FROM devices"
    ).fetchall()
    for d in old_device_rows:
        fp_old = str(d["fingerprint"] or "").strip()
        if not fp_old:
            continue
        try:
            old_ident = json.loads(d["identity_json"] or "{}")
        except json.JSONDecodeError:
            old_ident = {}
        if not isinstance(old_ident, dict):
            old_ident = {}
        if old_ident:
            old_ident = enrich_identity(old_ident)
        fp_new = str(old_ident.get("fingerprint") or fp_old).strip()
        if not fp_new:
            continue
        prev = old_prefs.get(fp_new, {})
        old_prefs[fp_new] = {
            "user_label": _merge_pref_text(prev.get("user_label"), d["user_label"]),
            "user_notes": _merge_pref_text(prev.get("user_notes"), d["user_notes"]),
            "user_brand": _merge_pref_text(prev.get("user_brand"), d["user_brand"]),
            "user_series": _merge_pref_text(prev.get("user_series"), d["user_series"]),
        }
        if isinstance(old_ident, dict) and old_ident and fp_new not in old_pref_identity:
            old_pref_identity[fp_new] = old_ident

    agg: dict[str, dict[str, Any]] = {}
    for r in run_rows:
        ident = enrich_identity(_parse_identity_from_run_row(r))
        fp = str(ident.get("fingerprint") or "").strip()
        conn.execute(
            "UPDATE runs SET identity_json=?, fingerprint=? WHERE run_id=?",
            (json.dumps(ident, ensure_ascii=False), fp or None, r["run_id"]),
        )
        if not fp:
            continue
        ts_first = r["started_at"] or r["finished_at"] or ""
        ts_last = r["finished_at"] or r["started_at"] or ""
        cur = agg.get(fp)
        if cur is None:
            agg[fp] = {
                "first_seen": ts_first,
                "last_seen": ts_last,
                "run_count": 1,
                "identity": ident,
            }
        else:
            cur["run_count"] = int(cur["run_count"]) + 1
            if ts_first and (not cur["first_seen"] or ts_first < cur["first_seen"]):
                cur["first_seen"] = ts_first
            if ts_last and (not cur["last_seen"] or ts_last > cur["last_seen"]):
                cur["last_seen"] = ts_last
            # Keep latest identity by finished timestamp.
            if ts_last and ts_last >= str(cur["last_seen"] or ""):
                cur["identity"] = ident

    conn.execute("DELETE FROM devices")
    for fp, a in agg.items():
        ident = a["identity"] if isinstance(a.get("identity"), dict) else {}
        prefs = old_prefs.get(fp, {})
        conn.execute(
            """INSERT INTO devices (
                fingerprint, first_seen, last_seen, run_count, vendor, model, serial,
                capacity_bytes, media_type, brand, series, identity_json,
                user_label, user_notes, user_brand, user_series
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                fp,
                a.get("first_seen") or datetime.now(timezone.utc).isoformat(),
                a.get("last_seen") or datetime.now(timezone.utc).isoformat(),
                int(a.get("run_count") or 0),
                ident.get("vendor"),
                ident.get("model"),
                ident.get("serial"),
                ident.get("capacity_bytes"),
                ident.get("media_type"),
                ident.get("brand"),
                ident.get("series"),
                json.dumps(ident, ensure_ascii=False),
                prefs.get("user_label", ""),
                prefs.get("user_notes", ""),
                prefs.get("user_brand", ""),
                prefs.get("user_series", ""),
            ),
        )

    # Preserve labels for fingerprints with no current runs.
    for fp, prefs in old_prefs.items():
        if fp in agg:
            continue
        ident = old_pref_identity.get(fp, {})
        conn.execute(
            """INSERT INTO devices (
                fingerprint, first_seen, last_seen, run_count, vendor, model, serial,
                capacity_bytes, media_type, brand, series, identity_json,
                user_label, user_notes, user_brand, user_series
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                fp,
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
                0,
                ident.get("vendor"),
                ident.get("model"),
                ident.get("serial"),
                ident.get("capacity_bytes"),
                ident.get("media_type"),
                ident.get("brand"),
                ident.get("series"),
                json.dumps(ident, ensure_ascii=False) if ident else "{}",
                prefs.get("user_label", ""),
                prefs.get("user_notes", ""),
                prefs.get("user_brand", ""),
                prefs.get("user_series", ""),
            ),
        )

    conn.execute(
        "INSERT OR REPLACE INTO app_meta(key, value) VALUES ('fingerprint_scheme', ?)",
        (_FINGERPRINT_SCHEME,),
    )


def insert_run(db_path: Path, record: dict[str, Any]) -> None:
    from sd_health.storage_identity import enrich_identity

    init_db(db_path)
    ident = record.get("identity") or {}
    if not isinstance(ident, (dict, list)):
        ident = {}
    if isinstance(ident, dict):
        ident = enrich_identity(ident)
        record["identity"] = ident
    fp = ident.get("fingerprint") if isinstance(ident, dict) else None
    row = (
        record["run_id"],
        record["started_at"],
        record["finished_at"],
        record.get("host_os"),
        record["test_mode"],
        record.get("device_path"),
        record.get("mount_point"),
        json.dumps(ident, ensure_ascii=False),
        record["result"],
        record["summary"],
        record.get("error_detail"),
        record.get("operator_notes"),
        fp,
    )
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO runs (
                run_id, started_at, finished_at, host_os, test_mode,
                device_path, mount_point, identity_json, result, summary,
                error_detail, operator_notes, fingerprint
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            row,
        )
        if fp and isinstance(ident, dict):
            _upsert_device(conn, ident, record.get("finished_at") or record.get("started_at"))
        conn.commit()


def list_runs(db_path: Path) -> list[dict[str, Any]]:
    from sd_health.storage_identity import enrich_identity

    _migrate_legacy_db_name(db_path)
    if not db_path.is_file():
        return []
    init_db(db_path)
    with get_conn(db_path) as conn:
        rows = conn.execute("SELECT * FROM runs ORDER BY finished_at DESC").fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        try:
            ident = json.loads(d.pop("identity_json") or "{}")
        except json.JSONDecodeError:
            ident = {}
        if isinstance(ident, dict):
            ident = dict(ident)
            ex0 = ident.get("extra")
            if isinstance(ex0, str):
                try:
                    ex0 = json.loads(ex0)
                except json.JSONDecodeError:
                    ex0 = {}
            if not isinstance(ex0, dict):
                ex0 = {}
            else:
                ex0 = dict(ex0)
            mp = d.get("mount_point")
            if mp and not ex0.get("mount_point"):
                ex0["mount_point"] = mp
            dp = d.get("device_path")
            if dp and not ex0.get("device_identifier"):
                s = str(dp).replace("/dev/", "").strip()
                if s:
                    ex0["device_identifier"] = s
            ident["extra"] = ex0
            d["identity"] = enrich_identity(ident)
            fp_live = d["identity"].get("fingerprint") or ""
            if fp_live:
                d["fingerprint"] = fp_live
                d["identity"]["fingerprint"] = fp_live
                d["identity"]["fingerprint_short"] = fp_live[:12]
                d["fingerprint_short"] = fp_live[:12]
        else:
            d["identity"] = {}
        out.append(d)
    return out


def _upsert_device(conn: sqlite3.Connection, identity: dict[str, Any], ts: str | None) -> None:
    fp = identity.get("fingerprint")
    if not fp or not ts:
        return
    row = conn.execute("SELECT run_count FROM devices WHERE fingerprint=?", (fp,)).fetchone()
    vendor = identity.get("vendor")
    model = identity.get("model")
    serial = identity.get("serial")
    cap = identity.get("capacity_bytes")
    if isinstance(identity.get("extra"), dict):
        ex = identity["extra"]
        vendor = vendor or ex.get("usb_vendor")
        model = model or ex.get("device_model")
    if row:
        conn.execute(
            """UPDATE devices SET last_seen=?, run_count=run_count+1,
               vendor=COALESCE(?,vendor), model=COALESCE(?,model), serial=COALESCE(?,serial),
               capacity_bytes=COALESCE(?,capacity_bytes), media_type=?, brand=?, series=?,
               identity_json=?
            WHERE fingerprint=?""",
            (
                ts,
                vendor,
                model,
                serial,
                cap,
                identity.get("media_type"),
                identity.get("brand"),
                identity.get("series"),
                json.dumps(identity, ensure_ascii=False),
                fp,
            ),
        )
    else:
        conn.execute(
            """INSERT INTO devices (
                fingerprint, first_seen, last_seen, run_count, vendor, model, serial,
                capacity_bytes, media_type, brand, series, identity_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                fp,
                ts,
                ts,
                1,
                vendor,
                model,
                serial,
                cap,
                identity.get("media_type"),
                identity.get("brand"),
                identity.get("series"),
                json.dumps(identity, ensure_ascii=False),
            ),
        )


def set_device_user_prefs(
    db_path: Path,
    fingerprint: str,
    *,
    user_label: str,
    user_notes: str,
    user_brand: str,
    user_series: str,
) -> None:
    """Pen name on card, manual brand/series, and notes (Disk Utility often omits vendor)."""
    if not fingerprint:
        return
    init_db(db_path)
    ts = datetime.now(timezone.utc).isoformat()
    with get_conn(db_path) as conn:
        _ensure_device_user_columns(conn)
        conn.execute(
            """
            INSERT OR IGNORE INTO devices (
                fingerprint, first_seen, last_seen, run_count, identity_json
            ) VALUES (?, ?, ?, 0, '{}')
            """,
            (fingerprint, ts, ts),
        )
        conn.execute(
            """UPDATE devices SET user_label = ?, user_notes = ?, user_brand = ?, user_series = ?
               WHERE fingerprint = ?""",
            (user_label, user_notes, user_brand, user_series, fingerprint),
        )
        conn.commit()


def list_devices(db_path: Path) -> list[dict[str, Any]]:
    from sd_health.storage_identity import enrich_identity

    _migrate_legacy_db_name(db_path)
    if not db_path.is_file():
        return []
    init_db(db_path)
    with get_conn(db_path) as conn:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='devices'"
        ).fetchone():
            return []
        rows = conn.execute(
            "SELECT * FROM devices ORDER BY datetime(last_seen) DESC"
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        try:
            ident = json.loads(d.pop("identity_json") or "{}")
        except json.JSONDecodeError:
            ident = {}
        if isinstance(ident, dict):
            ident = enrich_identity(ident)
            d["identity"] = ident
            fp_live = ident.get("fingerprint")
            if fp_live:
                d["fingerprint"] = fp_live
                d["identity"]["fingerprint_short"] = fp_live[:12]
                d["fingerprint_short"] = fp_live[:12]
        else:
            d["identity"] = {}
        out.append(d)
    return out


def migrate_from_jsonl(db_path: Path, jsonl_path: Path | None) -> int:
    """If DB has no rows but JSONL has data, import. Returns rows imported."""
    init_db(db_path)
    with get_conn(db_path) as conn:
        n = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
    if n > 0 or not jsonl_path or not jsonl_path.is_file():
        return 0
    imported = 0
    for rec in read_jsonl(jsonl_path):
        if rec.get("run_id"):
            insert_run(db_path, rec)
            imported += 1
    return imported
