from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import typer

from sd_health.format_disk import (
    format_guide_text,
    normalize_fs,
    run_macos_format,
    supported_execute_platform,
)
from sd_health.full_test import full_test_record
from sd_health.info import list_removable_candidates
from sd_health.macos_verify import repair_volume_record, verify_volume_record
from sd_health.models import RunRecord
from sd_health.persist import persist_run
from sd_health.quick_test import cam_stress_record, quick_test_record
from sd_health.report import write_report
from sd_health.safety import is_likely_system_disk, validate_mount_point, validate_raw_read_device

app = typer.Typer(
    no_args_is_help=True,
    help="Storage Health — removable storage testing: quick check, full F3 test, web dashboard, HTML report.",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_jsonl() -> Path:
    return Path.cwd() / "results.jsonl"


def _default_db() -> Path:
    return Path.cwd() / "storage_health.db"


@app.command("format")
def cmd_format(
    fs: str = typer.Option("exfat", "--fs", help="exfat or fat32 (typical for cameras & dashcams)"),
    name: str = typer.Option("CAMCARD", "--name", "-n", help="Volume label after format"),
    example: str = typer.Option(
        "/dev/disk4",
        "--example",
        help="Device path shown in the printed guide (macOS placeholder)",
    ),
    device: str | None = typer.Option(
        None,
        "--device",
        "-d",
        help="macOS: whole disk e.g. /dev/disk4 (required with --execute)",
    ),
    execute: bool = typer.Option(
        False,
        "--execute",
        help="Actually run diskutil eraseDisk (macOS only; destroys all data)",
    ),
    i_know: bool = typer.Option(
        False,
        "--i-know",
        help="Required with --execute: you accept wiping the chosen disk",
    ),
) -> None:
    """Print cross-platform format commands (Windows / Linux / macOS). Optional macOS erase via diskutil."""
    try:
        fs_n = normalize_fs(fs)
    except ValueError as e:
        raise typer.Exit(str(e))
    if execute:
        if not supported_execute_platform():
            raise typer.Exit(
                "Automated --execute is only implemented on macOS. "
                "Run without --execute to print commands for Windows/Linux."
            )
        if not device:
            raise typer.Exit("--execute requires --device /dev/diskN")
        if not i_know:
            raise typer.Exit("Refusing to erase without --i-know (this destroys all data on the disk).")
        code, out, _ = run_macos_format(device, fs_n, name)
        typer.echo(out)
        if code != 0:
            raise typer.Exit(code)
        typer.echo("Format finished.")
        return
    typer.echo(format_guide_text(example, fs_n, name))


@app.command("list")
def cmd_list() -> None:
    """List removable/USB disk candidates (SD, USB, external drives; heuristic)."""
    from sd_health.storage_identity import enrich_candidate_row

    rows = list_removable_candidates()
    for i, r in enumerate(rows, 1):
        if "error" in r:
            typer.echo(f"{i}. ERROR: {r.get('error')}")
            continue
        er = enrich_candidate_row(dict(r), {})
        cap = er.get("capacity_label") or "—"
        typer.echo(
            f"{i}. device={r.get('device')} mount={r.get('mount_point')} "
            f"name={r.get('volume_name')} cap={cap} "
            f"model={r.get('model') or r.get('friendly_name')}"
        )


@app.command("verify")
def cmd_verify(
    mount: str = typer.Option(
        ...,
        "--mount",
        "-m",
        help="Mounted volume path, e.g. /Volumes/Untitled",
    ),
    notes: str | None = typer.Option(None, "--notes", "-n", help="Operator label"),
    jsonl: Path | None = typer.Option(None, "--jsonl", help="JSONL log file", show_default=False),
    db: Path | None = typer.Option(None, "--db", help="SQLite database path", show_default=False),
) -> None:
    """macOS: run diskutil verifyVolume (read-only); logged like other tests with test_mode=verify."""
    if sys.platform != "darwin":
        raise typer.Exit("verify only runs on macOS (uses diskutil).")
    mp = validate_mount_point(mount)
    bad, reason = is_likely_system_disk(None, str(mp))
    if bad:
        raise typer.Exit(f"Safety: {reason}")
    log_path = jsonl or _default_jsonl()
    db_path = db or _default_db()
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
        operator_notes=notes,
    )
    persist_run(rec.to_json(), db_path=db_path, jsonl_path=log_path)
    typer.echo(f"Result: {rec.result} — {rec.summary[:500]}")
    if rec.error_detail:
        typer.echo("---")
        typer.echo(str(rec.error_detail)[-4000:])
    typer.echo(f"Logged to {db_path} (and {log_path})")
    if rec.result != "pass":
        raise typer.Exit(1)


@app.command("repair-volume")
def cmd_repair_volume(
    mount: str = typer.Option(
        ...,
        "--mount",
        "-m",
        help="Mounted volume path, e.g. /Volumes/Untitled",
    ),
    notes: str | None = typer.Option(None, "--notes", "-n", help="Operator label"),
    jsonl: Path | None = typer.Option(None, "--jsonl", help="JSONL log file", show_default=False),
    db: Path | None = typer.Option(None, "--db", help="SQLite database path", show_default=False),
) -> None:
    """macOS: run diskutil repairVolume (First Aid–style repair); logged with test_mode=repair."""
    if sys.platform != "darwin":
        raise typer.Exit("repair-volume only runs on macOS (uses diskutil).")
    mp = validate_mount_point(mount)
    bad, reason = is_likely_system_disk(None, str(mp))
    if bad:
        raise typer.Exit(f"Safety: {reason}")
    log_path = jsonl or _default_jsonl()
    db_path = db or _default_db()
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
        operator_notes=notes,
    )
    persist_run(rec.to_json(), db_path=db_path, jsonl_path=log_path)
    typer.echo(f"Result: {rec.result} — {rec.summary[:500]}")
    if rec.error_detail:
        typer.echo("---")
        typer.echo(str(rec.error_detail)[-4000:])
    typer.echo(f"Logged to {db_path} (and {log_path})")
    if rec.result != "pass":
        raise typer.Exit(1)


@app.command("quick")
def cmd_quick(
    mount: str | None = typer.Option(None, "--mount", "-m", help="Mounted volume path to test"),
    device: str | None = typer.Option(None, "--device", "-d", help="Block device (for identity / raw read)"),
    raw_read: bool = typer.Option(False, "--raw-read", help="Read entire device to null (non-destructive, slow)"),
    i_know: bool = typer.Option(False, "--i-know", help="Confirm correct target for risky operations"),
    notes: str | None = typer.Option(None, "--notes", "-n", help="Operator label, e.g. sticker id"),
    sample_mib: int = typer.Option(
        4,
        "--sample-mib",
        help="Quick test file size in MiB (1–512); larger gives steadier speed numbers (uses more RAM)",
    ),
    jsonl: Path | None = typer.Option(None, "--jsonl", help="JSONL log file", show_default=False),
    db: Path | None = typer.Option(None, "--db", help="SQLite database path", show_default=False),
) -> None:
    """Quick non-destructive test (temp file on mount) or optional full-device read."""
    log_path = jsonl or _default_jsonl()
    db_path = db or _default_db()
    started = _now_iso()
    run_id = str(uuid.uuid4())

    if raw_read:
        if not device:
            raise typer.Exit("raw-read requires --device")
        validate_raw_read_device(device, i_know)
        bad, reason = is_likely_system_disk(device, None)
        if bad:
            raise typer.Exit(f"Safety: {reason}")
    else:
        if not mount:
            raise typer.Exit("Provide --mount for quick file test, or use --raw-read with --device")
        mp = validate_mount_point(mount)
        bad, reason = is_likely_system_disk(device, str(mp))
        if bad:
            raise typer.Exit(f"Safety: {reason}")

    try:
        out = quick_test_record(
            mount_point=str(mount) if mount else None,
            device_path=device,
            raw_read=raw_read,
            i_know=i_know,
            sample_mib=sample_mib,
        )
    except ValueError as e:
        raise typer.Exit(str(e))

    finished = _now_iso()
    rec = RunRecord(
        run_id=run_id,
        started_at=started,
        finished_at=finished,
        host_os=sys.platform,
        test_mode="quick",
        device_path=device,
        mount_point=mount,
        identity=out["identity"],
        result=out["result"],  # type: ignore[arg-type]
        summary=out["summary"],
        error_detail=out.get("error_detail"),
        operator_notes=notes,
    )
    persist_run(rec.to_json(), db_path=db_path, jsonl_path=log_path)
    typer.echo(f"Result: {rec.result} — {rec.summary}")
    typer.echo(f"Logged to {db_path} (and {log_path})")


@app.command("cam-stress")
def cmd_cam_stress(
    mount: str = typer.Option(..., "--mount", "-m", help="Mounted volume path to stress"),
    device: str | None = typer.Option(None, "--device", "-d", help="Optional block device for identity metadata"),
    duration_min: int = typer.Option(
        1,
        "--duration-min",
        help="Stress duration in minutes (1–240); longer catches camera write dropouts",
    ),
    chunk_mib: int = typer.Option(
        16,
        "--chunk-mib",
        help="Sequential write chunk size in MiB (1–64)",
    ),
    profile: str | None = typer.Option(
        None,
        "--profile",
        help="Target camera mode for minimum sustained write check: hd|fhd|2k|4k|8k",
    ),
    min_write_mib_s: float | None = typer.Option(
        None,
        "--min-write-mib-s",
        help="Override minimum required sustained write speed (MiB/s)",
    ),
    notes: str | None = typer.Option(None, "--notes", "-n", help="Operator label"),
    jsonl: Path | None = typer.Option(None, "--jsonl", help="JSONL log file", show_default=False),
    db: Path | None = typer.Option(None, "--db", help="SQLite database path", show_default=False),
) -> None:
    """Camera-style sustained write/read stress test (non-destructive temp file)."""
    log_path = jsonl or _default_jsonl()
    db_path = db or _default_db()
    started = _now_iso()
    run_id = str(uuid.uuid4())
    mp = validate_mount_point(mount)
    bad, reason = is_likely_system_disk(device, str(mp))
    if bad:
        raise typer.Exit(f"Safety: {reason}")

    try:
        out = cam_stress_record(
            mount_point=str(mp),
            device_path=device,
            duration_min=duration_min,
            chunk_mib=chunk_mib,
            profile=profile,
            min_write_mib_s=min_write_mib_s,
        )
    except ValueError as e:
        raise typer.Exit(str(e))

    finished = _now_iso()
    rec = RunRecord(
        run_id=run_id,
        started_at=started,
        finished_at=finished,
        host_os=sys.platform,
        test_mode="cam_stress",
        device_path=device,
        mount_point=str(mp),
        identity=out["identity"],
        result=out["result"],  # type: ignore[arg-type]
        summary=out["summary"],
        error_detail=out.get("error_detail"),
        operator_notes=notes,
    )
    persist_run(rec.to_json(), db_path=db_path, jsonl_path=log_path)
    typer.echo(f"Result: {rec.result} — {rec.summary[:500]}")
    if rec.error_detail:
        typer.echo("---")
        typer.echo(str(rec.error_detail)[-4000:])
    typer.echo(f"Logged to {db_path} (and {log_path})")
    if rec.result != "pass":
        raise typer.Exit(1)


@app.command("full")
def cmd_full(
    mount: str = typer.Option(..., "--mount", "-m", help="Mounted volume path (F3 writes here)"),
    device: str | None = typer.Option(None, "--device", "-d", help="Optional device path for identity metadata"),
    timeout: int | None = typer.Option(None, "--timeout", help="Per-phase timeout in seconds"),
    i_know: bool = typer.Option(False, "--i-know", help="Confirm this WILL ERASE data on the volume"),
    notes: str | None = typer.Option(None, "--notes", "-n", help="Operator label"),
    jsonl: Path | None = typer.Option(None, "--jsonl", help="JSONL log file"),
    db: Path | None = typer.Option(None, "--db", help="SQLite database path", show_default=False),
) -> None:
    """Destructive full-card test using F3 (f3write + f3read). Erases free space / fills card."""
    if not i_know:
        raise typer.Exit(
            "Full test overwrites the card contents (F3). Re-run with --i-know after backup."
        )
    log_path = jsonl or _default_jsonl()
    db_path = db or _default_db()
    started = _now_iso()
    run_id = str(uuid.uuid4())
    mp = validate_mount_point(mount)
    bad, reason = is_likely_system_disk(device, str(mp))
    if bad:
        raise typer.Exit(f"Safety: {reason}")

    out = full_test_record(str(mp), device, timeout)
    finished = _now_iso()
    rec = RunRecord(
        run_id=run_id,
        started_at=started,
        finished_at=finished,
        host_os=sys.platform,
        test_mode="full",
        device_path=device,
        mount_point=str(mp),
        identity=out["identity"],
        result=out["result"],  # type: ignore[arg-type]
        summary=out["summary"],
        error_detail=out.get("error_detail"),
        operator_notes=notes,
    )
    persist_run(rec.to_json(), db_path=db_path, jsonl_path=log_path)
    typer.echo(f"Result: {rec.result} — {rec.summary[:500]}")
    typer.echo(f"Logged to {db_path} (and {log_path})")


@app.command("report")
def cmd_report(
    jsonl: Path | None = typer.Option(None, "--jsonl", help="JSONL log file"),
    db: Path | None = typer.Option(None, "--db", help="SQLite database path", show_default=False),
    out: Path = typer.Option(Path("report.html"), "--out", "-o", help="Output HTML path"),
) -> None:
    """Write a standalone HTML copy of run history (for email/USB); use the dashboard as the main report."""
    jl = jsonl or _default_jsonl()
    db_path = db or _default_db()
    write_report(out, jsonl_path=jl if jl.is_file() else None, db_path=db_path)
    typer.echo(f"Wrote {out}")


@app.command("serve")
def cmd_serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address (use 127.0.0.1 only for local use)"),
    port: int = typer.Option(5003, "--port", "-p", help="HTTP port"),
    jsonl: Path | None = typer.Option(None, "--jsonl", help="JSONL log file"),
    report: Path | None = typer.Option(None, "--report", "-r", help="Static report.html output path"),
    db: Path | None = typer.Option(None, "--db", help="SQLite database path", show_default=False),
) -> None:
    """Run local web dashboard (FastAPI + uvicorn) on http://host:port/"""
    import uvicorn

    from sd_health.web import create_app

    jl = jsonl or _default_jsonl()
    rp = report or (Path.cwd() / "report.html")
    db_path = db or _default_db()
    web_app = create_app(jl, rp, db_path)
    typer.echo(f"Open http://{host}:{port}/ in your browser")
    uvicorn.run(web_app, host=host, port=port, log_level="info")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
