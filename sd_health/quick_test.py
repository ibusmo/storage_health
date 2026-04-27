from __future__ import annotations

import hashlib
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

from sd_health.info import collect_identity_for_path

CAM_STRESS_PROFILES: dict[str, dict[str, float]] = {
    # Typical sustained recording bitrate targets (conservative defaults).
    "hd": {"bitrate_mbps": 8.0},
    "fhd": {"bitrate_mbps": 16.0},
    "2k": {"bitrate_mbps": 32.0},
    "4k": {"bitrate_mbps": 60.0},
    "8k": {"bitrate_mbps": 120.0},
}


def _profile_required_write_mib_s(profile: str | None) -> tuple[str, float | None]:
    p = str(profile or "").strip().lower()
    if not p:
        return "", None
    if p not in CAM_STRESS_PROFILES:
        return "", None
    mbps = CAM_STRESS_PROFILES[p]["bitrate_mbps"]
    return p, mbps / 8.0


def run_mounted_quick_test(
    mount_point: str,
    sample_mib: int = 4,
) -> tuple[str, str | None, dict[str, object]]:
    """
    Write a random temp file, fsync, read back, verify hash.
    Records write/read throughput (MiB/s) in returned metrics for integrity + speed.
    Returns (summary, error_detail, metrics).
    """
    root = Path(mount_point).resolve()
    metrics: dict[str, object] = {}
    if not root.is_dir():
        return "fail", f"not a directory: {root}", metrics

    mib = max(1, min(int(sample_mib), 512))
    data = secrets.token_bytes(mib * 1024 * 1024)
    h = hashlib.sha256(data).hexdigest()
    name = f".sd_health_quick_{secrets.token_hex(8)}.bin"
    path = root / name
    try:
        t0 = time.perf_counter()
        with path.open("wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        t1 = time.perf_counter()
        with path.open("rb") as f:
            read_back = f.read()
        t2 = time.perf_counter()
        ws = t1 - t0
        rs = t2 - t1
        metrics = {
            "sample_mib": mib,
            "write_seconds": round(ws, 4),
            "read_seconds": round(rs, 4),
            "write_mib_s": round(mib / ws, 2) if ws > 0 else None,
            "read_mib_s": round(mib / rs, 2) if rs > 0 else None,
        }
        if hashlib.sha256(read_back).hexdigest() != h:
            return "fail", "hash mismatch after read", metrics
        return "pass", None, metrics
    except OSError as e:
        return "error", str(e), metrics
    finally:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def run_camera_stress_test(
    mount_point: str,
    duration_min: int = 1,
    chunk_mib: int = 16,
    verify_every: int = 8,
    progress_cb: Callable[[dict[str, object]], None] | None = None,
) -> tuple[str, str | None, dict[str, object]]:
    """
    Camera-style stress: sustained sequential write + periodic readback checks,
    then full sequential read of written data.
    Returns (summary, error_detail, metrics).
    """
    root = Path(mount_point).resolve()
    metrics: dict[str, object] = {}
    if not root.is_dir():
        return "fail", f"not a directory: {root}", metrics

    mins = max(1, min(int(duration_min), 240))
    cmib = max(1, min(int(chunk_mib), 64))
    ve = max(1, min(int(verify_every), 64))
    chunk_bytes = cmib * 1024 * 1024
    name = f".sd_health_cam_stress_{secrets.token_hex(8)}.bin"
    path = root / name
    checks = 0
    t0 = time.perf_counter()
    total_written = 0
    try:
        with path.open("wb+") as f:
            idx = 0
            end_at = t0 + mins * 60
            while time.perf_counter() < end_at:
                data = secrets.token_bytes(chunk_bytes)
                h = hashlib.sha256(data).hexdigest()
                offset = total_written
                tw0 = time.perf_counter()
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
                tw1 = time.perf_counter()
                total_written += len(data)
                idx += 1
                chunk_write_mib_s = (cmib / (tw1 - tw0)) if (tw1 - tw0) > 0 else None
                if progress_cb:
                    elapsed = max(0.0, tw1 - t0)
                    avg_write = (total_written / (1024 * 1024)) / elapsed if elapsed > 0 else None
                    progress_cb(
                        {
                            "phase": "write",
                            "elapsed_sec": round(elapsed, 2),
                            "duration_target_sec": mins * 60,
                            "chunks_done": idx,
                            "written_mib": round(total_written / (1024 * 1024), 2),
                            "current_write_mib_s": round(chunk_write_mib_s, 2) if chunk_write_mib_s else None,
                            "avg_write_mib_s": round(avg_write, 2) if avg_write else None,
                        }
                    )

                if idx % ve == 0:
                    f.seek(offset)
                    back = f.read(len(data))
                    checks += 1
                    if hashlib.sha256(back).hexdigest() != h:
                        metrics = {
                            "duration_min": mins,
                            "chunk_mib": cmib,
                            "verify_every": ve,
                            "verify_checks": checks,
                            "written_mib": round(total_written / (1024 * 1024), 2),
                        }
                        return "fail", f"readback mismatch at chunk {idx}", metrics
                    f.seek(0, os.SEEK_END)

            t1 = time.perf_counter()
            f.seek(0)
            read_total = 0
            while True:
                rr0 = time.perf_counter()
                buf = f.read(4 * 1024 * 1024)
                rr1 = time.perf_counter()
                if not buf:
                    break
                read_total += len(buf)
                if progress_cb:
                    c_mib = len(buf) / (1024 * 1024)
                    cur_read = c_mib / (rr1 - rr0) if (rr1 - rr0) > 0 else None
                    re_elapsed = max(0.0, rr1 - t1)
                    avg_read = (read_total / (1024 * 1024)) / re_elapsed if re_elapsed > 0 else None
                    progress_cb(
                        {
                            "phase": "read",
                            "elapsed_sec": round((rr1 - t0), 2),
                            "read_elapsed_sec": round(re_elapsed, 2),
                            "read_mib": round(read_total / (1024 * 1024), 2),
                            "current_read_mib_s": round(cur_read, 2) if cur_read else None,
                            "avg_read_mib_s": round(avg_read, 2) if avg_read else None,
                        }
                    )
            t2 = time.perf_counter()
        wrote_mib = total_written / (1024 * 1024)
        ws = t1 - t0
        rs = t2 - t1
        metrics = {
            "duration_min": mins,
            "chunk_mib": cmib,
            "verify_every": ve,
            "verify_checks": checks,
            "written_mib": round(wrote_mib, 2),
            "write_seconds": round(ws, 3),
            "read_seconds": round(rs, 3),
            "write_mib_s": round(wrote_mib / ws, 2) if ws > 0 else None,
            "read_mib_s": round(wrote_mib / rs, 2) if rs > 0 else None,
        }
        return "pass", None, metrics
    except OSError as e:
        return "error", str(e), metrics
    finally:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def run_raw_readonly_test(device_path: str, i_know: bool) -> tuple[str, str | None]:
    """
    Read entire device to null sink (non-destructive). Long-running.
    """
    if sys.platform == "darwin":
        dev = device_path if device_path.startswith("/dev/rdisk") else device_path.replace("/dev/disk", "/dev/rdisk")
        cmd = ["dd", f"if={dev}", "of=/dev/null", "bs=4m"]
    elif sys.platform == "linux":
        cmd = ["dd", f"if={device_path}", "of=/dev/null", "bs=4M"]
    elif sys.platform == "win32":
        # PowerShell: read PhysicalDrive in chunks (slower but no extra tools)
        ps = f"""
$path = '{device_path.replace("'", "''")}'
$fs = [System.IO.File]::OpenRead($path)
try {{
  $buf = New-Object byte[] (4MB)
  while ($fs.Read($buf, 0, $buf.Length) -gt 0) {{ }}
}} finally {{ $fs.Close() }}
"""
        cmd = ["powershell", "-NoProfile", "-Command", ps]
    else:
        return "error", f"unsupported platform: {sys.platform}"

    if not i_know:
        return "error", "raw read requires --i-know"

    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=86400,
        )
        if p.returncode != 0:
            err = (p.stderr or "") + (p.stdout or "")
            return "fail", err.strip() or f"exit {p.returncode}"
        return "pass", None
    except subprocess.TimeoutExpired:
        return "error", "timeout"
    except OSError as e:
        return "error", str(e)


def quick_test_record(
    mount_point: str | None,
    device_path: str | None,
    raw_read: bool,
    i_know: bool,
    sample_mib: int = 4,
) -> dict:
    """Returns identity + result fields for RunRecord."""
    if raw_read:
        if not device_path:
            raise ValueError("raw read requires --device")
        summary, err = run_raw_readonly_test(device_path, i_know)
        identity = collect_identity_for_path(device_path, None)
    else:
        if not mount_point:
            raise ValueError("quick test requires --mount (or use --raw-read with --device)")
        summary, err, bench = run_mounted_quick_test(mount_point, sample_mib=sample_mib)
        identity = collect_identity_for_path(device_path, mount_point)
        if isinstance(identity, dict):
            ex = identity.setdefault("extra", {})
            if isinstance(ex, dict) and bench:
                ex["quick_benchmark"] = bench

    if summary == "pass":
        result = "pass"
    elif summary == "error":
        result = "error"
    else:
        result = "fail"
    if result == "pass":
        if isinstance(identity, dict):
            ex = identity.get("extra") if isinstance(identity.get("extra"), dict) else {}
            b = ex.get("quick_benchmark") if isinstance(ex, dict) else None
            if isinstance(b, dict) and b.get("write_mib_s") is not None and b.get("read_mib_s") is not None:
                line = (
                    f"Quick test OK — write ~{b['write_mib_s']} MiB/s, read ~{b['read_mib_s']} MiB/s "
                    f"({b.get('sample_mib', '?')} MiB sample)"
                )
            else:
                line = "Quick test OK (temp file write/read)"
        else:
            line = "Quick test OK (temp file write/read)"
    else:
        line = err or summary
    return {
        "identity": identity,
        "result": result,
        "summary": line,
        "error_detail": err,
    }


def cam_stress_record(
    mount_point: str | None,
    device_path: str | None,
    duration_min: int = 1,
    chunk_mib: int = 16,
    profile: str | None = None,
    min_write_mib_s: float | None = None,
    progress_cb: Callable[[dict[str, object]], None] | None = None,
) -> dict:
    """Returns identity + result fields for camera-style sustained stress test."""
    if not mount_point:
        raise ValueError("camera stress test requires --mount")
    summary, err, bench = run_camera_stress_test(
        mount_point,
        duration_min=duration_min,
        chunk_mib=chunk_mib,
        progress_cb=progress_cb,
    )
    identity = collect_identity_for_path(device_path, mount_point)
    if isinstance(identity, dict):
        ex = identity.setdefault("extra", {})
        if isinstance(ex, dict) and bench:
            ex["cam_stress_benchmark"] = bench

    prof_key, prof_min = _profile_required_write_mib_s(profile)
    req_min = float(min_write_mib_s) if min_write_mib_s is not None else prof_min
    if req_min is not None and req_min < 0:
        req_min = None
    if isinstance(bench, dict):
        bench["profile"] = prof_key or None
        bench["required_min_write_mib_s"] = round(req_min, 2) if req_min is not None else None

    if summary == "pass":
        result = "pass"
    elif summary == "error":
        result = "error"
    else:
        result = "fail"
    if result == "pass":
        b = bench if isinstance(bench, dict) else {}
        w = b.get("write_mib_s")
        if req_min is not None and isinstance(w, (int, float)) and float(w) < req_min:
            result = "fail"
            line = (
                "Cam stress FAIL — "
                f"write ~{w} MiB/s below required ~{round(req_min, 2)} MiB/s"
                + (f" for {prof_key.upper()}" if prof_key else "")
            )
            return {
                "identity": identity,
                "result": result,
                "summary": line,
                "error_detail": None,
            }
        line = (
            "Cam stress OK — "
            f"write ~{b.get('write_mib_s', '?')} MiB/s, read ~{b.get('read_mib_s', '?')} MiB/s "
            f"({b.get('written_mib', '?')} MiB over ~{b.get('duration_min', '?')} min)"
            + (f", profile {prof_key.upper()}" if prof_key else "")
        )
    else:
        line = err or summary
    return {
        "identity": identity,
        "result": result,
        "summary": line,
        "error_detail": err,
    }
