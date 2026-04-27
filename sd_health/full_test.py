from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from sd_health.info import collect_identity_for_path


def find_f3() -> tuple[str | None, str | None]:
    """Returns (f3write_path, f3read_path) or (None, None)."""
    w = shutil.which("f3write")
    r = shutil.which("f3read")
    return w, r


def run_f3_full_test(mount_point: str, timeout_sec: int | None) -> tuple[str, str | None]:
    """
    Run f3write then f3read on mount_point directory. Destructive: fills free space.
    """
    root = Path(mount_point).resolve()
    if not root.is_dir():
        return "error", f"not a directory: {root}"

    f3w, f3r = find_f3()
    if not f3w or not f3r:
        return (
            "error",
            "f3write/f3read not found on PATH. Install F3 (e.g. brew install f3 on macOS).",
        )

    timeout = timeout_sec or 86400 * 7
    out_log: list[str] = []

    for exe, phase in ((f3w, "write"), (f3r, "read")):
        cmd = [exe, str(root)]
        try:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(root),
            )
            chunk = (p.stdout or "") + "\n" + (p.stderr or "")
            out_log.append(f"=== f3 {phase} ===\n{chunk}")
            if p.returncode != 0:
                return "fail", "\n".join(out_log)
        except subprocess.TimeoutExpired:
            return "error", f"f3 {phase} timed out"
        except OSError as e:
            return "error", str(e)

    return "pass", None


def full_test_record(mount_point: str, device_path: str | None, timeout_sec: int | None) -> dict:
    summary, err = run_f3_full_test(mount_point, timeout_sec)
    identity = collect_identity_for_path(device_path, mount_point)
    if summary == "pass" and err is None:
        result = "pass"
    elif summary == "error":
        result = "error"
    else:
        result = "fail"
    line = "F3 full write/read completed OK" if result == "pass" else (err or summary)
    return {
        "identity": identity,
        "result": result,
        "summary": line[:8000] if line else "",
        "error_detail": err if result != "pass" else None,
    }
