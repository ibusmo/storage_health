from __future__ import annotations

import sys
from typing import Any, Literal

from sd_health.info import _run_capture


def verify_volume_record(mount_point: str, *, timeout: int = 7200) -> dict[str, Any]:
    """
    Run diskutil verifyVolume and return identity + RunRecord fields (pass/fail/error).
    """
    return _volume_maintenance_record(mount_point, op="verify", timeout=timeout)


def repair_volume_record(mount_point: str, *, timeout: int = 7200) -> dict[str, Any]:
    """
    Run diskutil repairVolume — filesystem repair pass (Disk Utility First Aid repair path).
    """
    return _volume_maintenance_record(mount_point, op="repair", timeout=timeout)


def _volume_maintenance_record(
    mount_point: str, *, op: Literal["verify", "repair"], timeout: int = 7200
) -> dict[str, Any]:
    from sd_health.info import collect_identity_for_path

    mp = mount_point.strip()
    out = (
        verify_volume(mp, timeout=timeout)
        if op == "verify"
        else repair_volume(mp, timeout=timeout)
    )
    raw_id = collect_identity_for_path(None, mp) if mp else {}
    identity: dict[str, Any] = dict(raw_id) if isinstance(raw_id, dict) else {}
    ex = identity.setdefault("extra", {})
    if isinstance(ex, dict):
        key = "volume_verify" if op == "verify" else "volume_repair"
        ex[key] = {
            "tool": out.get("tool"),
            "diskutil_result": out.get("result"),
        }
    r = out.get("result")
    if r not in ("pass", "fail", "error"):
        r = "error"
    return {
        "identity": identity,
        "result": r,
        "summary": out.get("summary") or "",
        "error_detail": out.get("detail"),
    }


def verify_volume(mount_point: str, *, timeout: int = 7200) -> dict[str, Any]:
    """
    Run ``diskutil verifyVolume`` — read-only filesystem check (same family of checks
    as Disk Utility “First Aid” verification; does not repair).
    """
    return _diskutil_volume_cmd(mount_point, verb="verifyVolume", timeout=timeout)


def repair_volume(mount_point: str, *, timeout: int = 7200) -> dict[str, Any]:
    """
    Run ``diskutil repairVolume`` — repair pass (same CLI family as Disk Utility First Aid repair).
    """
    return _diskutil_volume_cmd(mount_point, verb="repairVolume", timeout=timeout)


def _diskutil_volume_cmd(
    mount_point: str, *, verb: Literal["verifyVolume", "repairVolume"], timeout: int = 7200
) -> dict[str, Any]:
    tool = f"diskutil {verb}"
    if sys.platform != "darwin":
        return {
            "result": "error",
            "summary": f"Requires macOS ({tool}).",
            "detail": None,
            "tool": tool,
        }
    mp = mount_point.strip()
    if not mp:
        return {
            "result": "error",
            "summary": "Mount path required.",
            "detail": None,
            "tool": tool,
        }
    code, out, err = _run_capture(["diskutil", verb, mp], timeout=timeout)
    blob = (out or "").strip()
    if err and err.strip():
        blob = blob + ("\n" + err.strip() if blob else err.strip())
    tail = blob[-8000:] if len(blob) > 8000 else blob
    if code == 0:
        lines = blob.splitlines()
        last_line = lines[-1] if lines else "finished"
        return {
            "result": "pass",
            "summary": f"{tool}: OK — {last_line[:220]}",
            "detail": tail or None,
            "tool": tool,
        }
    return {
        "result": "fail",
        "summary": f"{tool} failed (exit {code})",
        "detail": tail or None,
        "tool": tool,
    }
