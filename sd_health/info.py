from __future__ import annotations

import json
import plistlib
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from sd_health.models import DiskIdentity


def _run_capture(cmd: list[str], timeout: int = 120) -> tuple[int, str, str]:
    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return p.returncode, p.stdout or "", p.stderr or ""
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def collect_identity_for_path(
    device_path: str | None,
    mount_point: str | None,
) -> dict[str, Any]:
    """Best-effort identity for a device or mount; merges OS-specific probes."""
    plat = sys.platform
    if plat == "darwin":
        return _mac_identity(device_path, mount_point)
    if plat == "linux":
        return _linux_identity(device_path, mount_point)
    if plat == "win32":
        return _windows_identity(device_path, mount_point)
    return DiskIdentity(extra={"platform": plat}).to_json()


def list_removable_candidates() -> list[dict[str, Any]]:
    """Return human-readable rows for `storage-health list`."""
    plat = sys.platform
    if plat == "darwin":
        return _mac_list()
    if plat == "linux":
        return _linux_list()
    if plat == "win32":
        return _windows_list()
    return [{"error": f"unsupported platform: {plat}"}]


# --- macOS ---


def _mac_plist_all_disks() -> dict[str, Any] | None:
    code, out, err = _run_capture(["diskutil", "list", "-plist"])
    if code != 0:
        return None
    try:
        return plistlib.loads(out.encode("utf-8"))
    except Exception:
        try:
            return plistlib.loads(out.encode("latin-1"))
        except Exception:
            return None


def _mac_disk_info_plist(disk_id: str) -> dict[str, Any] | None:
    code, out, _ = _run_capture(["diskutil", "info", "-plist", disk_id])
    if code != 0 or not out.strip():
        return None
    try:
        return plistlib.loads(out.encode("utf-8"))
    except Exception:
        try:
            return plistlib.loads(out.encode("latin-1"))
        except Exception:
            return None


def _mac_list() -> list[dict[str, Any]]:
    root = _mac_plist_all_disks()
    if not root:
        return [{"error": "diskutil list -plist failed"}]
    rows: list[dict[str, Any]] = []
    for ent in root.get("AllDisksAndPartitions", []) or []:
        dev = ent.get("DeviceIdentifier")
        if not dev:
            continue
        info = _mac_disk_info_plist(f"/dev/{dev}") or {}
        removable = info.get("RemovableMedia") or info.get("RemovableMediaOrExternalDevice")
        if removable is not True:
            continue
        parts = ent.get("Partitions") or []
        mount = None
        device_id_for_row = dev
        name = info.get("VolumeName") or (parts[0].get("VolumeName") if parts else None)
        for p in parts:
            m = p.get("MountPoint")
            if m:
                mount = m
                part_id = p.get("DeviceIdentifier")
                if part_id:
                    device_id_for_row = part_id
                name = p.get("VolumeName") or name
                break
        cap = info.get("TotalSize")
        fs = None
        vol_uuid = info.get("VolumeUUID")
        if device_id_for_row:
            pinf = _mac_disk_info_plist(f"/dev/{device_id_for_row}")
            if pinf:
                fs = pinf.get("FilesystemName") or pinf.get("Content")
                vol_uuid = pinf.get("VolumeUUID") or vol_uuid
                # Match quick-test / mount identity: use partition volume size, not whole-disk size.
                ts = pinf.get("TotalSize")
                if ts is not None:
                    cap = ts
        rows.append(
            {
                "device": f"/dev/{device_id_for_row}",
                "mount_point": mount,
                "volume_name": name,
                "capacity_bytes": cap,
                "filesystem": fs,
                "removable": True,
                "protocol": info.get("Protocol"),
                "media_name": info.get("MediaName") or info.get("Content"),
                "volume_uuid": vol_uuid,
                "internal": info.get("Internal"),
            }
        )
    return rows


def _normalize_bsd_disk_path(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    if t.startswith("/dev/"):
        return t
    if re.match(r"^disk\d", t):
        return "/dev/" + t
    return ""


def eject_removable_bsd(device: str) -> dict[str, Any]:
    """
    Run ``diskutil eject`` on a BSD path that appears in ``list_removable_candidates()``.
    Returns ``{ok, message, detail}`` (``detail`` is full diskutil output when present).
    """
    if sys.platform != "darwin":
        return {"ok": False, "message": "Eject requires macOS diskutil.", "detail": None}
    want = _normalize_bsd_disk_path(device)
    if not want or not re.match(r"^/dev/disk\d", want):
        return {"ok": False, "message": "Invalid device path.", "detail": None}
    candidates = list_removable_candidates()
    allowed = False
    for r in candidates:
        if r.get("error"):
            continue
        d = _normalize_bsd_disk_path(str(r.get("device") or ""))
        if d == want:
            allowed = True
            break
    if not allowed:
        return {
            "ok": False,
            "message": "Device is not in the current removable list (refusing eject).",
            "detail": None,
        }
    code, out, err = _run_capture(["diskutil", "eject", want], timeout=120)
    blob = (out or "").strip()
    if err and err.strip():
        blob = blob + ("\n" + err.strip() if blob else err.strip())
    if code == 0:
        lines = blob.splitlines()
        last = lines[-1] if lines else "Ejected."
        return {"ok": True, "message": last[:500], "detail": blob or None}
    return {
        "ok": False,
        "message": f"diskutil eject failed (exit {code})",
        "detail": blob or None,
    }


def _mac_identity(device_path: str | None, mount_point: str | None) -> dict[str, Any]:
    identity = DiskIdentity(removable=None)
    extra: dict[str, Any] = {}

    target = device_path
    if mount_point and not target:
        target = _mac_device_for_mount(mount_point)

    if target:
        info = _mac_disk_info_plist(target)
        if info:
            identity.capacity_bytes = info.get("TotalSize")
            identity.filesystem = info.get("FilesystemName") or info.get("Content")
            identity.volume_name = info.get("VolumeName")
            identity.removable = bool(
                info.get("RemovableMedia") or info.get("RemovableMediaOrExternalDevice")
            )
            extra["internal"] = info.get("Internal")
            extra["device_identifier"] = info.get("DeviceIdentifier")
            extra["protocol"] = info.get("Protocol")
            extra["media_name"] = info.get("MediaName") or info.get("Content")
            extra["volume_uuid"] = info.get("VolumeUUID")
            if info.get("DeviceModel"):
                extra["device_model"] = info.get("DeviceModel")

    if mount_point:
        mp = Path(mount_point)
        if mp.is_dir():
            try:
                usage = shutil.disk_usage(str(mp))
                extra["disk_usage"] = {
                    "total": usage.total,
                    "used": usage.used,
                    "free": usage.free,
                }
            except OSError:
                pass

    # USB vendor/model from system_profiler (slow; optional)
    code, sp_out, _ = _run_capture(
        ["system_profiler", "SPUSBDataType", "-json"],
        timeout=60,
    )
    if code == 0 and sp_out:
        try:
            data = json.loads(sp_out)
            extra["usb_hits"] = _mac_match_usb_serial(data, identity.serial)
            hits = extra.get("usb_hits") or []
            if hits and not identity.model:
                h0 = hits[0]
                if isinstance(h0, dict):
                    identity.model = h0.get("name") or identity.model
                    if h0.get("serial"):
                        identity.serial = identity.serial or h0.get("serial")
        except json.JSONDecodeError:
            pass

    identity.extra = extra
    d = identity.to_json()
    if mount_point:
        d.setdefault("extra", {})["mount_point"] = mount_point
    return d


def _mac_device_for_mount(mount: str) -> str | None:
    code, out, _ = _run_capture(["diskutil", "info", "-plist", mount])
    if code != 0 or not out.strip():
        return None
    try:
        info = plistlib.loads(out.encode("utf-8"))
    except Exception:
        return None
    did = info.get("DeviceIdentifier")
    if did:
        return f"/dev/{did}"
    return None


def _mac_match_usb_serial(data: Any, serial: str | None) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            name = obj.get("_name") or obj.get("name")
            if "serial_num" in obj or "serial_number" in obj:
                hits.append(
                    {
                        "name": name,
                        "vendor": obj.get("vendor_id"),
                        "product": obj.get("product_id"),
                        "serial": obj.get("serial_num") or obj.get("serial_number"),
                    }
                )
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for x in obj:
                walk(x)

    walk(data)
    return hits[:20]


# --- Linux ---


def _linux_list() -> list[dict[str, Any]]:
    code, out, _ = _run_capture(
        ["lsblk", "-J", "-o", "NAME,PATH,TYPE,SIZE,RM,MOUNTPOINT,LABEL,FSTYPE,MODEL,SERIAL,TRAN"]
    )
    if code != 0:
        return [{"error": "lsblk failed", "stderr": out}]
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return [{"error": "lsblk JSON parse failed"}]
    rows: list[dict[str, Any]] = []
    for dev in data.get("blockdevices", []) or []:
        if dev.get("rm") is not True and dev.get("rm") != "1":
            continue
        path = dev.get("path")
        mount = dev.get("mountpoint")
        fs = dev.get("fstype")
        vol_label = dev.get("label")
        if not mount and dev.get("children"):
            for ch in dev["children"]:
                m = ch.get("mountpoint")
                if m:
                    mount = m
                    fs = ch.get("fstype") or fs
                    vol_label = ch.get("label") or vol_label
                    break
        size = dev.get("size")
        cap = _parse_size_bytes(size) if isinstance(size, str) else None
        rows.append(
            {
                "device": path,
                "mount_point": mount,
                "volume_name": vol_label,
                "capacity_bytes": cap,
                "filesystem": fs,
                "model": dev.get("model"),
                "serial": dev.get("serial"),
                "removable": True,
                "protocol": dev.get("tran"),
            }
        )
    return rows


def _parse_size_bytes(s: str) -> int | None:
    s = s.strip()
    m = re.match(r"^([\d.]+)\s*([KMGTPE])i?B?$", s, re.I)
    if not m:
        m2 = re.match(r"^([\d.]+)\s*([KMGTPE])$", s, re.I)
        if not m2:
            return None
        num, suf = m2.groups()
    else:
        num, suf = m.groups()
    mult = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5, "E": 1024**6}
    try:
        return int(float(num) * mult[suf.upper()])
    except (KeyError, ValueError):
        return None


def _linux_identity(device_path: str | None, mount_point: str | None) -> dict[str, Any]:
    identity = DiskIdentity(removable=True)
    code, out, _ = _run_capture(
        ["lsblk", "-J", "-o", "NAME,PATH,TYPE,SIZE,RM,MOUNTPOINT,LABEL,FSTYPE,MODEL,SERIAL,TRAN"]
    )
    if code == 0:
        try:
            data = json.loads(out)
        except json.JSONDecodeError:
            data = {}
        for dev in data.get("blockdevices", []) or []:
            if device_path and dev.get("path") == device_path:
                identity = _identity_from_lsblk_dev(dev)
                break
            if mount_point:
                if dev.get("mountpoint") == mount_point:
                    identity = _identity_from_lsblk_dev(dev)
                    break
                for ch in dev.get("children") or []:
                    if ch.get("mountpoint") == mount_point:
                        identity = _identity_from_lsblk_dev(dev, ch)
                        break
    d = identity.to_json()
    if mount_point:
        d.setdefault("extra", {})["mount_point"] = mount_point
    return d


def _identity_from_lsblk_dev(dev: dict[str, Any], part: dict[str, Any] | None = None) -> DiskIdentity:
    p = part or dev
    size = dev.get("size")
    cap = _parse_size_bytes(size) if isinstance(size, str) else None
    return DiskIdentity(
        capacity_bytes=cap,
        filesystem=p.get("fstype"),
        volume_name=p.get("label"),
        model=str(dev.get("model")).strip() if dev.get("model") else None,
        serial=str(dev.get("serial")).strip() if dev.get("serial") else None,
        removable=dev.get("rm") is True or dev.get("rm") == "1",
        extra={"path": dev.get("path"), "protocol": dev.get("tran")},
    )


# --- Windows ---


def _windows_ps_json(script: str) -> Any:
    cmd = [
        "powershell",
        "-NoProfile",
        "-Command",
        script,
    ]
    code, out, err = _run_capture(cmd, timeout=120)
    if code != 0:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def _windows_list() -> list[dict[str, Any]]:
    script = r"""
$disks = Get-Disk | Where-Object { $_.BusType -eq 'USB' -or $_.IsBoot -eq $false }
$rows = @()
foreach ($d in $disks) {
  if (-not $d.IsRemovable -and $d.BusType -ne 'USB') { continue }
  $parts = Get-Partition -DiskNumber $d.Number -ErrorAction SilentlyContinue
  $mount = $null
  foreach ($p in $parts) {
    if ($p.DriveLetter) {
      $mount = $p.DriveLetter + ':\'
      break
    }
  }
  $fs = $null
  if ($mount -and $mount.Length -ge 1) {
    $letter = $mount.Substring(0,1)
    $vol = Get-Volume -DriveLetter $letter -ErrorAction SilentlyContinue
    if ($vol) { $fs = $vol.FileSystemType }
  }
  $rows += [PSCustomObject]@{
    device = '\\.\PhysicalDrive' + $d.Number
    disk_number = $d.Number
    mount_point = $mount
    capacity_bytes = $d.Size
    filesystem = $fs
    friendly_name = $d.FriendlyName
    serial_number = $d.SerialNumber
    removable = $d.IsRemovable
  }
}
$rows | ConvertTo-Json -Depth 6 -Compress
"""
    data = _windows_ps_json(script)
    if data is None:
        return [{"error": "PowerShell Get-Disk failed"}]
    if isinstance(data, dict):
        return [data]
    return list(data) if isinstance(data, list) else []


def _windows_identity(device_path: str | None, mount_point: str | None) -> dict[str, Any]:
    identity = DiskIdentity()
    if device_path and "PhysicalDrive" in device_path:
        m = re.search(r"PhysicalDrive(\d+)", device_path, re.I)
        if m:
            num = m.group(1)
            script = (
                "$d = Get-Disk -Number "
                + num
                + """ -ErrorAction SilentlyContinue
if (-not $d) { '{}' | ConvertTo-Json }
else {
  [PSCustomObject]@{
    capacity_bytes = $d.Size
    model = $d.FriendlyName
    serial = $d.SerialNumber
    removable = $d.IsRemovable
  } | ConvertTo-Json -Compress
}
"""
            )
            data = _windows_ps_json(script)
            if isinstance(data, dict):
                identity.capacity_bytes = data.get("capacity_bytes")
                identity.model = data.get("model")
                identity.serial = data.get("serial")
                identity.removable = data.get("removable")
    if mount_point:
        letter = mount_point[0].upper() if len(mount_point) >= 2 else None
        if letter:
            script = (
                "$vol = Get-Volume -DriveLetter '"
                + letter
                + """' -ErrorAction SilentlyContinue
if (-not $vol) { '{}' | ConvertTo-Json }
else {
  [PSCustomObject]@{
    filesystem = $vol.FileSystemType
    volume_name = $vol.FileSystemLabel
    size_bytes = $vol.Size
  } | ConvertTo-Json -Compress
}
"""
            )
            data = _windows_ps_json(script)
            if isinstance(data, dict) and data:
                identity.filesystem = data.get("filesystem")
                identity.volume_name = data.get("volume_name")
                if not identity.capacity_bytes:
                    identity.capacity_bytes = data.get("size_bytes")
    d = identity.to_json()
    if mount_point:
        d.setdefault("extra", {})["mount_point"] = mount_point
    return d
