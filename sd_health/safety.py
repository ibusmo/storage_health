from __future__ import annotations

import re
import sys
from pathlib import Path


def require_confirmation(i_know: bool, message: str) -> None:
    if not i_know:
        raise SystemExit(
            f"{message}\nRe-run with --i-know to confirm you selected the correct removable device."
        )


def validate_mount_point(mount: str) -> Path:
    p = Path(mount).expanduser().resolve()
    if not p.is_dir():
        raise SystemExit(f"Mount point is not a directory: {p}")
    return p


def is_likely_system_disk(device_path: str | None, mount_point: str | None) -> tuple[bool, str]:
    """
    Heuristic only — refuse obvious system paths. Not a security boundary.
    """
    if sys.platform == "darwin":
        if mount_point:
            mp = str(Path(mount_point).resolve())
            if mp in ("/", "/System/Volumes/Data", "/System/Volumes/Preboot"):
                return True, "refusing root/system volume"
            if mp.startswith("/Volumes/Macintosh HD") or mp == "/Volumes/Macintosh HD":
                return True, "refusing typical macOS system volume name"
        if device_path:
            if re.match(r"^/dev/disk0", device_path):
                return True, "refusing disk0 (typically internal)"
    if sys.platform == "linux":
        if mount_point in ("/", "/boot", "/boot/efi"):
            return True, "refusing root/boot mount"
        if device_path and re.match(r"^/dev/(nvme|sd[a-z])$", device_path):
            # Could be USB sdX — don't block solely on pattern; check mount
            pass
    if sys.platform == "win32":
        if mount_point and mount_point.upper().startswith("C:\\"):
            return True, "refusing C: drive"
    return False, ""


def validate_raw_read_device(device_path: str, i_know: bool) -> None:
    require_confirmation(
        i_know,
        "Raw read will stress-read the entire block device and may take a long time.",
    )
    if sys.platform == "darwin":
        if not re.match(r"^/dev/r?disk\d+", device_path):
            raise SystemExit("Expected /dev/diskN or /dev/rdiskN on macOS")
        if re.match(r"^/dev/r?disk0", device_path):
            raise SystemExit("Refusing disk0")
    elif sys.platform == "linux":
        if not re.match(r"^/dev/sd[a-z]+$", device_path) and not re.match(
            r"^/dev/mmcblk\d+$", device_path
        ):
            raise SystemExit("Expected /dev/sdX or /dev/mmcblkN on Linux")
    elif sys.platform == "win32":
        if not re.match(r"^\\\\\.\\PhysicalDrive\d+$", device_path, re.I):
            raise SystemExit(r"Expected \\.\PhysicalDriveN on Windows")
    else:
        raise SystemExit(f"Raw read not supported on {sys.platform}")
