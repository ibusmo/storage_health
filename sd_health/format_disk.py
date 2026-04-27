from __future__ import annotations

import re
import subprocess
import sys
from typing import Literal

Filesystem = Literal["exfat", "fat32"]


def normalize_fs(fs: str) -> Filesystem:
    s = fs.strip().lower().replace("-", "")
    if s in ("exfat", "exf"):
        return "exfat"
    if s in ("fat32", "msdos", "vfat"):
        return "fat32"
    raise ValueError(f"Unsupported filesystem: {fs!r} (use exfat or fat32)")


def diskutil_fs_name(fs: Filesystem) -> str:
    return "ExFAT" if fs == "exfat" else "MS-DOS FAT32"


def validate_macos_whole_disk(device: str) -> None:
    if not re.match(r"^/dev/disk\d+$", device):
        raise ValueError("macOS whole-disk format expects --device /dev/diskN (not rdisk or partition slice)")
    if re.match(r"^/dev/disk0$", device):
        raise ValueError("Refusing to format disk0 (system disk)")


def shlex_quote(s: str) -> str:
    if not s:
        return "''"
    if re.match(r"^[\w.-]+$", s):
        return s
    return "'" + s.replace("'", "'\"'\"'") + "'"


def build_macos_diskutil_cmd(device: str, fs: Filesystem, volume_name: str) -> list[str]:
    validate_macos_whole_disk(device)
    vn = volume_name.strip() or "UNTITLED"
    if len(vn) > 11 and fs == "fat32":
        vn = vn[:11]
    return [
        "diskutil",
        "eraseDisk",
        diskutil_fs_name(fs),
        vn,
        "MBRFormat",
        device,
    ]


def format_guide_text(device_example: str, fs: Filesystem, volume_name: str) -> str:
    """Copy-paste commands for Windows, Linux, and macOS (camera-friendly exFAT/FAT32)."""
    vn = volume_name.strip() or "CAMCARD"
    v_mac = vn[:11] if fs == "fat32" and len(vn) > 11 else vn
    mac_cmd = (
        f"diskutil eraseDisk {diskutil_fs_name(fs)} {shlex_quote(v_mac)} MBRFormat {device_example}"
    )
    fs_win = "exFAT" if fs == "exfat" else "FAT32"
    lines = [
        "=== Storage Health — format hints (DESTROYS ALL DATA on the target) ===",
        "",
        "Use the correct drive for your system. Unmount/eject the card first where needed.",
        "",
        "--- macOS (whole disk; replace diskN with your disk from `diskutil list`) ---",
        f"  {mac_cmd}",
        "  Or use Disk Utility: select disk → Erase → Scheme: Master Boot Record → Format: "
        + ("ExFAT" if fs == "exfat" else "MS-DOS (FAT)")
        + ".",
        "",
        "--- Windows (Administrator Command Prompt; E: = your card’s drive letter) ---",
        f'  format E: /FS:{fs_win} /V:{vn[:32]} /Q',
        "  Or: Disk Management → right-click partition → Format.",
        "",
        "--- Linux (replace /dev/sdX1 with your partition; unmount first: umount ...) ---",
        (
            "  sudo mkfs.exfat -n "
            + shlex_quote(vn[:15])
            + " /dev/sdX1"
            if fs == "exfat"
            else "  sudo mkfs.vfat -F 32 -n "
            + shlex_quote(vn[:11])
            + " /dev/sdX1"
        ),
        "  Install exfatprogs or exfat-utils if mkfs.exfat is missing.",
        "",
    ]
    return "\n".join(lines)


def run_macos_format(device: str, fs: Filesystem, volume_name: str) -> tuple[int, str, str]:
    cmd = build_macos_diskutil_cmd(device, fs, volume_name)
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    out = (p.stdout or "") + (p.stderr or "")
    return p.returncode, out, ""


def supported_execute_platform() -> bool:
    return sys.platform == "darwin"
