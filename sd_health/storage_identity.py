from __future__ import annotations

import hashlib
import json
import re
import sys
from typing import Any

# First word often matches vendor for major flash brands (longer first — match "western digital" before "wd")
_KNOWN_BRANDS = (
    "sandisk",
    "samsung",
    "kingston",
    "sony",
    "lexar",
    "transcend",
    "pny",
    "integral",
    "toshiba",
    "kioxia",
    "hiksemi",
    "teamgroup",
    "adata",
    "verbatim",
    "crucial",
    "wd",
    "western digital",
)

_BRAND_SORTED = tuple(sorted(_KNOWN_BRANDS, key=len, reverse=True))


def format_capacity(n: Any) -> str:
    try:
        b = int(n) if n is not None else 0
    except (TypeError, ValueError):
        return "—"
    if b <= 0:
        return "—"
    for label, div in (("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if b >= div:
            val = b / div
            s = f"{val:.1f}".rstrip("0").rstrip(".")
            return f"{s} {label}"
    return f"{b} B"


def format_capacity_tooltip(
    capacity_bytes: Any, market_label: str | None = None
) -> str:
    """OS-reported size; optional market/sticker line when inference found a label."""
    try:
        b = int(capacity_bytes) if capacity_bytes is not None else 0
    except (TypeError, ValueError):
        return ""
    if b <= 0:
        return ""
    gib = b / (1024**3)
    gdec = b / 1e9
    base = f"OS-reported: {b:,} bytes (~{gib:.2f} GiB · ~{gdec:.2f} GB decimal)"
    ml = (market_label or "").strip()
    if ml:
        return f"Market label: {ml} · {base}"
    return base


def _nominal_capacity_from_bytes_tiers(b: int) -> str | None:
    """Infer sticker-style decimal-GB tier (OS size is almost always below nominal)."""
    gb_dec = b / 1e9
    tiers = (4, 8, 16, 32, 64, 128, 256, 512, 1024, 2000, 4000)
    for tier in reversed(tiers):
        if gb_dec >= tier * 0.82:
            return f"{tier} GB"
    if gb_dec >= 0.95:
        return f"{max(1, round(gb_dec))} GB"
    return None


def infer_nominal_capacity_label(
    capacity_bytes: int | None,
    model: str | None,
    media_type: str | None,
) -> str | None:
    """
    Marketing / label-style capacity (e.g. 64 GB) when OS reports slightly less.
    Uses model string patterns first, then common decimal-GB tiers vs reported bytes.
    """
    cb_int: int | None = None
    if capacity_bytes is not None:
        try:
            cb_int = int(capacity_bytes)
        except (TypeError, ValueError):
            cb_int = None
    m = (model or "").strip()
    if m:
        for pat in (
            r"[-_/](\d{3,4})\s*g(?:i?)?b?\b",
            r"\b(\d{2,4})\s*g(?:i?)?b\b",
            r"\b(\d{2,4})g\b",
        ):
            mm = re.search(pat, m, re.I)
            if mm:
                n = int(mm.group(1))
                if 4 <= n <= 8192:
                    return f"{n} GB"
    if cb_int and cb_int > 0:
        return _nominal_capacity_from_bytes_tiers(cb_int)
    return None


def _norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()


def _stable_block_parent(device_identifier: Any) -> str:
    """
    Whole-disk id for fingerprinting: disk5s1→disk5, sda1→sda, nvme0n1p2→nvme0n1.
    Reduces duplicate IDs when the OS reports a partition path vs whole disk.
    """
    if device_identifier is None:
        return ""
    s = str(device_identifier).strip().replace("/dev/", "").split("/")[-1]
    m = re.match(r"^(disk\d+)", s, re.I)
    if m:
        return m.group(1).lower()
    m = re.match(r"^(nvme\d+n\d+)p\d+$", s, re.I)
    if m:
        return m.group(1).lower()
    m = re.match(r"^((?:sd|vd|xvd)[a-z])\d+$", s, re.I)
    if m:
        return m.group(1).lower()
    return ""


def _fingerprint_capacity_key(identity: dict[str, Any]) -> str:
    """
    Coarse capacity bucket so list vs quick-test probes (diskutil vs usage, partition vs
    whole-disk edge cases) still land on the same hash. Prefers marketing-style tier
    (e.g. 32 GB) from reported bytes; falls back to 4 MiB-rounded byte count.
    """
    ex = identity.get("extra") if isinstance(identity.get("extra"), dict) else {}
    raw = identity.get("capacity_bytes")
    if raw is None or raw == "":
        du = ex.get("disk_usage")
        if isinstance(du, dict):
            raw = du.get("total")
    try:
        cb_int = int(raw) if raw is not None and raw != "" else 0
    except (TypeError, ValueError):
        cb_int = 0
    if cb_int <= 0:
        return ""
    mt = identity.get("media_type")
    nominal = infer_nominal_capacity_label(cb_int, None, mt)
    if nominal:
        return _norm(nominal).replace(" ", "")
    step = 4 * 1024 * 1024
    return str((cb_int + step // 2) // step * step)


def fingerprint_identity(identity: dict[str, Any]) -> str:
    """
    Stable id for UI/dedup (not cryptographically secure).

    Prefer filesystem UUID (volume UUID) when present — this matches what users see in
    Disk Utility and keeps quick-test/list rows aligned.

    When UUID is unavailable and we can anchor to a whole-disk id (disk5s1→disk5, etc.),
    fingerprint uses a **nominal capacity tier** (or rounded bytes) + that anchor.

    Without a block parent (e.g. some network paths), fall back to serial/model/vendor/uuid/capacity,
    then volume name + device path.
    """
    ex = identity.get("extra") if isinstance(identity.get("extra"), dict) else {}
    vol_uuid = _norm(identity.get("volume_uuid") or ex.get("volume_uuid"))
    if vol_uuid:
        raw = "vol_uuid|" + vol_uuid
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    cap = str(identity.get("capacity_bytes") or ex.get("disk_usage", {}).get("total") or "")
    block_parent = _stable_block_parent(ex.get("device_identifier"))

    if block_parent:
        cap_key = _fingerprint_capacity_key(identity)
        if not cap_key:
            cap_key = cap
        raw = "|".join([cap_key, block_parent])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    serial_n = _norm(identity.get("serial"))
    model = _norm(identity.get("model"))
    vendor = _norm(identity.get("vendor"))
    parts = [serial_n, model, vendor, vol_uuid, cap]
    raw = "|".join(parts)
    if raw.strip("|") == "":
        raw = "|".join(
            [
                _norm(identity.get("volume_name")),
                str(ex.get("device_identifier") or ""),
            ]
        )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fingerprint_short(fp: str, n: int = 12) -> str:
    return fp[:n] if fp else ""


def infer_media_type(identity: dict[str, Any]) -> str:
    ex = identity.get("extra") if isinstance(identity.get("extra"), dict) else {}
    proto = _norm(ex.get("protocol") or identity.get("bus_type") or "")
    media = _norm(ex.get("media_name") or ex.get("media_type") or "")
    name = _norm(identity.get("volume_name") or "") + " " + media

    if "secure digital" in name or "sd card" in media or "sd" == proto:
        return "sd_card"
    if "usb" in proto or "usb" in media:
        return "usb"
    if "nvme" in proto or "sata" in proto or "pci" in proto:
        return "internal_or_ssd"
    if "disk image" in media or "disk image" in name:
        return "disk_image"
    return "removable_unknown"


def split_brand_series(model: str | None, vendor: str | None) -> tuple[str | None, str | None]:
    m = (model or "").strip()
    if not m:
        if vendor is None:
            return None, None
        vs = vendor.strip() if isinstance(vendor, str) else str(vendor).strip()
        return (vs or None), None
    low = m.lower()
    for b in _BRAND_SORTED:
        if low == b or low.startswith(b + " ") or low.startswith(b + "-"):
            rest = m[len(b) :].strip().lstrip("-_/ ")
            return b.replace("-", " ").title(), rest if rest else None
    parts = m.split(None, 1)
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[1]


def _ensure_device_identifier_for_fingerprint(identity: dict[str, Any], ex: dict[str, Any]) -> None:
    """
    Fingerprinting needs a BSD/Linux block id in extra.device_identifier. Some stored rows
    lose extra or omit device_identifier; without it we fall back to USB serial/model and
    keep an old unstable hash. Repair from mount (macOS) when possible.
    """
    if _stable_block_parent(ex.get("device_identifier")):
        return
    mp = ex.get("mount_point") or identity.get("mount_point")
    if mp and sys.platform == "darwin":
        try:
            from sd_health.info import _mac_device_for_mount
        except ImportError:
            return
        dev = _mac_device_for_mount(str(mp))
        if dev:
            ex["device_identifier"] = dev.replace("/dev/", "").strip()


def enrich_identity(identity: dict[str, Any]) -> dict[str, Any]:
    """Add fingerprint, media_type, brand, series, labels for UI/API."""
    if not isinstance(identity, dict):
        identity = {}
    else:
        identity = dict(identity)

    ex = identity.get("extra")
    if isinstance(ex, str):
        try:
            ex = json.loads(ex)
        except json.JSONDecodeError:
            ex = {}
    if not isinstance(ex, dict):
        ex = {}
    else:
        ex = dict(ex)

    _ensure_device_identifier_for_fingerprint(identity, ex)

    vendor = identity.get("vendor") or ex.get("usb_vendor") or ex.get("vendor_name")
    model = (
        identity.get("model")
        or ex.get("model_name")
        or ex.get("product_name")
        or ex.get("device_model")
    )
    if ex.get("volume_uuid"):
        identity["volume_uuid"] = ex.get("volume_uuid")
    brand, series = split_brand_series(model, vendor)
    if vendor and not brand:
        brand = str(vendor).strip()

    identity["extra"] = ex
    identity["media_type"] = infer_media_type(identity)
    identity["brand"] = brand
    identity["series"] = series
    fp = fingerprint_identity(identity)
    identity["fingerprint"] = fp
    identity["fingerprint_short"] = fingerprint_short(fp)

    friendly = {
        "sd_card": "SD / microSD",
        "usb": "USB storage",
        "internal_or_ssd": "Internal / SSD",
        "disk_image": "Disk image",
        "removable_unknown": "Removable",
    }
    identity["media_type_label"] = friendly.get(identity["media_type"], identity["media_type"])
    cb_raw = identity.get("capacity_bytes")
    if isinstance(ex.get("disk_usage"), dict):
        du = ex["disk_usage"].get("total")
        if (not cb_raw) and du is not None:
            identity["capacity_bytes"] = du
            cb_raw = du
    cb: Any = cb_raw
    try:
        cb_int = int(cb) if cb is not None and cb != "" else 0
    except (TypeError, ValueError):
        cb_int = 0
    if cb_int > 0:
        identity["capacity_bytes"] = cb_int
        cb = cb_int
    else:
        cb = None
    os_label = format_capacity(cb)
    nominal = infer_nominal_capacity_label(cb, model, identity.get("media_type"))
    identity["capacity_os_label"] = os_label
    identity["capacity_nominal_label"] = nominal
    nom_s = str(nominal).strip() if nominal else ""
    # Primary UI string: marketing/sticker tier when inferred, else OS-formatted.
    identity["capacity_label"] = nom_s if nom_s else os_label
    identity["capacity_tooltip"] = format_capacity_tooltip(cb, nominal)

    return identity


def enrich_candidate_row(
    row: dict[str, Any],
    user_prefs_by_fp: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Merge list-removable row + fingerprint / type / brand for dashboard API."""
    if row.get("error"):
        return dict(row)
    ex: dict[str, Any] = {}
    if row.get("protocol"):
        ex["protocol"] = row["protocol"]
    if row.get("media_name"):
        ex["media_name"] = row["media_name"]
    if row.get("volume_uuid"):
        ex["volume_uuid"] = row["volume_uuid"]
    dev = row.get("device") or row.get("device_path")
    if dev:
        s = str(dev).replace("/dev/", "").strip()
        if s:
            ex["device_identifier"] = s
    ident_in: dict[str, Any] = {
        "volume_name": row.get("volume_name") or row.get("friendly_name"),
        "capacity_bytes": row.get("capacity_bytes"),
        "filesystem": row.get("filesystem"),
        "model": row.get("model") or row.get("friendly_name"),
        "serial": row.get("serial") or row.get("serial_number"),
        "vendor": row.get("vendor"),
        "extra": ex,
    }
    enriched = enrich_identity(ident_in)
    out = dict(row)
    for k in (
        "fingerprint",
        "fingerprint_short",
        "media_type",
        "media_type_label",
        "brand",
        "series",
        "volume_uuid",
        "filesystem",
        "capacity_label",
        "capacity_os_label",
        "capacity_nominal_label",
        "capacity_tooltip",
    ):
        if k in enriched:
            out[k] = enriched[k]
    fp = enriched.get("fingerprint") or ""
    if user_prefs_by_fp and fp and fp in user_prefs_by_fp:
        pr = user_prefs_by_fp[fp]
        out["user_label"] = pr.get("user_label") or ""
        out["user_notes"] = pr.get("user_notes") or ""
        out["user_brand"] = pr.get("user_brand") or ""
        out["user_series"] = pr.get("user_series") or ""
    else:
        out.setdefault("user_label", "")
        out.setdefault("user_notes", "")
        out.setdefault("user_brand", "")
        out.setdefault("user_series", "")

    # "Known" = user explicitly saved labels (Edit → Save), not merely seen in a test run.
    has_saved_prefs = bool(
        (out.get("user_label") or "").strip()
        or (out.get("user_notes") or "").strip()
        or (out.get("user_brand") or "").strip()
        or (out.get("user_series") or "").strip()
    )
    out["known_device"] = has_saved_prefs
    out["identity_unique"] = not has_saved_prefs

    ub = (out.get("user_brand") or "").strip()
    us = (out.get("user_series") or "").strip()
    auto_b = out.get("brand")
    auto_s = out.get("series")
    out["display_brand"] = ub or auto_b or ""
    out["display_series"] = us or auto_s or ""
    pen_user = (out.get("user_label") or "").strip()
    vol_label = (row.get("volume_name") or row.get("friendly_name") or "").strip()
    # Prefer saved pen name; otherwise show volume label so "name on card" is never blank when we know the id.
    out["display_pen_name"] = pen_user or vol_label or ""
    return out
