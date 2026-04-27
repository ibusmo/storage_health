from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


ResultStatus = Literal["pass", "fail", "error"]


@dataclass
class DiskIdentity:
    capacity_bytes: int | None = None
    filesystem: str | None = None
    volume_name: str | None = None
    vendor: str | None = None
    model: str | None = None
    serial: str | None = None
    removable: bool | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        out: dict[str, Any] = {}
        for k, v in d.items():
            if v is None:
                continue
            if k == "extra" and not v:
                continue
            out[k] = v
        return out


@dataclass
class RunRecord:
    run_id: str
    started_at: str
    finished_at: str
    host_os: str
    test_mode: Literal["quick", "full", "verify", "repair", "cam_stress"]
    device_path: str | None
    mount_point: str | None
    identity: dict[str, Any]
    result: ResultStatus
    summary: str
    error_detail: str | None = None
    operator_notes: str | None = None

    def to_json(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "host_os": self.host_os,
            "test_mode": self.test_mode,
            "device_path": self.device_path,
            "mount_point": self.mount_point,
            "identity": self.identity,
            "result": self.result,
            "summary": self.summary,
        }
        if self.error_detail is not None:
            d["error_detail"] = self.error_detail
        if self.operator_notes is not None:
            d["operator_notes"] = self.operator_notes
        return d
