from __future__ import annotations

from typing import NoReturn

from fastapi import HTTPException

REMOVED_410_DETAILS: dict[str, str] = {
    "data.ingest.download": "download endpoint removed; use main-schedule + main-l5",
    "data.ingest.download_contract_range": "download-contract-range endpoint removed; use main-schedule + main-l5",
    "data.ingest.record": "record endpoint removed in current PRD phase",
    "data.ingest.update_variety": "update-variety endpoint removed in current PRD phase",
    "data.integrity.audit": "integrity audit endpoint removed in current PRD phase",
    "jobs.download_contract_range": "download_contract_range job type removed in current PRD phase",
    "api.data.coverage": "data coverage endpoint removed in current PRD phase",
    "api.data.coverage_summary": "coverage summary endpoint removed in current PRD phase",
    "api.data.quality_detail": "data quality detail endpoint removed in current PRD phase",
    "api.data.diagnose": "data diagnose endpoint removed in current PRD phase",
    "api.data.repair": "data repair endpoint removed in current PRD phase",
    "api.data.health": "data health endpoint removed in current PRD phase",
    "api.contracts.explorer": "contracts explorer endpoint removed in current PRD phase",
    "api.contracts.fill": "contracts fill endpoint removed in current PRD phase",
    "api.contracts.update": "contracts update endpoint removed in current PRD phase",
    "api.contracts.audit": "contracts audit endpoint removed in current PRD phase",
}


def raise_removed_410(key: str, *, default: str | None = None) -> NoReturn:
    detail = REMOVED_410_DETAILS.get(str(key), str(default or "endpoint removed in current PRD phase"))
    raise HTTPException(status_code=410, detail=detail)
