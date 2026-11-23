"""指标聚合专用入口：计算、撤边/更正、物化读取都走同一套归属校验。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.fusion_engine import fusion_engine

router = APIRouter(prefix="/api/metric_aggr", tags=["指标聚合"])


class ComputePayload(BaseModel):
    lineage_edge: int | str
    target_wide_table: str
    agg_function: str
    group_dimensions: list[str] = Field(default_factory=list)
    measure_field: str | None = None


class RevokePayload(BaseModel):
    reason: str = ""


class CorrectionPayload(BaseModel):
    reason: str = ""
    related_fields: list[str] | None = None


@router.post("/compute")
def compute_metric(payload: ComputePayload) -> dict[str, Any]:
    """按血缘边计算指标；归属或字段权限不满足时立即拒绝，不读取表数据。"""
    return fusion_engine.compute_metric_from_lineage(
        lineage_edge_id=payload.lineage_edge,
        target_wide_table=payload.target_wide_table,
        agg_function=payload.agg_function,
        group_dimensions=payload.group_dimensions,
        measure_field=payload.measure_field,
    )


@router.get("/receipts/{receipt_code}")
def read_receipt(receipt_code: str) -> dict[str, Any]:
    receipt = fusion_engine.get_aggregation_receipt(receipt_code)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"聚合依据回执 {receipt_code} 不存在")
    return receipt


@router.get("/materializations")
def list_materializations(metric_rule_code: str | None = None,
                          target_wide_table: str | None = None) -> dict[str, Any]:
    items = fusion_engine.list_materializations(
        metric_rule_code=metric_rule_code,
        target_wide_table=target_wide_table,
    )
    return {"items": items, "total": len(items)}


@router.get("/materializations/{materialization_code}")
def read_materialization(materialization_code: str,
                         allow_stale: bool = False) -> dict[str, Any]:
    result = fusion_engine.read_materialization(
        materialization_code, allow_stale=allow_stale)
    if result.get("message", "").startswith("物化记录") and not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


lineage_router = APIRouter(prefix="/api/lineage_edge", tags=["数据血缘"])


@lineage_router.post("/{edge_ref}/revoke")
def revoke_edge(edge_ref: str, payload: RevokePayload) -> dict[str, Any]:
    result = fusion_engine.revoke_lineage_edge(edge_ref, reason=payload.reason)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@lineage_router.post("/{edge_ref}/corrections")
def publish_correction(edge_ref: str, payload: CorrectionPayload) -> dict[str, Any]:
    result = fusion_engine.publish_correction_edge(
        edge_ref, related_fields=payload.related_fields, reason=payload.reason)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@lineage_router.post("/{edge_ref}/confirm")
def confirm_edge(edge_ref: str) -> dict[str, Any]:
    result = fusion_engine.confirm_lineage_edge(edge_ref)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result
