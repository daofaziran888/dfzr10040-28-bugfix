"""指标聚合归属校验与聚合依据回执测试。

覆盖：
- 用与目标表无关的血缘边算指标必须立即拒绝，且不产生规则/回执/物化；
- 分组字段未被边覆盖（如 tax）时无读取权限；
- 物化只按回执授权的字段建立输入范围，缺键行不进入合法分组；
- 撤边与更正边并发时，立即读取物化必须给出拒绝依据并暴露采用的边版本；
- 更正边确认后重算采用新版本，旧物化仍保持失效，无关表 X 始终不产出。
"""
from __future__ import annotations

import pytest

from app.fusion_engine import FusionEngine


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


def _seed_b_rows(engine: FusionEngine) -> None:
    """让宽表 B 有一批可聚合的真实记录。"""
    engine.register_source(
        source_code="SRC-B", source_name="b_source", source_type="MySQL",
        connection_string="conn://b",
    )
    job = engine.create_fusion_job(
        target_wide_table="B",
        participant_sources=["SRC-B"],
        join_key="id",
    )
    job.merged_records = [
        {"_source": "SRC-B", "id": "r1", "region": "north", "tax": "0.1", "sales": 100},
        {"_source": "SRC-B", "id": "r2", "region": "north", "tax": "0.1", "sales": 200},
        {"_source": "SRC-B", "id": "r3", "region": "south", "tax": "0.2", "sales": 50},
        {"_source": "SRC-B", "id": "r4", "region": "", "tax": "0.2", "sales": 999},
        {"_source": "SRC-B", "id": "r5", "tax": "0.2", "sales": 777},
    ]


class TestOwnershipCheck:
    """血缘边归属校验。"""

    def test_edge_from_a_to_b_cannot_compute_x(self, engine: FusionEngine) -> None:
        """A 至 B 的边给 X 算指标，应立即拒绝越权取数。"""
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        result = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_code,
            target_wide_table="X",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert result["ok"] is False
        assert result["unauthorized"] is True
        assert "越权" in result["message"]
        # 拒绝必须发生在取数之前：未生成任何规则、回执或物化。
        assert engine.domain_snapshot()["metrics"] == 0
        assert engine.list_materializations(target_wide_table="X") == []
        assert engine.get_aggregation_receipt("RCPT-0001") is None

    def test_group_field_not_covered_has_no_read_permission(self, engine: FusionEngine) -> None:
        """边只覆盖 region/sales，tax 分组无读取权限，必须拒绝。"""
        _seed_b_rows(engine)
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        result = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["tax"],
            measure_field="sales",
        )
        assert result["ok"] is False
        assert "tax" in result["denied_fields"]
        assert result["denied_fields"]["tax"]
        assert engine.list_materializations(target_wide_table="B") == []

    def test_cannot_borrow_fields_from_another_edge(self, engine: FusionEngine) -> None:
        """同一张表上另一条边覆盖了字段，也不能被本条边借用做授权。"""
        _seed_b_rows(engine)
        region_edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region"],
        )
        sales_edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["sales"],
        )
        result = engine.compute_metric_from_lineage(
            lineage_edge_id=sales_edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert result["ok"] is False
        assert "region" in result["denied_fields"]
        assert engine.list_materializations(target_wide_table="B") == []

    def test_revoked_edge_as_entry_point_rejected(self, engine: FusionEngine) -> None:
        """直接被撤销的边作为入口时也拒绝，不新建回执。"""
        _seed_b_rows(engine)
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        engine.revoke_lineage_edge(edge.edge_id, reason="测试撤销")
        result = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert result["ok"] is False
        assert result["basis_rejected"] is True
        assert engine.list_materializations(target_wide_table="B") == []

    def test_region_metric_accepted_and_materialized(self, engine: FusionEngine) -> None:
        """B 在 region 由合法边覆盖时接受计算，物化只读取授权字段。"""
        _seed_b_rows(engine)
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        result = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert result["ok"] is True
        assert result["receipt_code"]
        assert result["adopted_edge_versions"][0]["edge_code"] == edge.edge_code
        assert result["adopted_edge_versions"][0]["version"] == 1
        assert result["input_range"]["read_fields"] == ["region", "sales"]
        # tax 没有被回执授权，输入范围里不能出现。
        assert "tax" not in result["input_range"]["read_fields"]

        groups = {group["group_key"]: group for group in result["groups"]}
        assert set(groups) == {"region=north", "region=south"}
        assert groups["region=north"]["measure"] == 300
        assert groups["region=north"]["source_fields"]["region"] == f"{edge.edge_code}@v1"
        assert groups["region=north"]["group_fields"] == ["region"]
        # 缺 region 的行（空串/缺键）被计入跳过，不显示成合法分组。
        assert result["skipped_missing_key"] == 2
        assert all(group["group"]["region"] for group in result["groups"])

        receipt = engine.get_aggregation_receipt(result["receipt_code"])
        assert receipt is not None
        assert receipt["permissions"]["table"]["name"] == "B"
        assert receipt["permissions"]["fields"]["region"]["read"] is True
        assert "tax" not in receipt["permissions"]["fields"]


class TestRevocationAndCorrection:
    """撤边 / 更正边与物化读取的并发场景。"""

    def test_read_after_revoke_rejects_and_exposes_versions(self, engine: FusionEngine) -> None:
        """受理后撤边并立即读取：拒绝该依据，同时能核对采用的边版本。"""
        _seed_b_rows(engine)
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        accepted = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert accepted["ok"] is True
        materialization_code = accepted["materialization_code"]

        correction = engine.publish_correction_edge(
            edge.edge_id, related_fields=["region", "sales", "tax"],
            reason="tax 字段关系需要更正",
        )
        assert correction["ok"] is True
        assert correction["status"] == "待确认"
        assert correction["version"] == 2
        # 更正边未确认前不能用于新计算。
        not_confirmed = engine.compute_metric_from_lineage(
            lineage_edge_id=correction["edge_code"],
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert not_confirmed["ok"] is False
        assert not_confirmed["basis_rejected"] is True

        engine.revoke_lineage_edge(edge.edge_id, reason="字段关系已更正")

        read = engine.read_materialization(materialization_code)
        assert read["ok"] is False
        assert read["status"] == "依据已失效"
        assert read["requires_recompute"] is True
        assert read["adopted_edge_versions"][0]["edge_code"] == edge.edge_code
        assert read["adopted_edge_versions"][0]["version"] == 1
        assert read["stale_edges"][0]["adopted_version"] == 1
        assert read["stale_edges"][0]["current_version"] == 1
        assert "撤销" in read["message"]
        # 历史分组可以核对，但不作为当前有效依据返回。
        assert "groups" not in read
        assert len(read["historical_groups"]) == 2

    def test_recompute_after_correction_confirmed(self, engine: FusionEngine) -> None:
        """新边确认后重算，新记录采用新字段与新版本；旧记录与无关表 X 不受影响。"""
        _seed_b_rows(engine)
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        accepted = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        old_code = accepted["materialization_code"]

        x_edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        x_attempt = engine.compute_metric_from_lineage(
            lineage_edge_id=x_edge.edge_id,
            target_wide_table="X",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert x_attempt["ok"] is False

        correction = engine.publish_correction_edge(
            edge.edge_id, related_fields=["region", "sales", "tax"],
            reason="补上 tax 字段关系",
        )
        engine.revoke_lineage_edge(edge.edge_id, reason="字段关系已更正")
        confirmed = engine.confirm_lineage_edge(correction["edge_code"])
        assert confirmed["ok"] is True
        assert edge.edge_code in confirmed["superseded_edges"]

        # 老边已撤销，直接传老边重算仍被拒绝。
        stale_retry = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert stale_retry["ok"] is False

        recomputed = engine.compute_metric_from_lineage(
            lineage_edge_id=correction["edge_code"],
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        assert recomputed["ok"] is True
        assert recomputed["materialization_code"] != old_code
        adopted = recomputed["adopted_edge_versions"][0]
        assert adopted["edge_code"] == correction["edge_code"]
        assert adopted["version"] == 2
        assert set(adopted["fields"]) == {"region", "sales"}
        assert adopted["status"] == "已确认"

        # 新记录读取有效，且展示采用的字段/边；旧记录仍失效。
        fresh = engine.read_materialization(recomputed["materialization_code"])
        assert fresh["ok"] is True
        assert fresh["adopted_edge_versions"][0]["version"] == 2
        assert fresh["groups"][0]["adopted_edges"][0]["version"] == 2
        old = engine.read_materialization(old_code)
        assert old["ok"] is False

        # 更新后的边已覆盖 tax，同表 tax 聚合此时才有权限。
        tax_metric = engine.compute_metric_from_lineage(
            lineage_edge_id=correction["edge_code"],
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["tax"],
            measure_field="sales",
        )
        assert tax_metric["ok"] is True
        assert tax_metric["input_range"]["read_fields"] == ["sales", "tax"]

        # X 仍然不产生任何物化。
        assert engine.list_materializations(target_wide_table="X") == []

    def test_revoke_then_correct_reverse_order(self, engine: FusionEngine) -> None:
        """先撤边再发布更正：本次物化仍按冻结回执判为失效。"""
        _seed_b_rows(engine)
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        accepted = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        engine.revoke_lineage_edge(edge.edge_id, reason="先撤边")
        correction = engine.publish_correction_edge(
            edge.edge_id, related_fields=["region", "sales"],
            reason="再补更正边",
        )
        assert correction["ok"] is True
        read = engine.read_materialization(accepted["materialization_code"])
        assert read["ok"] is False
        assert read["pending_corrections"][0]["edge_code"] == correction["edge_code"]
        assert read["pending_corrections"][0]["status"] == "待确认"

    def test_unconfirmed_correction_keeps_old_basis_valid(self, engine: FusionEngine) -> None:
        """只发布更正边而未撤原边时，本次物化仍可读取，并提示待确认的更正边。"""
        _seed_b_rows(engine)
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        accepted = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="SUM",
            group_dimensions=["region"],
            measure_field="sales",
        )
        correction = engine.publish_correction_edge(
            edge.edge_id, related_fields=["region", "sales", "tax"],
            reason="补充 tax",
        )
        read = engine.read_materialization(accepted["materialization_code"])
        assert read["ok"] is True
        assert read["basis_valid"] is True
        assert read["adopted_edge_versions"][0]["version"] == 1
        assert read["pending_corrections"][0]["edge_code"] == correction["edge_code"]
        assert read["pending_corrections"][0]["usable"] is False

    def test_missing_group_key_not_counted_as_legal_group(self, engine: FusionEngine) -> None:
        """缺键行不计入任何分组，也不显示空键分组。"""
        _seed_b_rows(engine)
        edge = engine.create_lineage_edge(
            upstream_node="A", downstream_node="B",
            related_fields=["region", "sales"],
        )
        result = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="B",
            agg_function="COUNT",
            group_dimensions=["region"],
        )
        assert result["ok"] is True
        assert result["skipped_missing_key"] == 2
        assert result["input_range"]["rows_used"] == 3
        assert all(group["group"]["region"] for group in result["groups"])
