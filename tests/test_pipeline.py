"""端到端断言：覆盖事件型仓库的全部关键需求。

前置条件：bash scripts/run_local.sh 已完成建仓与灌数（本文件只读断言，
除 test_12_schema_evolution 以 IF NOT EXISTS 加列外不修改业务数据）。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.spark_utils import (
    BACKFILL_LOG, DIM_TABLES, DQ_TABLE, DWD_EVENT, ODS_EVENT, WM_LOG,
)

ROOT = Path(__file__).resolve().parent.parent


# ---------- 1. ODS 原始层：原样保存、不清洗不合并 ----------
def test_01_ods_keeps_duplicate_and_raw_payload(spark):
    # E401 被两个落地文件重复推送：ODS 必须保留两份（去重只允许发生在 DWD）
    ods_dup = (spark.table(ODS_EVENT)
               .where("event_id = 'E401'").count())
    assert ods_dup == 2, f"ODS 应原样保留重复上报，实际 {ods_dup} 行"

    raw = (spark.table(ODS_EVENT)
           .where("event_id = 'E502'").select("raw_payload").collect()[0][0])
    payload = json.loads(raw)
    assert payload["exception_reason"] == "外包装破损"
    # 原始报文全文保留（含业务字段），未做裁剪
    assert payload["hub_code"] == "HUB_SH" and payload["exception_code"] == "EX_DAMAGE"

    # 二级分区字段存在
    parts = spark.sql(
        f"SELECT DISTINCT biz_date, source_system FROM {ODS_EVENT}").count()
    assert parts >= 4


# ---------- 2. DWD 去重：同 event_id 只留一条 ----------
def test_02_dwd_dedup_by_event_id(spark):
    n = (spark.table(DWD_EVENT).where("event_id = 'E401'").count())
    assert n == 1, f"DWD 按 event_id 去重后应剩 1 行，实际 {n}"
    total_dup = (spark.table(ODS_EVENT).groupBy("event_id")
                 .count().where("count > 1").count())
    assert total_dup == 1  # 只有 E401 一组重复


# ---------- 3. 乱序：序号按业务发生时间，而非上报时间 ----------
def test_03_out_of_order_seq_uses_event_time(spark):
    rows = (spark.table(DWD_EVENT).where("waybill_no = 'WB003'")
            .orderBy("event_seq").collect())
    seq_map = {r.event_type: r.event_seq for r in rows}
    # ARRIVE 业务时间 15:00 早于 DEPART 18:00，尽管它更晚上报
    assert seq_map["ARRIVE"] < seq_map["DEPART"]
    assert [r.event_seq for r in rows] == list(range(1, len(rows) + 1))


# ---------- 4. 多次异常独立保留，兼容视图末次原因口径正确 ----------
def test_04_multiple_exceptions_kept_and_compat_view(spark):
    exc = (spark.table(DWD_EVENT)
           .where("waybill_no = 'WB005' AND event_type = 'EXCEPTION'")
           .orderBy("event_seq").collect())
    assert len(exc) == 2, "多次异常必须独立保留，不能只留最后一次"
    assert [r.exception_reason for r in exc] == ["外包装破损", "冷链温度超标"]

    wide = spark.sql(
        "SELECT last_exception_reason, exception_chain "
        "FROM ltl.app.v_waybill_wide_compat WHERE waybill_no = 'WB005'").collect()[0]
    assert wide["last_exception_reason"] == "冷链温度超标"
    assert len(wide["exception_chain"]) == 2


# ---------- 5. 缺失节点补齐后重放：迟到 ARRIVE 插入正确序号 ----------
def test_05_late_event_replay_resequences(spark):
    rows = (spark.table(DWD_EVENT).where("waybill_no = 'WB006'")
            .orderBy("event_seq").collect())
    type_seq = {r.event_type: r.event_seq for r in rows}
    assert type_seq["ARRIVE"] == 3, "迟到补报的 ARRIVE 应重排为第3个事件"
    assert [r.event_seq for r in rows] == list(range(1, len(rows) + 1))
    # 迟到事件业务日期仍为 11-01（event_date 取自 event_time，不是上报日）
    arrive = next(r for r in rows if r.event_type == "ARRIVE")
    assert str(arrive.event_date) == "2026-11-01"


# ---------- 6. 拉链维表：时点关联，禁止最新维表回刷历史 ----------
def test_06_scd2_point_in_time_join(spark):
    enriched = spark.sql("""
        SELECT event_date, hub_name_at_event, service_area_at_event,
               customer_level_at_event
        FROM ltl.app.v_waybill_event_enriched
        WHERE waybill_no = 'WB009' ORDER BY event_seq
    """).collect()
    old = next(r for r in enriched if str(r.event_date) == "2026-11-01")
    new = next(r for r in enriched if str(r.event_date) == "2026-11-03")
    # 11-01 的事实必须关联到当时版本，而不是 11-02 改名后的最新名称
    assert old["hub_name_at_event"] == "北京一集配站"
    assert old["service_area_at_event"] == "浦东A片区"
    assert old["customer_level_at_event"] == "KA"
    assert new["hub_name_at_event"] == "北京华北转运中心"
    assert new["service_area_at_event"] == "浦东B片区"
    assert new["customer_level_at_event"] == "STD"

    # 维表本身保留两个版本，旧版本已按快照日闭合
    versions = (spark.table(DIM_TABLES["hub"])
                .where("hub_code = 'HUB_BJ'").orderBy("eff_date").collect())
    assert len(versions) == 2
    assert str(versions[0].exp_date) == "2026-11-02"
    assert versions[0].is_current is False
    assert versions[1].is_current is True

    # 合并撤销不物理删除：HUB_OLD 保留 MERGED 历史版本
    merged = (spark.table(DIM_TABLES["hub"])
              .where("hub_code = 'HUB_OLD' AND status = 'MERGED'").count())
    assert merged >= 1


# ---------- 7. 分拨压货定位：停留 >48 小时可回答客服 ----------
def test_07_hub_dwell_over_two_days(spark):
    row = spark.sql("""
        SELECT dwell_minutes, dwell_over_2d_flag, hub_name_at_event
        FROM ltl.app.v_hub_dwell
        WHERE waybill_no = 'WB002' AND hub_code = 'HUB_BJ'
    """).collect()[0]
    assert row["dwell_over_2d_flag"] is True
    assert row["dwell_minutes"] >= 48 * 60
    assert row["hub_name_at_event"] == "北京一集配站"  # 11-01 时点名称


# ---------- 8. 维表未匹配：代理键 -1 可被 DQ 捕获 ----------
def test_08_unmatched_dim_sk_minus_one(spark):
    rows = (spark.table(DWD_EVENT)
            .where("waybill_no = 'WB007'").select("hub_sk").distinct().collect())
    assert {r.hub_sk for r in rows} == {-1}


# ---------- 9. DQ 日检：五类指标齐全且可下钻到来源/分拨 ----------
def test_09_dq_metrics_and_drilldown(spark):
    codes = {r.metric_code for r in spark.table(DQ_TABLE).collect()}
    for base in ["EVENT_MISSING", "OUT_OF_ORDER", "DUPLICATE",
                 "DIM_UNMATCHED_HUB", "PARTITION_LATENCY"]:
        assert base in codes, f"缺少质量指标 {base}"
    # 汇总行 + 来源下钻 + 分拨下钻三级
    drill = spark.sql(f"""
        SELECT DISTINCT source_system, hub_code FROM {DQ_TABLE}
    """).collect()
    assert any(r.source_system == "ALL" and r.hub_code == "ALL" for r in drill)
    assert any(r.source_system != "ALL" and r.hub_code != "ALL" for r in drill)
    # 异常行带阈值与 breach 标志，可直接定位责任来源
    breaches = spark.sql(
        f"SELECT * FROM {DQ_TABLE} WHERE is_breach = true").collect()
    assert len(breaches) > 0
    assert all(r.source_system is not None for r in breaches)


# ---------- 10. 补数：只重跑受影响运单，影子表对比 + 台账 + 受控切换 ----------
def test_10_backfill_audit_trail(spark):
    logs = spark.sql(f"""
        SELECT * FROM {BACKFILL_LOG}
        WHERE scope_type = 'WAYBILL' AND status = 'SWITCHED'
    """).collect()
    assert len(logs) == 1
    log = logs[0]
    assert "WB006" in log.scope_value
    affected = json.loads(log.affected_partitions)
    # 只重放受影响分区，且包含迟到事件所属的 11-01 HUB 分区
    assert any(p.startswith("2026-11-01|") and p.endswith("HUB") for p in affected)
    diff = json.loads(log.diff_summary)
    assert diff["added_rows"] == 1  # 新增迟到 ARRIVE
    assert diff["seq_reordered_rows"] >= 1
    # 对比结果文件落盘
    files = list((ROOT / "data/backfill").glob("*_diff.json"))
    assert files, "必须产出补数前后对比文件"
    payload = json.loads(files[0].read_text(encoding="utf-8"))
    assert payload["rows_before"] < payload["rows_after"]


# ---------- 11. 水位线按日推进，失败不写入（本夹具全部成功）----------
def test_11_watermark_advanced(spark):
    rows = spark.sql(
        f"SELECT DISTINCT layer, biz_date FROM {WM_LOG} "
        "WHERE status = 'SUCCESS' ORDER BY biz_date").collect()
    layers = {r.layer for r in rows}
    assert {"ODS", "DWD"}.issubset(layers)
    assert len({r.biz_date for r in rows}) >= 4


# ---------- 12. Schema 演进：只加列 + 回填默认值，既有视图与报表不受影响 ----------
def test_12_schema_evolution_add_only(spark):
    from common.spark_utils import add_column_if_absent
    # 只加列（Spark 3.5 不支持 ADD COLUMN IF NOT EXISTS，用存在性检查保证幂等）
    add_column_if_absent(
        spark, "ltl.dwd.waybill_event_detail",
        "evol_test_col", "STRING", "演进演练列")
    # 历史分区回填默认值（schema_evolution_job 的标准动作，幂等）
    spark.sql("""
        UPDATE ltl.dwd.waybill_event_detail
        SET evol_test_col = 'NA' WHERE evol_test_col IS NULL
    """)
    # 既有下游视图无需改动仍可查询（视图是明细层之上的稳定投影）
    cnt = spark.sql(
        "SELECT count(*) FROM ltl.app.v_waybill_event_enriched").collect()[0][0]
    assert cnt > 0
    default_val = spark.sql("""
        SELECT DISTINCT evol_test_col FROM ltl.dwd.waybill_event_detail
    """).collect()
    assert {r.evol_test_col for r in default_val} == {"NA"}
    # 演进作业幂等：重复加列不报错
    assert add_column_if_absent(
        spark, "ltl.dwd.waybill_event_detail",
        "evol_test_col", "STRING") is False
    # 新作业节点枚举值无需 DDL：EXCEPTION 等枚举可直接落地（模型可扩展）
    assert spark.table(DWD_EVENT).where("event_type = 'EXCEPTION'").count() >= 2


# ---------- 13. 旧宽表兼容视图：一行一运单，关键时间列可透视 ----------
def test_13_wide_compat_view_rows(spark):
    row = spark.sql("""
        SELECT pickup_time, sign_time, appointment_window, is_cold_chain
        FROM ltl.app.v_waybill_wide_compat WHERE waybill_no = 'WB001'
    """).collect()[0]
    assert row["pickup_time"] is not None and row["sign_time"] is not None
    assert row["appointment_window"] == "2026-11-02 09:00-11:00"
    assert row["is_cold_chain"] == "true"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
