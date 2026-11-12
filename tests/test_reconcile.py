"""多来源可信度 / 交叉校验 / 冲突裁决 / 口径版本的端到端断言。

前置：scripts/run_local.sh 已跑完全部治理链路——
  reconcile 首跑 -> v2026.11.0 时效出报 -> WB010/WB011 人工裁决
  -> RULE_0001 沉淀并随 v2026.11.1 发布 -> 全区间重算（WB014 被规则自动裁决）
  -> v2026.11.1 时效出报。
本文件只读断言，不可变红线通过"同版本重跑幂等跳过且逐行内容不变"验证。
"""
from __future__ import annotations

import json

import pytest

from common.spark_utils import (
    CALIBRE_TABLE, CONFLICT_TABLE, DWD_EVENT, NODE_MASTER, ODS_EVENT,
    REVIEW_QUEUE, RULE_TABLE, TIMELINESS_TABLE,
)
from timeliness_job import run_timeliness


# ---------- 14. 来源优先级定主记录：WB010 四来源 SIGN，主记录取 SCANNER ----------
def test_14_master_record_uses_source_priority(spark):
    row = (spark.table(NODE_MASTER)
           .where("waybill_no = 'WB010' AND event_type = 'SIGN'").collect()[0])
    assert row.master_source_system == "SCANNER"
    assert row.master_event_id == "EA02"
    assert row.source_count == 4
    sources = sorted(json.loads(row.source_systems))
    assert sources == ["CS", "HUB", "ROUTE", "SCANNER"]


# ---------- 15. 冲突不丢原始记录：四个来源的 SIGN 在 ODS/DWD 全部保留 ----------
def test_15_all_source_observations_retained(spark):
    dwd_sign = (spark.table(DWD_EVENT)
                .where("waybill_no = 'WB010' AND event_type = 'SIGN'").count())
    assert dwd_sign == 4, "冲突运单的多来源原始事件必须全部保留，不能只留主记录"
    ods_sign = (spark.table(ODS_EVENT)
                .where("waybill_no = 'WB010' AND event_type = 'SIGN'").count())
    assert ods_sign == 4
    payloads = (spark.table(ODS_EVENT)
                .where("waybill_no = 'WB010' AND source_system = 'CS'")
                .select("raw_payload").collect())
    assert payloads and json.loads(payloads[0].raw_payload)["event_id"] == "EA05"


# ---------- 16. 时间冲突条目：差异秒数/容差/离群来源/可能原因齐全 ----------
def test_16_time_conflict_diff_seconds_and_reason(spark):
    row = (spark.table(CONFLICT_TABLE)
           .where("waybill_no = 'WB010' AND conflict_field = 'EVENT_TIME'")
           .collect()[0])
    assert row.status == "ADJUDICATED"  # run_local 已人工裁决
    # 10:00 ~ 14:00 最大差异 4 小时 = 14400 秒，容差 300 秒
    assert row.max_diff_seconds == 14400
    assert row.master_diff_seconds == 14400
    assert row.tolerance_seconds == 300
    assert row.outlier_source_system == "CS"
    assert row.reason_code == "MANUAL_BACKFILL_LAG"
    assert "客服补录" in row.possible_reason
    values = json.loads(row.source_values)
    assert len(values) == 4
    deltas = {v["source_system"]: v["delta_seconds_vs_master"] for v in values}
    assert deltas == {"SCANNER": 0, "HUB": 360, "ROUTE": 1800, "CS": 14400}
    # 确定性冲突ID：sha256(运单|节点|字段)，重跑不产生新条目
    assert row.conflict_id and len(row.conflict_id) == 64


# ---------- 17. 终态互斥状态冲突：签收/拒收同时存在 ----------
def test_17_terminal_status_conflict(spark):
    row = (spark.table(CONFLICT_TABLE)
           .where("waybill_no = 'WB011' AND conflict_field = 'EVENT_STATUS'")
           .collect()[0])
    assert row.event_type == "TERMINAL"
    assert row.reason_code == "STATUS_MISRECORD"
    assert row.max_diff_seconds is None  # 状态冲突无时间差异秒数
    values = json.loads(row.source_values)
    assert {v["field_value"] for v in values} == {"SIGN", "REJECT"}
    assert row.status == "ADJUDICATED"
    assert row.adjudicated_event_id == "EB02"  # 采信扫描枪 SIGN


# ---------- 18. 容差内差异不生成冲突，主记录仍取高优先级来源 ----------
def test_18_within_tolerance_no_conflict(spark):
    n = (spark.table(CONFLICT_TABLE)
         .where("waybill_no = 'WB012'").count())
    assert n == 0, "180s 差异在 300s 容差内，不应生成冲突"
    row = (spark.table(NODE_MASTER)
           .where("waybill_no = 'WB012' AND event_type = 'DELIVER'").collect()[0])
    assert row.master_source_system == "SCANNER"
    assert row.source_count == 2
    assert row.conflict_flag is False


# ---------- 19. 待核查队列：未裁决 PENDING，裁决后 RESOLVED ----------
def test_19_review_queue_lifecycle(spark):
    pending = (spark.table(REVIEW_QUEUE)
               .where("waybill_no = 'WB013'").collect()[0])
    assert pending.queue_status == "PENDING"
    assert pending.open_conflict_count >= 1
    fields = json.loads(pending.conflict_fields)
    assert "EVENT_TIME" in fields
    reasons = json.loads(pending.reason_codes)
    assert "ROUTE_CALLBACK_DELAY" in reasons

    resolved = (spark.table(REVIEW_QUEUE)
                .where("waybill_no = 'WB010'").collect()[0])
    assert resolved.queue_status == "RESOLVED"
    assert resolved.open_conflict_count == 0
    assert resolved.resolved_time is not None


# ---------- 20. 首版时效统计排除全部待核查运单 ----------
def test_20_v1_timeliness_excludes_pending(spark):
    v1 = spark.table(TIMELINESS_TABLE).where(
        "calibre_version = 'v2026.11.0'")
    v1_waybills = {r.waybill_no for r in v1.select("waybill_no").collect()}
    # 首版出报时 WB010/011/013/014 全部存在未裁决冲突
    assert "WB013" not in v1_waybills
    assert "WB010" not in v1_waybills
    assert "WB011" not in v1_waybills
    assert "WB014" not in v1_waybills
    # 正常运单正常统计：WB001 揽收 11-01 08:00 -> 签收 11-02 12:00 = 28h
    wb001 = v1.where("waybill_no = 'WB001'").collect()[0]
    assert wb001.transit_hours == 28.0
    assert wb001.conflict_free_flag is True
    src = json.loads(wb001.node_sources)
    assert src["PICKUP"] == "SCANNER" and src["SIGN"] == "SCANNER"


# ---------- 21. 已发布报表不可变：同版本重跑幂等跳过，已发布行数与内容不变 ----------
def test_21_published_version_is_immutable(spark, conf):
    from timeliness_job import run_timeliness
    v1 = "v2026.11.0"
    before_df = (spark.table(TIMELINESS_TABLE)
                 .where(f"calibre_version = '{v1}'").orderBy("waybill_no").collect())
    before = len(before_df)
    # 重跑同版本：已出报运单全部幂等跳过，不新增也不重写任何行
    out = run_timeliness(spark, conf, v1)
    assert out["stat_rows_new"] == 0
    assert out["skipped_existing_waybills"] == before
    after_df = (spark.table(TIMELINESS_TABLE)
                .where(f"calibre_version = '{v1}'").orderBy("waybill_no").collect())
    assert len(after_df) == before
    # 逐行比对关键口径列，确认已发布内容一字未变
    for b, a in zip(before_df, after_df):
        assert (b.waybill_no, b.transit_hours, b.deliver_hours,
                b.sign_leg_hours, b.node_sources) == \
               (a.waybill_no, a.transit_hours, a.deliver_hours,
                a.sign_leg_hours, a.node_sources)


# ---------- 22. 裁决沉淀规则只在下一口径版本生效，自动裁决新冲突 ----------
def test_22_rule_takes_effect_only_in_next_version(spark):
    rule = spark.table(RULE_TABLE).where("rule_id = 'RULE_0001'").collect()[0]
    assert rule.rule_status == "PUBLISHED"
    assert rule.effective_calibre_version == "v2026.11.1"
    assert rule.decision == "PREFER_SOURCE"
    assert json.loads(rule.decision_value)["prefer_source"] == "SCANNER"

    # WB014 在 v2 重算时被规则自动裁决（首跑时规则尚未发布，冲突曾为 OPEN）
    wb014 = (spark.table(CONFLICT_TABLE)
             .where("waybill_no = 'WB014'").collect()[0])
    assert wb014.status == "ADJUDICATED"
    assert wb014.rule_id == "RULE_0001"
    assert wb014.adjudicated_by == "RULE:RULE_0001"
    assert wb014.adjudicated_event_id == "EE02"  # 采信扫描枪 12:00

    # 版本链：v2 的父版本是 v1，且 v1 不携带该规则
    versions = {r.calibre_version: r for r in spark.table(CALIBRE_TABLE).collect()}
    assert versions["v2026.11.1"].parent_version == "v2026.11.0"
    assert json.loads(versions["v2026.11.0"].rule_ids) == []
    assert "RULE_0001" in json.loads(versions["v2026.11.1"].rule_ids)


# ---------- 23. 人工裁决后队列解除、原始记录不动、节点主记录标志刷新 ----------
def test_23_manual_adjudication_resolves_without_touching_raw(spark):
    c = (spark.table(CONFLICT_TABLE)
         .where("waybill_no = 'WB010' AND conflict_field = 'EVENT_TIME'")
         .collect()[0])
    assert c.adjudicated_by == "zhangsan"
    assert c.adjudicated_event_id == "EA02"
    assert c.adjudicated_value == "2026-11-02 10:00:00"

    q = spark.table(REVIEW_QUEUE).where("waybill_no = 'WB010'").collect()[0]
    assert q.queue_status == "RESOLVED"

    # DWD 四条多来源 SIGN 一行未删，主记录节点标志已解除冲突
    assert spark.table(DWD_EVENT).where(
        "waybill_no = 'WB010' AND event_type = 'SIGN'").count() == 4
    nm = (spark.table(NODE_MASTER)
          .where("waybill_no = 'WB010' AND event_type = 'SIGN'").collect()[0])
    assert nm.conflict_flag is False
    assert nm.adjudicated_flag is True


# ---------- 24. 新版本出报追加而非覆盖：v2 纳入已裁决运单，仍排除 WB013 ----------
def test_24_v2_appends_and_includes_resolved(spark):
    v2 = spark.table(TIMELINESS_TABLE).where(
        "calibre_version = 'v2026.11.1'")
    v2_waybills = {r.waybill_no for r in v2.select("waybill_no").collect()}
    assert "WB010" in v2_waybills
    assert "WB011" in v2_waybills
    assert "WB014" in v2_waybills
    assert "WB013" not in v2_waybills  # 路由离群冲突仍 OPEN，继续排除

    # 已发布视图只暴露 PUBLISHED 版本（本流程两个版本均已发布）
    published_versions = {r.calibre_version for r in spark.sql(
        "SELECT DISTINCT calibre_version FROM ltl.app.v_timeliness_published"
    ).collect()}
    assert published_versions == {"v2026.11.0", "v2026.11.1"}

    # 开放冲突浏览视图与队列视图可查询
    open_rows = spark.sql(
        "SELECT waybill_no FROM ltl.app.v_waybill_conflict_open").collect()
    assert "WB013" in {r.waybill_no for r in open_rows}
    queue_rows = spark.sql(
        "SELECT waybill_no FROM ltl.app.v_waybill_review_queue "
        "WHERE queue_status = 'PENDING'").collect()
    assert "WB013" in {r.waybill_no for r in queue_rows}


# ---------- 25. 节点主记录冲突标志：离群来源与未裁决计数正确 ----------
def test_25_node_master_flags(spark):
    row = (spark.table(NODE_MASTER)
           .where("waybill_no = 'WB013' AND event_type = 'ARRIVE'").collect()[0])
    assert row.conflict_flag is True
    assert row.open_conflict_count == 1
    assert row.master_source_system == "HUB"  # HUB 优先级高于 ROUTE
    c = (spark.table(CONFLICT_TABLE)
         .where("waybill_no = 'WB013'").collect()[0])
    assert c.outlier_source_system == "ROUTE"
    assert c.max_diff_seconds == 10800  # 20:00 vs 23:00


# ---------- 26. 人工裁决不回灌旧版本：v1 重算时已裁决运单仍被版本边界拦截 ----------
def test_26_adjudication_does_not_rewrite_older_version(spark, conf):
    # 终态下 WB010/011/014 的冲突均已 ADJUDICATED、队列 RESOLVED，
    # 但它们是在 v1 发布之后才裁决的，重算 v1 必须继续排除，只能进入 v2
    out = run_timeliness(spark, conf, "v2026.11.0")
    assert out["version_boundary_blocked_waybills"] >= 3
    v1 = {r.waybill_no for r in spark.table(TIMELINESS_TABLE)
          .where("calibre_version = 'v2026.11.0'").select("waybill_no").collect()}
    assert not ({"WB010", "WB011", "WB014"} & v1)

    # 规则裁决的冲突带生效版本；人工裁决的生效版本为空（语义=下一个发布版本）
    rule_adj = (spark.table(CONFLICT_TABLE)
                .where("waybill_no = 'WB014'").collect()[0])
    assert rule_adj.effective_calibre_version == "v2026.11.1"
    manual_adj = (spark.table(CONFLICT_TABLE)
                  .where("waybill_no = 'WB010'").collect()[0])
    assert manual_adj.effective_calibre_version is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
