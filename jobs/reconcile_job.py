"""多来源可信度 / 交叉校验 / 冲突裁决作业（reconcile）。

处理链路（对应需求，全部基于 DWD 已去重明细，原始记录仍在 ODS/DWD 全量保留）：

  1. 来源优先级定主记录
     优先级来自 conf/pipeline.conf 的 reconcile.source.priority
     （缺省 SCANNER > HUB > ROUTE > CS > LEGACY），同一运单同一关键节点
     取优先级最高来源的事件为主记录，写入 dwd.waybill_node_master。
  2. 关键节点交叉校验
     a. 时间矛盾 EVENT_TIME：同节点被 >=2 个来源观测，且各来源事件时间
        两两最大差异超过 reconcile.time.tolerance.seconds 容差；
     b. 状态矛盾 EVENT_STATUS：终态节点（SIGN/REJECT）同时出现互斥状态。
     冲突生成 meta.waybill_conflict 条目，记录冲突字段、各来源取值 JSON、
     差异秒数、容差与推断的可能原因（原因码仅作核查线索，不是定论）。
  3. 待核查队列
     存在 OPEN 冲突的运单写入/刷新 meta.waybill_review_queue（PENDING）；
     冲突全部裁决或因数据修正消失后置 RESOLVED。未裁决运单由时效作业排除。
  4. 规则自动裁决
     仅应用 rule_status=PUBLISHED 且 effective_calibre_version 已发布的规则；
     DRAFT 规则对作业不可见，保证人工沉淀的规则只能在下一口径版本生效。
  5. 幂等
     conflict_id=sha2(waybill_no|event_type|conflict_field) 确定性生成，
     重跑只刷新观测列与 last_seen，不覆盖任何人工裁决列；
     节点主记录按受影响运单做动态分区覆盖。

用法：
  spark-submit jobs/reconcile_job.py --biz-date 2026-11-02
  spark-submit jobs/reconcile_job.py --scope WAYBILL --waybill-nos WB010
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.spark_utils import (
    CALIBRE_TABLE, CONFLICT_TABLE, INITIAL_CALIBRE_VERSION, NODE_MASTER,
    REVIEW_QUEUE, RULE_TABLE, DWD_EVENT, build_spark, key_nodes,
    load_conf, new_batch_id, source_priority, terminal_nodes,
)

# 原因码 -> 中文说明（possible_reason 只描述最可能的成因，供核查人员参考）
REASON_TEXT = {
    "CLOCK_SKEW": "各来源时钟不同步或作业系统时间戳延迟，差异略超容差",
    "MANUAL_BACKFILL_LAG": "客服补录通常事后录入，业务时间滞后于现场扫描",
    "ROUTE_CALLBACK_DELAY": "第三方路由回调异步推送，事件时间可能取回调到达时间",
    "OUTLIER_SOURCE": "单一来源事件时间明显偏离其他来源，疑似该来源记录错误",
    "STATUS_MISRECORD": "签收/拒收互斥终态被同时上报，疑似终态状态误录或补录修正未闭环",
}


# ---------------------------------------------------------------------
# 0. 口径版本：首次运行幂等登记初始 PUBLISHED 版本，返回当前生效版本
# ---------------------------------------------------------------------
def ensure_current_version(spark, conf: dict) -> str:
    initial = conf.get("reconcile.initial.calibre_version",
                       INITIAL_CALIBRE_VERSION)
    existing = spark.table(CALIBRE_TABLE).count()
    if existing == 0:
        spark.sql(f"""
            INSERT INTO {CALIBRE_TABLE}
            VALUES ('{initial}', 'PUBLISHED', current_date(), NULL, '[]',
                    '建仓初始口径版本：来源可信度与交叉校验首版规则',
                    'system', current_timestamp(), current_timestamp())
        """)
        print(f"[reconcile] 登记初始口径版本 {initial}")
    row = (spark.table(CALIBRE_TABLE)
           .filter(F.col("version_status") == "PUBLISHED")
           .orderBy(F.col("published_time").desc())
           .select("calibre_version").first())
    return row["calibre_version"]


def _affected_waybills(spark, start_date, end_date, waybill_nos):
    q = spark.table(DWD_EVENT)
    if waybill_nos:
        return (q.filter(F.col("waybill_no").isin(waybill_nos))
                 .select("waybill_no").distinct())
    if start_date:
        q = q.filter(F.col("event_date") >= F.lit(start_date))
    if end_date:
        q = q.filter(F.col("event_date") <= F.lit(end_date))
    return q.select("waybill_no").distinct()


# ---------------------------------------------------------------------
# 1. 节点观测 + 主记录选择
# ---------------------------------------------------------------------
def build_node_observations(spark, conf: dict, affected):
    """DWD 关键节点事件 + 来源优先级，返回带 priority 的观测 DataFrame。"""
    prio = source_priority(conf)
    default_prio = int(conf.get("reconcile.source.default_priority", "100"))
    prio_map = F.create_map([x for kv in prio.items() for x in
                             (F.lit(kv[0]), F.lit(kv[1]))])
    nodes = key_nodes(conf)
    obs = (
        spark.table(DWD_EVENT)
        .join(affected, on="waybill_no", how="left_semi")
        .filter(F.col("event_type").isin(nodes))
        .withColumn("priority",
                    F.coalesce(prio_map[F.col("source_system")],
                               F.lit(default_prio)))
    )
    return obs


def build_master(obs):
    """按 (运单,节点) 选优先级最高的事件为主记录（event_time/event_id 兜底确定性）。"""
    w = Window.partitionBy("waybill_no", "event_type").orderBy(
        F.col("priority").asc(), F.col("event_time").asc(),
        F.col("event_id").asc())
    ranked = obs.withColumn("_rn", F.row_number().over(w))
    master = (
        ranked.filter(F.col("_rn") == 1).drop("_rn")
        .select(
            F.col("waybill_no"), F.col("event_type"),
            F.col("event_id").alias("master_event_id"),
            F.col("source_system").alias("master_source_system"),
            F.col("event_time").alias("master_event_time"),
            F.col("report_time").alias("master_report_time"),
        )
    )
    agg = (
        obs.groupBy("waybill_no", "event_type")
        .agg(
            F.countDistinct("source_system").alias("source_count"),
            F.to_json(F.collect_set("source_system")).alias("source_systems"),
        )
    )
    return master.join(agg, ["waybill_no", "event_type"])


# ---------------------------------------------------------------------
# 2. 交叉校验：时间矛盾 / 终态状态矛盾
# ---------------------------------------------------------------------
def _infer_time_reason(outlier_source: str, max_diff: int, tolerance: int) -> str:
    if outlier_source == "CS":
        return "MANUAL_BACKFILL_LAG"
    if outlier_source == "ROUTE":
        return "ROUTE_CALLBACK_DELAY"
    # 差异在容差 4 倍以内优先判时钟偏差，更远的差异判离群来源
    if max_diff <= tolerance * 4:
        return "CLOCK_SKEW"
    return "OUTLIER_SOURCE"


# 时间/状态冲突在合并前的统一中间 schema（含推断原因码，共 11 列）
CONFLICT_STAGING_SCHEMA = (
    "waybill_no string, event_type string, conflict_field string, "
    "master_event_id string, master_source_system string, master_value string, "
    "source_values string, max_diff_seconds bigint, master_diff_seconds bigint, "
    "outlier_source_system string, reason_code string"
)


def build_time_conflicts(spark, obs, master, tolerance: int):
    """同节点多来源事件时间差异超容差 -> EVENT_TIME 冲突行。"""
    bounds = (
        obs.groupBy("waybill_no", "event_type")
        .agg(F.min("event_time").alias("min_et"),
             F.max("event_time").alias("max_et"),
             F.countDistinct("source_system").alias("src_cnt"))
        .filter((F.col("src_cnt") >= 2)
                & ((F.unix_timestamp("max_et") - F.unix_timestamp("min_et"))
                   > tolerance))
    )
    if bounds.count() == 0:
        return spark.createDataFrame([], CONFLICT_STAGING_SCHEMA)

    enriched = (
        obs.join(bounds.select("waybill_no", "event_type"),
                 ["waybill_no", "event_type"], "left_semi")
        .join(master, ["waybill_no", "event_type"], "inner")
        .withColumn("diff_vs_master",
                    F.abs(F.unix_timestamp("event_time")
                          - F.unix_timestamp("master_event_time")))
    )
    # 离群来源 = 与主记录时间差距最大的来源（并列时取优先级高者，保证确定性）
    w_out = Window.partitionBy("waybill_no", "event_type").orderBy(
        F.col("diff_vs_master").desc(), F.col("priority").asc(),
        F.col("event_id").asc())
    outlier = (
        enriched.withColumn("_orn", F.row_number().over(w_out))
        .filter(F.col("_orn") == 1)
        .select(F.col("waybill_no"), F.col("event_type"),
                F.col("source_system").alias("outlier_source_system"),
                F.col("diff_vs_master").alias("master_diff_seconds"))
    )
    values = (
        enriched.groupBy("waybill_no", "event_type")
        .agg(F.to_json(F.collect_list(F.struct(
                F.col("source_system"), F.col("event_id"),
                F.date_format(F.col("event_time"),
                              "yyyy-MM-dd HH:mm:ss").alias("event_time"),
                F.date_format(F.col("report_time"),
                              "yyyy-MM-dd HH:mm:ss").alias("report_time"),
                F.col("diff_vs_master").alias("delta_seconds_vs_master")))
            ).alias("source_values"))
    )
    rows = (
        bounds.join(master, ["waybill_no", "event_type"])
        .join(outlier, ["waybill_no", "event_type"])
        .join(values, ["waybill_no", "event_type"])
        .select(
            F.col("waybill_no"), F.col("event_type"),
            F.lit("EVENT_TIME").alias("conflict_field"),
            F.col("master_event_id"), F.col("master_source_system"),
            F.date_format(F.col("master_event_time"),
                          "yyyy-MM-dd HH:mm:ss").alias("master_value"),
            F.col("source_values"),
            (F.unix_timestamp("max_et") - F.unix_timestamp("min_et"))
                .alias("max_diff_seconds"),
            F.col("master_diff_seconds"),
            F.col("outlier_source_system"),
        )
    )
    # 原因码依赖离群来源与差异量级，逐行推断（数据量为冲突节点数，collect 可接受）
    out_rows = []
    for r in rows.collect():
        reason = _infer_time_reason(r.outlier_source_system,
                                    int(r.max_diff_seconds), tolerance)
        out_rows.append((r.waybill_no, r.event_type, "EVENT_TIME",
                         r.master_event_id, r.master_source_system,
                         r.master_value, r.source_values,
                         int(r.max_diff_seconds),
                         int(r.master_diff_seconds),
                         r.outlier_source_system, reason))
    return spark.createDataFrame(out_rows, CONFLICT_STAGING_SCHEMA)


def build_status_conflicts(obs, conf: dict):
    """终态互斥：同一运单同时出现 SIGN 与 REJECT 观测 -> EVENT_STATUS 冲突。"""
    terms = terminal_nodes(conf)
    terminal = obs.filter(F.col("event_type").isin(terms))
    present = (
        terminal.groupBy("waybill_no")
        .agg(F.collect_set("event_type").alias("term_set"))
        .filter(F.size(F.array_intersect(
            F.col("term_set"), F.array(*[F.lit(t) for t in terms]))) >= 2)
    )
    if present.count() == 0:
        return obs.sparkSession.createDataFrame([], CONFLICT_STAGING_SCHEMA)
    w = Window.partitionBy("waybill_no").orderBy(
        F.col("priority").asc(), F.col("event_time").asc(),
        F.col("event_id").asc())
    ranked = terminal.join(present.select("waybill_no"), "waybill_no", "left_semi") \
                     .withColumn("_rn", F.row_number().over(w))
    master_t = (ranked.filter(F.col("_rn") == 1)
                .select(F.col("waybill_no"),
                        F.col("event_id").alias("master_event_id"),
                        F.col("source_system").alias("master_source_system"),
                        F.col("event_type").alias("master_value")))
    values = (
        ranked.groupBy("waybill_no")
        .agg(F.to_json(F.collect_list(F.struct(
                F.col("source_system"), F.col("event_id"),
                F.date_format(F.col("event_time"),
                              "yyyy-MM-dd HH:mm:ss").alias("event_time"),
                F.date_format(F.col("report_time"),
                              "yyyy-MM-dd HH:mm:ss").alias("report_time"),
                F.col("event_type").alias("field_value")))
            ).alias("source_values"))
    )
    rows = (
        master_t.join(values, "waybill_no")
        .select(
            F.col("waybill_no"), F.lit("TERMINAL").alias("event_type"),
            F.lit("EVENT_STATUS").alias("conflict_field"),
            F.col("master_event_id"), F.col("master_source_system"),
            F.col("master_value"), F.col("source_values"),
            F.lit(None).cast("bigint").alias("max_diff_seconds"),
            F.lit(None).cast("bigint").alias("master_diff_seconds"),
            F.lit(None).cast("string").alias("outlier_source_system"),
            F.lit("STATUS_MISRECORD").alias("reason_code"),
        )
    )
    return rows


def build_conflicts(spark, obs, master, conf: dict):
    tolerance = int(conf.get("reconcile.time.tolerance.seconds", "300"))
    time_c = build_time_conflicts(spark, obs, master, tolerance)
    status_c = build_status_conflicts(obs, conf)
    conflicts = time_c.unionByName(status_c)
    if conflicts.count() == 0:
        return conflicts, tolerance
    conflicts = (
        conflicts
        .withColumn("possible_reason",
                    F.create_map([x for kv in REASON_TEXT.items() for x in
                                  (F.lit(kv[0]), F.lit(kv[1]))])[F.col("reason_code")])
        .withColumn("tolerance_seconds", F.lit(tolerance).cast("bigint"))
        .withColumn("conflict_id",
                    F.sha2(F.concat_ws("|", F.col("waybill_no"),
                                       F.col("event_type"),
                                       F.col("conflict_field")), 256))
    )
    return conflicts, tolerance


# ---------------------------------------------------------------------
# 3. 冲突条目幂等落表（不覆盖人工裁决列）
# ---------------------------------------------------------------------
def merge_conflicts(spark, conflicts, batch_id: str,
                    affected_waybills) -> None:
    if conflicts.count() == 0:
        current_ids = []
    else:
        conflicts.createOrReplaceTempView("stg_conflict")
        spark.sql(f"""
            MERGE INTO {CONFLICT_TABLE} t
            USING stg_conflict s
            ON t.conflict_id = s.conflict_id
            WHEN MATCHED AND t.status = 'OPEN' THEN UPDATE SET
                t.master_event_id = s.master_event_id,
                t.master_source_system = s.master_source_system,
                t.master_value = s.master_value,
                t.source_values = s.source_values,
                t.max_diff_seconds = s.max_diff_seconds,
                t.master_diff_seconds = s.master_diff_seconds,
                t.tolerance_seconds = s.tolerance_seconds,
                t.reason_code = s.reason_code,
                t.possible_reason = s.possible_reason,
                t.outlier_source_system = s.outlier_source_system,
                t.last_seen_batch_id = '{batch_id}',
                t.last_seen_time = current_timestamp()
            WHEN NOT MATCHED THEN INSERT *
        """)
        current_ids = [r.conflict_id for r in
                       conflicts.select("conflict_id").collect()]

    # 受影响运单上一轮存在、本轮已观测不到的 OPEN 冲突 -> INVALID（数据已修正）
    afl = affected_waybills
    old_open = (
        spark.table(CONFLICT_TABLE).alias("c")
        .join(afl.alias("a"), on="waybill_no", how="inner")
        .filter(F.col("status") == "OPEN")
        .select("conflict_id").distinct()
    )
    if current_ids:
        old_open = old_open.filter(~F.col("conflict_id").isin(current_ids))
    invalid = [r.conflict_id for r in old_open.collect()]
    if invalid:
        spark.sql(f"""
            UPDATE {CONFLICT_TABLE}
            SET status = 'INVALID',
                adjudication_note = coalesce(adjudication_note,
                    '重跑交叉校验时冲突条件不再成立，自动置 INVALID'),
                last_seen_time = current_timestamp()
            WHERE status = 'OPEN' AND conflict_id IN ({",".join(
                "'" + i + "'" for i in invalid)})
        """)
    # 新冲突补齐审计列（MERGE 的 INSERT * 已要求全列，stg 缺列在落表前补齐，见 run()）
    print(f"[reconcile] 本轮冲突条目={len(current_ids)} 失效条目={len(invalid)}")


# ---------------------------------------------------------------------
# 4. 已发布规则自动裁决（DRAFT 规则不可见）
# ---------------------------------------------------------------------
def apply_published_rules(spark, batch_id: str, current_version: str) -> int:
    rules = (
        spark.table(RULE_TABLE)
        .filter(F.col("rule_status") == "PUBLISHED")
        # 只允许在已发布口径版本中生效的规则；当前版本之前发布的规则继续适用
        .join(spark.table(CALIBRE_TABLE)
              .filter(F.col("version_status") == "PUBLISHED")
              .select(F.col("calibre_version").alias("eff_ver")),
              F.col("effective_calibre_version") == F.col("eff_ver"),
              "inner")
        .orderBy(F.col("rule_id").asc())
    )
    rule_rows = rules.collect()
    if not rule_rows:
        return 0
    applied = 0
    for rule in rule_rows:
        # 每条规则重新读取 OPEN 集合：前一条规则裁决掉的冲突不再参与匹配，
        # 避免同一条冲突被重复计数
        open_conflicts = spark.table(CONFLICT_TABLE).filter(
            F.col("status") == "OPEN")
        cand = open_conflicts.filter(
            (F.col("event_type") == rule.event_type)
            & (F.col("conflict_field") == rule.conflict_field))
        if rule.outlier_source_system:
            cand = cand.filter(
                F.col("outlier_source_system") == rule.outlier_source_system)
        if rule.reason_code:
            cand = cand.filter(F.col("reason_code") == rule.reason_code)
        ids = [r.conflict_id for r in cand.select("conflict_id").collect()]
        if not ids:
            continue
        id_list = ",".join("'" + i + "'" for i in ids)
        if rule.decision == "PREFER_SOURCE":
            params = json.loads(rule.decision_value)
            prefer = params["prefer_source"]
            # 采用指定来源的观测值：从 source_values JSON 中提取该来源事件
            picked = (
                spark.table(CONFLICT_TABLE)
                .filter(F.col("conflict_id").isin(ids))
                .select(
                    F.col("conflict_id"),
                    F.explode(F.from_json(
                        F.col("source_values"),
                        "array<struct<source_system:string,event_id:string,"
                        "event_time:string,report_time:string,"
                        "field_value:string,delta_seconds_vs_master:bigint>>")
                    ).alias("sv"))
                .filter(F.col("sv.source_system") == prefer)
                .select(F.col("conflict_id"),
                        F.col("sv.event_id").alias("pick_event_id"),
                        F.coalesce(F.col("sv.field_value"),
                                   F.col("sv.event_time")).alias("pick_value"))
            ).collect()
            pick_map = {r.conflict_id: (r.pick_event_id, r.pick_value)
                        for r in picked}
            for cid, (eid, val) in pick_map.items():
                spark.sql(f"""
                    UPDATE {CONFLICT_TABLE}
                    SET status = 'ADJUDICATED',
                        adjudicated_value = '{val.replace("'", "''")}',
                        adjudicated_event_id = '{eid}',
                        adjudicated_by = 'RULE:{rule.rule_id}',
                        adjudicated_time = current_timestamp(),
                        rule_id = '{rule.rule_id}',
                        effective_calibre_version = '{rule.effective_calibre_version}'
                    WHERE conflict_id = '{cid}' AND status = 'OPEN'
                """)
            applied += len(pick_map)
        elif rule.decision == "DISMISS":
            spark.sql(f"""
                UPDATE {CONFLICT_TABLE}
                SET status = 'ADJUDICATED',
                    adjudicated_value = master_value,
                    adjudicated_event_id = master_event_id,
                    adjudicated_by = 'RULE:{rule.rule_id}',
                    adjudicated_time = current_timestamp(),
                    rule_id = '{rule.rule_id}',
                    effective_calibre_version = '{rule.effective_calibre_version}'
                WHERE status = 'OPEN' AND conflict_id IN ({id_list})
            """)
            applied += len(ids)
    print(f"[reconcile] 已发布规则自动裁决 {applied} 条（版本 {current_version}）")
    return applied


# ---------------------------------------------------------------------
# 5. 节点主记录落表 + 待核查队列重建
# ---------------------------------------------------------------------
def write_node_master(spark, master, affected, batch_id: str,
                      version: str) -> None:
    open_cnt = (
        spark.table(CONFLICT_TABLE).filter(F.col("status") == "OPEN")
        .groupBy("waybill_no", "event_type").count()
        .select(F.col("waybill_no"), F.col("event_type"),
                F.col("count").alias("open_conflict_count"))
    )
    adj_cnt = (
        spark.table(CONFLICT_TABLE).filter(F.col("status") == "ADJUDICATED")
        .groupBy("waybill_no", "event_type").count()
        .select(F.col("waybill_no"), F.col("event_type"),
                F.col("count").alias("adj_count"))
    )
    out = (
        master.join(open_cnt, ["waybill_no", "event_type"], "left")
        .join(adj_cnt, ["waybill_no", "event_type"], "left")
        .withColumn("conflict_flag",
                    F.coalesce(F.col("open_conflict_count") > 0, F.lit(False)))
        .withColumn("adjudicated_flag",
                    F.coalesce(F.col("adj_count") > 0, F.lit(False)))
        .withColumn("open_conflict_count",
                    F.coalesce(F.col("open_conflict_count"), F.lit(0)))
        .withColumn("calibre_version", F.lit(version))
        .withColumn("biz_date", F.to_date(F.col("master_event_time")))
        .withColumn("ingest_batch_id", F.lit(batch_id))
        .withColumn("ingest_time", F.current_timestamp())
        .select("waybill_no", "event_type", "master_event_id",
                "master_source_system", "master_event_time",
                "master_report_time", "source_count", "source_systems",
                "conflict_flag", "open_conflict_count", "adjudicated_flag",
                "calibre_version", "biz_date", "ingest_batch_id", "ingest_time")
    )
    # 受影响运单动态分区覆盖（与 dwd_detail_job.replace_affected 同构）
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    new_parts = out.select("biz_date").distinct()
    old_parts = (
        spark.table(NODE_MASTER).alias("t")
        .join(affected.alias("a"), on="waybill_no", how="inner")
        .select(F.col("biz_date")).distinct()
    )
    parts = new_parts.union(old_parts).distinct()
    rows_in_parts = (
        spark.table(NODE_MASTER).alias("t")
        .join(parts.alias("p"),
              F.col("t.biz_date") == F.col("p.biz_date"),
              how="left_semi")
    )
    kept = rows_in_parts.join(affected, on="waybill_no", how="left_anti")
    # semi join 的 USING 键会扰动列序，INSERT OVERWRITE 按位置映射，
    # 必须按目标表 DDL 列序重排，避免 STRING 错位写入 TIMESTAMP 列
    target_cols = spark.table(NODE_MASTER).columns
    kept.unionByName(out).select(*target_cols) \
        .createOrReplaceTempView("tmp_node_master_rebuilt")
    spark.sql(
        f"INSERT OVERWRITE TABLE {NODE_MASTER} "
        "SELECT * FROM tmp_node_master_rebuilt")


def rebuild_review_queue(spark, affected, batch_id: str, version: str) -> None:
    conf_all = spark.table(CONFLICT_TABLE).join(
        affected, on="waybill_no", how="inner")
    cur = (
        conf_all.groupBy("waybill_no")
        .agg(
            F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))
                .alias("open_cnt"),
            F.to_json(F.filter(
                F.collect_list(
                    F.when(F.col("status") == "OPEN", F.col("conflict_id"))),
                lambda x: x.isNotNull())).alias("conflict_ids"),
            F.to_json(F.filter(
                F.collect_set(
                    F.when(F.col("status") == "OPEN", F.col("conflict_field"))),
                lambda x: x.isNotNull())).alias("conflict_fields"),
            F.to_json(F.filter(
                F.collect_set(
                    F.when(F.col("status") == "OPEN", F.col("reason_code"))),
                lambda x: x.isNotNull())).alias("reason_codes"),
            F.max(F.when(F.col("status") == "ADJUDICATED",
                         F.col("adjudicated_by"))).alias("last_adj_by"),
        )
    )
    existing = {r.waybill_no: r for r in
                spark.table(REVIEW_QUEUE).join(
                    affected, on="waybill_no", how="inner").collect()}
    rows = []
    for r in cur.collect():
        prev = existing.get(r.waybill_no)
        first_seen = prev.first_seen_time if prev else None
        if r.open_cnt > 0:
            rows.append((r.waybill_no, "PENDING", int(r.open_cnt),
                         r.conflict_ids, r.conflict_fields, r.reason_codes,
                         first_seen, None, version, batch_id))
        else:
            # 解决人取该运单最近一次裁决人（人工账号或 RULE:<id>），保留历史兜底
            resolved_by = r.last_adj_by or (
                prev.resolved_by if prev else None)
            rows.append((r.waybill_no, "RESOLVED", 0,
                         r.conflict_ids, r.conflict_fields, r.reason_codes,
                         first_seen, resolved_by, version, batch_id))
    # 先删除受影响运单旧队列行，再原子追加新行（队列单行 per 运单）
    afl_ids = [r.waybill_no for r in affected.collect()]
    if afl_ids:
        id_list = ",".join("'" + x + "'" for x in afl_ids)
        spark.sql(f"DELETE FROM {REVIEW_QUEUE} WHERE waybill_no IN ({id_list})")
    if rows:
        from pyspark.sql.types import (
            IntegerType, StringType, StructField, StructType, TimestampType,
        )
        schema = StructType([
            StructField("waybill_no", StringType()),
            StructField("queue_status", StringType()),
            StructField("open_conflict_count", IntegerType()),
            StructField("conflict_ids", StringType()),
            StructField("conflict_fields", StringType()),
            StructField("reason_codes", StringType()),
            StructField("first_seen_time", TimestampType()),
            StructField("resolved_by", StringType()),
            StructField("current_calibre_version", StringType()),
            StructField("ingest_batch_id", StringType()),
        ])
        df = spark.createDataFrame(rows, schema)
        (df.withColumn("first_seen_time",
                       F.coalesce(F.col("first_seen_time"), F.current_timestamp()))
           .withColumn("last_seen_time", F.current_timestamp())
           .withColumn("resolved_time",
                       F.when(F.col("queue_status") == "RESOLVED",
                              F.current_timestamp()))
           .select("waybill_no", "queue_status", "open_conflict_count",
                   "conflict_ids", "conflict_fields", "reason_codes",
                   "first_seen_time", "last_seen_time", "resolved_time",
                   "resolved_by", "current_calibre_version",
                   "ingest_batch_id", F.current_timestamp().alias("ingest_time"))
           .writeTo(REVIEW_QUEUE).append())


def run_reconcile(spark, conf: dict, start_date=None, end_date=None,
                  waybill_nos=None) -> dict:
    batch_id = new_batch_id("rec")
    version = ensure_current_version(spark, conf)
    affected = _affected_waybills(spark, start_date, end_date, waybill_nos)
    affected.cache()
    if affected.count() == 0:
        print("[reconcile] 范围内无运单，跳过")
        return {"waybills": 0, "conflicts": 0, "auto_adjudicated": 0}

    obs = build_node_observations(spark, conf, affected)
    obs.cache()
    master = build_master(obs)
    master.cache()

    conflicts, tolerance = build_conflicts(spark, obs, master, conf)
    if conflicts.count() > 0:
        # 补齐冲突表全列，供 MERGE INSERT * 使用
        conflicts = (
            conflicts
            .withColumn("status", F.lit("OPEN"))
            .withColumn("adjudicated_value", F.lit(None).cast("string"))
            .withColumn("adjudicated_event_id", F.lit(None).cast("string"))
            .withColumn("adjudicated_by", F.lit(None).cast("string"))
            .withColumn("adjudicated_time", F.lit(None).cast("timestamp"))
            .withColumn("adjudication_note", F.lit(None).cast("string"))
            .withColumn("rule_id", F.lit(None).cast("string"))
            .withColumn("effective_calibre_version", F.lit(None).cast("string"))
            .withColumn("calibre_version_at_detect", F.lit(version))
            .withColumn("detect_date", F.current_date())
            .withColumn("first_detected_batch_id", F.lit(batch_id))
            .withColumn("first_detected_time", F.current_timestamp())
            .withColumn("last_seen_batch_id", F.lit(batch_id))
            .withColumn("last_seen_time", F.current_timestamp())
            .withColumn("ingest_time", F.current_timestamp())
            .select(*spark.table(CONFLICT_TABLE).columns)
        )
    merge_conflicts(spark, conflicts, batch_id, affected)
    auto = apply_published_rules(spark, batch_id, version)
    write_node_master(spark, master, affected, batch_id, version)
    rebuild_review_queue(spark, affected, batch_id, version)

    open_n = (spark.table(CONFLICT_TABLE)
              .join(affected, on="waybill_no", how="inner")
              .filter(F.col("status") == "OPEN").count())
    result = {"waybills": affected.count(),
              "conflict_rows_detected": conflicts.count(),
              "open_conflicts": open_n,
              "auto_adjudicated": auto,
              "tolerance_seconds": tolerance,
              "calibre_version": version}
    print("[reconcile] " + json.dumps(result, ensure_ascii=False))
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--biz-date", default=None)
    ap.add_argument("--start-date", default=None)
    ap.add_argument("--end-date", default=None)
    ap.add_argument("--scope", choices=["WAYBILL", "DATE_RANGE"],
                    default="DATE_RANGE")
    ap.add_argument("--waybill-nos", default="")
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_reconcile", conf)
    try:
        nos = [x.strip() for x in args.waybill_nos.split(",") if x.strip()] or None
        if args.scope == "WAYBILL":
            run_reconcile(spark, conf, waybill_nos=nos)
        else:
            run_reconcile(spark, conf,
                          start_date=args.start_date or args.biz_date,
                          end_date=args.end_date or args.biz_date,
                          waybill_nos=None)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
