"""时效统计作业：基于节点主记录产出按口径版本不可变的时效结果。

硬性口径（对应需求）：
  1. 待核查队列中 queue_status='PENDING' 的运单一律排除——冲突未裁决前
     不进入时效统计；它们只存在于 meta.waybill_review_queue 与冲突表中。
  2. 所有节点时间取 dwd.waybill_node_master 的主记录（来源优先级选定），
     不直接扫多来源明细，保证口径唯一且可血缘追溯（node_sources 记录每节点主来源）。
  3. 裁决生效版本边界：人工裁决只对"裁决时间之后发布的版本"生效，规则裁决只对
     "规则生效版本及之后"生效；对更早版本重算出报时这些运单被自动排除，
     保证已经发布的报表不会被后续裁决改变。
  4. 按 (calibre_version × 运单) 【只追加不重写】：作业全程只发 INSERT、不做
     UPDATE/DELETE；同版本下已出报的运单在计算前即被剔除（幂等跳过），
     新业务日期闭环的新运单持续追加，因此每日调度可安全重跑，已发布报表行
     物理上不可能被改写。裁决/新规则发布为下一口径版本后，重算结果进入新版本
     分区，旧版本分区原封不动。

用法：
  spark-submit jobs/timeliness_job.py --calibre-version v2026.11.0
  # 缺省 --calibre-version 时取当前最新 PUBLISHED 版本
  spark-submit jobs/timeliness_job.py --start-date 2026-11-01 --end-date 2026-11-06
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F

from common.spark_utils import (
    CALIBRE_TABLE, NODE_MASTER, REVIEW_QUEUE, TIMELINESS_TABLE,
    build_spark, load_conf, new_batch_id,
)


def current_published_version(spark) -> str:
    row = (spark.table(CALIBRE_TABLE)
           .filter(F.col("version_status") == "PUBLISHED")
           .orderBy(F.col("published_time").desc())
           .select("calibre_version").first())
    if row is None:
        raise RuntimeError("不存在 PUBLISHED 口径版本，请先运行 reconcile_job 初始化")
    return row.calibre_version


def run_timeliness(spark, conf: dict, calibre_version: str | None,
                   start_date=None, end_date=None) -> dict:
    version = calibre_version or current_published_version(spark)
    vrow = (spark.table(CALIBRE_TABLE)
            .filter((F.col("calibre_version") == version)
                    & (F.col("version_status") == "PUBLISHED")).first())
    if vrow is None:
        raise ValueError(f"口径版本 {version} 不存在或未 PUBLISHED，拒绝出报")

    batch_id = new_batch_id("tlm")
    master = spark.table(NODE_MASTER)
    if start_date:
        master = master.filter(F.col("biz_date") >= F.lit(start_date))
    if end_date:
        master = master.filter(F.col("biz_date") <= F.lit(end_date))

    # 排除待核查队列中的 PENDING 运单（RESOLVED 运单仍需按裁决生效版本再过滤）
    pending = (spark.table(REVIEW_QUEUE)
               .filter(F.col("queue_status") == "PENDING")
               .select("waybill_no").distinct())
    # 排除本版本已出报的运单：已发布行只追加不重写，重跑/每日调度自动幂等跳过
    existing_wb = (spark.table(TIMELINESS_TABLE)
                   .filter(F.col("calibre_version") == version)
                   .select("waybill_no").distinct())

    # 裁决生效版本边界（对应需求"裁决结果只能在下一个口径版本生效"）：
    #   OPEN 冲突：任何版本都排除（与 PENDING 队列等价，双保险）；
    #   规则裁决（effective_calibre_version 非空）：该版本发布时间晚于当前版本则排除；
    #   人工裁决（effective_calibre_version 为空）：裁决时间不早于当前版本发布时间，
    #     说明裁决发生在该版本发布之后，该版本不得纳入，只能等下一个版本。
    v_pub = (spark.table(CALIBRE_TABLE)
             .filter(F.col("calibre_version") == version)
             .select("published_time").first()["published_time"])
    eff_versions = (spark.table(CALIBRE_TABLE)
                    .select(F.col("calibre_version").alias("eff_v"),
                            F.col("published_time").alias("eff_pub_time")))
    blocked_wb = (
        spark.table("ltl.meta.waybill_conflict")
        .filter(F.col("status").isin("OPEN", "ADJUDICATED"))
        .join(eff_versions,
              F.col("effective_calibre_version") == F.col("eff_v"), "left")
        .withColumn("is_blocked",
                    F.when(F.col("status") == "OPEN", F.lit(True))
                    .when(F.col("effective_calibre_version").isNotNull(),
                          F.col("eff_pub_time") > F.lit(v_pub))
                    .otherwise(F.col("adjudicated_time") >= F.lit(v_pub)))
        .filter(F.col("is_blocked"))
        .select("waybill_no").distinct()
    )
    eligible = (master
                .join(pending, on="waybill_no", how="left_anti")
                .join(blocked_wb, on="waybill_no", how="left_anti")
                .join(existing_wb, on="waybill_no", how="left_anti"))

    pivot = (
        eligible.groupBy("waybill_no")
        .agg(
            F.max(F.when(F.col("event_type") == "PICKUP",
                         F.col("master_event_time"))).alias("pickup_time"),
            F.max(F.when(F.col("event_type") == "DELIVER",
                         F.col("master_event_time"))).alias("deliver_time"),
            F.max(F.when(F.col("event_type") == "SIGN",
                         F.col("master_event_time"))).alias("sign_time"),
            F.max(F.when(F.col("event_type") == "REJECT",
                         F.col("master_event_time"))).alias("reject_time"),
            F.to_json(F.map_from_entries(
                F.collect_list(F.struct("event_type", "master_source_system")))
            ).alias("node_sources"),
        )
        # 时效可算的前提：有揽收时间且有终态（签收或拒收）时间
        .filter(F.col("pickup_time").isNotNull()
                & (F.col("sign_time").isNotNull()
                   | F.col("reject_time").isNotNull()))
    )

    result = (
        pivot
        .withColumn(
            "transit_hours",
            F.round(
                (F.unix_timestamp(F.coalesce("sign_time", "reject_time"))
                 - F.unix_timestamp("pickup_time")) / 3600.0, 2))
        .withColumn(
            "deliver_hours",
            F.when(F.col("deliver_time").isNotNull(),
                   F.round((F.unix_timestamp("deliver_time")
                            - F.unix_timestamp("pickup_time")) / 3600.0, 2)))
        .withColumn(
            "sign_leg_hours",
            F.when(F.col("sign_time").isNotNull()
                   & F.col("deliver_time").isNotNull(),
                   F.round((F.unix_timestamp("sign_time")
                            - F.unix_timestamp("deliver_time")) / 3600.0, 2)))
        .withColumn("calibre_version", F.lit(version))
        .withColumn("conflict_free_flag", F.lit(True))
        .withColumn("calc_batch_id", F.lit(batch_id))
        .withColumn("calc_time", F.current_timestamp())
        .select("calibre_version", "waybill_no", "pickup_time", "deliver_time",
                "sign_time", "reject_time", "transit_hours", "deliver_hours",
                "sign_leg_hours", "node_sources", "conflict_free_flag",
                "calc_batch_id", "calc_time")
    )
    result.cache()
    n = result.count()
    excluded = pending.count()
    skipped_existing = existing_wb.count()
    blocked = blocked_wb.count()

    # 防御性红线：经过 anti-join 后不应再与已发布运单有交集；若出现说明逻辑被破坏
    overlap = (result.select("waybill_no").distinct()
               .join(existing_wb, on="waybill_no", how="inner").count())
    if overlap > 0:
        raise RuntimeError(
            f"口径版本 {version} 检测到 {overlap} 个已出报运单试图重写，"
            "已发布报表不可变")

    result.writeTo(TIMELINESS_TABLE).append()

    out = {"calibre_version": version, "stat_rows_new": n,
           "skipped_existing_waybills": skipped_existing,
           "pending_waybills_excluded": excluded,
           "version_boundary_blocked_waybills": blocked,
           "batch_id": batch_id}
    print("[timeliness] " + json.dumps(out, ensure_ascii=False))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--calibre-version", default=None)
    ap.add_argument("--start-date", default=None)
    ap.add_argument("--end-date", default=None)
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_timeliness", conf)
    try:
        run_timeliness(spark, conf, args.calibre_version,
                       start_date=args.start_date, end_date=args.end_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
