"""补数作业（独立入口）：按运单号或时间区间只重跑受影响分区。

治理约束（对应需求）：
  * 绝不整表重算：受影响集合 = 命中的运单；只重放这些运单的事件。
  * 先建独立影子表（生产表只读），产出补数前后对比结果；
    未加 --switch 时状态停在 DIFF_READY，生产数据零改动。
  * --switch 对比通过后才生效：默认走受影响分区的原子动态覆盖，
    无关分区物理不动；--full-rebuild 走"独立表重建 + 换表"，
    旧表保留为回滚点（backup 表），全程不直接覆盖生产分区。
  * 每次运行写 meta.backfill_job_log，含分区清单、前后行数、差异摘要。

用法：
  # 按运单号补数（先只出对比）
  spark-submit jobs/backfill_job.py --scope WAYBILL --waybill-nos WB001,WB002
  # 按时间区间补数并生效
  spark-submit jobs/backfill_job.py --scope DATE_RANGE \
      --start-date 2026-10-20 --end-date 2026-10-25 --switch
  # 全量重建（独立表 + 换表，旧表备份）
  spark-submit jobs/backfill_job.py --scope DATE_RANGE \
      --start-date 2026-10-01 --end-date 2026-11-11 --full-rebuild --switch
"""
from __future__ import annotations

import argparse
import json
import os

from pyspark.sql import functions as F

from common.spark_utils import (
    BACKFILL_LOG, DWD_EVENT, build_spark, load_conf, new_batch_id,
)
from dwd_detail_job import build_detail, replace_affected


def _write_log(spark, row: dict) -> None:
    """以 DataFrame 追加台账，避免 JSON 文本中的引号转义问题。"""
    from pyspark.sql.types import (
        BooleanType, LongType, StringType, StructField, StructType,
    )
    schema = StructType([
        StructField("job_id", StringType()),
        StructField("scope_type", StringType()),
        StructField("scope_value", StringType()),
        StructField("affected_partitions", StringType()),
        StructField("rows_before", LongType()),
        StructField("rows_after", LongType()),
        StructField("diff_summary", StringType()),
        StructField("shadow_table", StringType()),
        StructField("view_switched", BooleanType()),
        StructField("status", StringType()),
        StructField("ingest_batch_id", StringType()),
    ])
    spark.createDataFrame([(
        row["job_id"], row["scope_type"], row["scope_value"],
        row["affected_partitions"], row["rows_before"], row["rows_after"],
        row["diff_summary"], row["shadow_table"], row["view_switched"],
        row["status"], row["ingest_batch_id"],
    )], schema=schema).withColumn(
        "ingest_time", F.current_timestamp()
    ).writeTo(BACKFILL_LOG).append()


def _diff_before_after(spark, affected, shadow: str) -> dict:
    """对比受影响运单在生产表与影子表中的差异。"""
    prod = spark.table(DWD_EVENT).join(affected, on="waybill_no", how="left_semi")
    cand = spark.table(shadow).join(affected, on="waybill_no", how="left_semi")

    key_cols = ["waybill_no", "event_id"]
    # 全行哈希（不含入湖审计列），用于识别内容变化
    biz_cols = [c for c in prod.columns
                if c not in ("ingest_batch_id", "ingest_time")]
    prod_h = prod.select(*key_cols,
                         F.hash(*[F.col(c) for c in biz_cols]).alias("h"),
                         F.col("event_seq").alias("seq_prod"))
    cand_h = cand.select(*key_cols,
                         F.hash(*[F.col(c) for c in biz_cols]).alias("h"),
                         F.col("event_seq").alias("seq_new"))
    j = prod_h.alias("p").join(
        cand_h.alias("c"), key_cols, how="full")

    added = j.filter(F.col("p.event_id").isNull()).count()
    removed = j.filter(F.col("c.event_id").isNull()).count()
    changed = j.filter(
        F.col("p.event_id").isNotNull() & F.col("c.event_id").isNotNull()
        & (F.col("p.h") != F.col("c.h"))).count()
    seq_reordered = j.filter(
        F.col("p.event_id").isNotNull() & F.col("c.event_id").isNotNull()
        & (F.col("p.seq_prod") != F.col("c.seq_new"))).count()

    return {"added_rows": added, "removed_rows": removed,
            "content_changed_rows": changed,
            "seq_reordered_rows": seq_reordered}


def run_backfill(spark, conf: dict, scope_type: str,
                 waybill_nos=None, start_date=None, end_date=None,
                 switch: bool = False, full_rebuild: bool = False) -> dict:
    job_id = new_batch_id("bf")
    batch_id = job_id
    scope_value = (",".join(waybill_nos) if scope_type == "WAYBILL"
                   else f"[{start_date},{end_date}]")

    detail, affected, stats = build_detail(
        spark,
        start_date=start_date if scope_type == "DATE_RANGE" else None,
        end_date=end_date if scope_type == "DATE_RANGE" else None,
        waybill_nos=waybill_nos if scope_type == "WAYBILL" else None,
        batch_id=batch_id)

    if detail is None:
        result = {"job_id": job_id, "status": "NO_OP",
                  "message": "范围内无受影响运单"}
        print("[backfill] " + json.dumps(result, ensure_ascii=False))
        return result

    affected.cache()
    detail.cache()
    part_rows = (detail.select("event_date", "source_system").distinct()
                 .collect())
    affected_partitions = sorted(
        {f"{r.event_date}|{r.source_system}" for r in part_rows})

    shadow = f"ltl.dwd.waybill_event_detail_shadow_{job_id.replace('-', '_')}"
    # 独立影子表 = 生产表剔除受影响运单 + 重算结果（生产表全程只读）
    kept = spark.table(DWD_EVENT).join(affected, on="waybill_no", how="left_anti")
    spark.sql(f"DROP TABLE IF EXISTS {shadow}")
    kept.unionByName(detail).writeTo(shadow).using("iceberg").create()

    rows_before = (spark.table(DWD_EVENT)
                   .join(affected, on="waybill_no", how="left_semi").count())
    rows_after = (spark.table(shadow)
                  .join(affected, on="waybill_no", how="left_semi").count())
    diff = _diff_before_after(spark, affected, shadow)

    out_dir = conf.get("backfill.output.dir", "/opt/data/backfill")
    os.makedirs(out_dir, exist_ok=True)
    diff_path = os.path.join(out_dir, f"{job_id}_diff.json")
    with open(diff_path, "w", encoding="utf-8") as f:
        json.dump({"job_id": job_id, "scope_type": scope_type,
                   "scope_value": scope_value,
                   "affected_partitions": affected_partitions,
                   "rows_before": rows_before, "rows_after": rows_after,
                   "diff": diff}, f, ensure_ascii=False, indent=2)

    base_log = {
        "job_id": job_id, "scope_type": scope_type, "scope_value": scope_value,
        "affected_partitions": json.dumps(affected_partitions, ensure_ascii=False),
        "rows_before": rows_before, "rows_after": rows_after,
        "diff_summary": json.dumps(diff, ensure_ascii=False),
        "shadow_table": shadow, "view_switched": False,
        "status": "DIFF_READY", "ingest_batch_id": batch_id,
    }
    _write_log(spark, base_log)
    print(f"[backfill] 对比完成 -> {diff_path}；差异 {diff}；"
          f"未加 --switch，生产表未改动")

    if not switch:
        return {**base_log, "diff_path": diff_path}

    if full_rebuild:
        # 独立表重建 + 换表：旧表保留为回滚点，不覆盖任何生产分区
        rebuild = f"ltl.dwd.waybill_event_detail_rebuild_{job_id.replace('-', '_')}"
        spark.sql(f"DROP TABLE IF EXISTS {rebuild}")
        spark.table(shadow).writeTo(rebuild).using("iceberg").create()
        backup = f"ltl.dwd.waybill_event_detail_backup_{job_id.replace('-', '_')}"
        spark.sql(f"DROP TABLE IF EXISTS {backup}")
        # metastore 原子换名（同命名空间内只给表名）；两张表物理文件均保留
        spark.sql(f"ALTER TABLE {DWD_EVENT} RENAME TO {backup.split('.')[-1]}")
        spark.sql(f"ALTER TABLE {rebuild} RENAME TO {DWD_EVENT.split('.')[-1]}")
        final_status = "SWITCHED"
        rollback_hint = backup
    else:
        # 常规补数：只对受影响分区做原子动态覆盖
        replace_affected(spark, detail, affected)
        final_status = "SWITCHED"
        rollback_hint = "Iceberg snapshot rollback: " \
                        f"CALL ltl.system.rollback_to_snapshot('{DWD_EVENT}', <snapshot_id>)"

    spark.sql(f"DROP TABLE IF EXISTS {shadow}")
    _write_log(spark, {**base_log, "view_switched": True,
                       "status": final_status,
                       "shadow_table": rollback_hint})
    print(f"[backfill] 已生效 status={final_status} 回滚点={rollback_hint}")
    return {**base_log, "status": final_status,
            "rollback": rollback_hint, "diff_path": diff_path}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", choices=["WAYBILL", "DATE_RANGE"], required=True)
    ap.add_argument("--waybill-nos", default="",
                    help="逗号分隔运单号，scope=WAYBILL 时必填")
    ap.add_argument("--start-date"), ap.add_argument("--end-date")
    ap.add_argument("--switch", action="store_true",
                    help="对比通过后生效；缺省只产出对比不动生产")
    ap.add_argument("--full-rebuild", action="store_true",
                    help="独立表重建+换表（全量回刷唯一允许路径）")
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_backfill", conf)
    try:
        nos = [x.strip() for x in args.waybill_nos.split(",") if x.strip()] or None
        run_backfill(spark, conf, args.scope, waybill_nos=nos,
                     start_date=args.start_date, end_date=args.end_date,
                     switch=args.switch, full_rebuild=args.full_rebuild)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
