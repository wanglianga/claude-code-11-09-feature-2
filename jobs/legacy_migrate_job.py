"""旧宽表一次性迁移作业：宽表时间列逆透视为 ODS 事件。

口径与边界：
  * 每个非空时间列 -> 一条事件，event_id 取确定性值 LEGACY_<waybill_no>_<EVENT_TYPE>，
    保证重复执行幂等（ODS MERGE 按 event_id+source_file 去重）。
  * source_system 固定 LEGACY，raw_payload 记录来源宽表列名，可血缘追溯。
  * 宽表只存"最后一次异常"，因此历史异常链无法还原（文档明确的能力边界）；
    迁移完成后新产生的 EXCEPTION 事件按发生顺序完整保留。
  * 事件入 ODS 后，复用 dwd_detail_job 的同一套重放逻辑生成序号与代理键，
    不另写明细转换，口径唯一。

用法：
  spark-submit jobs/legacy_migrate_job.py --start-date 2023-01-01 --end-date 2026-11-10
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from common.spark_utils import ODS_EVENT, build_spark, load_conf, new_batch_id
from dwd_detail_job import build_detail, replace_affected

LEGACY_TABLE = "ltl.ods.legacy_waybill_wide"

# 宽表时间列 -> 事件类型
TIME_COLUMN_MAP = [
    ("pickup_time", "PICKUP"),
    ("line_haul_depart_time", "DEPART"),
    ("arrive_hub_time", "ARRIVE"),
    ("transfer_time_1", "TRANSFER"),
    ("transfer_time_2", "TRANSFER"),
    ("transfer_time_3", "TRANSFER"),
    ("deliver_time", "DELIVER"),
    ("sign_time", "SIGN"),
    ("reject_time", "REJECT"),
    ("pod_time", "POD"),
    ("last_exception_time", "EXCEPTION"),
]


def unpivot(spark, start_date: str, end_date: str, batch_id: str):
    wide = (spark.table(LEGACY_TABLE)
            .filter((F.col("biz_date") >= start_date)
                    & (F.col("biz_date") <= end_date)))
    events = None
    for col_name, event_type in TIME_COLUMN_MAP:
        part = (wide.filter(F.col(col_name).isNotNull())
                .select(
                    F.concat(F.lit("LEGACY_"), F.col("waybill_no"),
                             F.lit("_"), F.lit(event_type),
                             # 三个中转列同类型，用列序号区分 event_id
                             F.lit("_" + col_name[-1]
                                   if col_name.startswith("transfer_time_") else ""))
                    .alias("event_id"),
                    F.col("waybill_no"),
                    F.lit(event_type).alias("event_type"),
                    F.lit("LEGACY").alias("source_system"),
                    F.col(col_name).alias("report_time"),
                    F.col(col_name).alias("event_time"),
                    F.to_json(F.struct(
                        F.col("origin_hub_code"),
                        F.col("last_hub_name").alias("hub_name_latest_only"),
                        F.col("courier_code"),
                        F.col("customer_code"),
                        F.col("route_code"),
                        F.col("appointment_window"),
                        F.col("is_cold_chain"),
                        F.when(F.lit(event_type) == "EXCEPTION",
                               F.col("last_exception_reason"))
                         .otherwise(F.lit(None)).alias("exception_reason"),
                        F.lit(col_name).alias("legacy_source_column"))
                    ).alias("raw_payload"),
                    F.lit(f"legacy://{col_name}").alias("source_file"),
                    F.lit(batch_id).alias("ingest_batch_id"),
                    F.current_timestamp().alias("ingest_time"),
                    F.to_date(F.col(col_name)).alias("biz_date")))
        events = part if events is None else events.unionByName(part)
    return events


def run(spark, conf: dict, start_date: str, end_date: str) -> None:
    batch_id = new_batch_id("legm")
    events = unpivot(spark, start_date, end_date, batch_id)
    n = events.count()
    if n == 0:
        print("[legacy_migrate] 范围内无宽表数据")
        return
    events.createOrReplaceTempView("tmp_legacy_events")
    spark.sql(f"""
        MERGE INTO {ODS_EVENT} t
        USING tmp_legacy_events s
        ON t.event_id = s.event_id AND t.source_file = s.source_file
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"[legacy_migrate] 逆透视事件 {n} 条已入 ODS（source_system=LEGACY）")

    # 复用 DWD 重放：按区间重放受影响运单，口径与新数据完全一致
    detail, affected, stats = build_detail(
        spark, start_date=start_date, end_date=end_date, batch_id=batch_id)
    if detail is not None:
        replace_affected(spark, detail, affected)
    print(f"[legacy_migrate] DWD 重放完成 stats={stats}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_legacy_migrate", conf)
    try:
        run(spark, conf, args.start_date, args.end_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
