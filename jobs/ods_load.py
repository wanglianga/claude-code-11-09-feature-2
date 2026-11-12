"""ODS 原始接入作业：上游 JSON Lines 落地文件原样入湖。

设计要点：
  * 不清洗、不合并：同一 event_id 的多次推送只要来自不同落地批次就全部保留，
    去重只发生在 DWD 层；raw_payload 保存报文全文。
  * 文件级幂等：同一落地文件重复读取时按 (source_file, event_id) MERGE，
    避免作业重试制造"假重复"；上游真实重推（新文件/新批次）仍会原样保留。
  * 二级分区：biz_date=to_date(event_time)，source_system 原值，补数可只扫受影响分区。
  * 写 ODS 水位线（成功才记 SUCCESS，失败不记）。

用法：
  spark-submit jobs/ods_load.py --biz-date 2026-11-11 [--conf ...]
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
)

from common.spark_utils import ODS_EVENT, WM_LOG, build_spark, load_conf, new_batch_id

# 仅解析入湖所需的公共头字段；其余字段全部留在 raw_payload，不在本层做业务解析
_HEADER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("waybill_no", StringType()),
    StructField("event_type", StringType()),
    StructField("source_system", StringType()),
    StructField("report_time", TimestampType()),
    StructField("event_time", TimestampType()),
])


def load_landing(spark, landing_dir: str, biz_date: str, batch_id: str):
    """读取当日落地文件（wholetext 逐行），报文全文进 raw_payload。"""
    path = os.path.join(landing_dir, f"biz_date={biz_date}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"落地目录不存在: {path}")
    raw = (
        spark.read.text(path, wholetext=False)
        .withColumn("source_file", F.input_file_name())
        .filter(F.trim(F.col("value")) != "")
    )
    parsed = raw.withColumn("h", F.from_json(F.col("value"), _HEADER_SCHEMA))
    df = (
        parsed.select(
            F.col("h.event_id").alias("event_id"),
            F.col("h.waybill_no").alias("waybill_no"),
            F.col("h.event_type").alias("event_type"),
            F.col("h.source_system").alias("source_system"),
            F.col("h.report_time").alias("report_time"),
            F.col("h.event_time").alias("event_time"),
            F.col("value").alias("raw_payload"),
            F.col("source_file"),
            F.lit(batch_id).alias("ingest_batch_id"),
            F.current_timestamp().alias("ingest_time"),
        )
        .withColumn("biz_date", F.to_date(F.col("event_time")))
    )
    missing = df.filter(F.col("event_id").isNull() | F.col("event_time").isNull())
    if missing.limit(1).count() > 0:
        bad = missing.select("source_file").distinct().limit(10).collect()
        raise ValueError(f"落地报文缺少 event_id/event_time，问题文件: {[r.source_file for r in bad]}")
    return df


def merge_into_ods(spark, df) -> int:
    df.createOrReplaceTempView("stg_ods_event")
    spark.sql(f"""
        MERGE INTO {ODS_EVENT} t
        USING stg_ods_event s
        ON t.event_id = s.event_id AND t.source_file = s.source_file
        WHEN NOT MATCHED THEN INSERT *
    """)
    return df.count()


def run(spark, conf: dict, biz_date: str) -> str:
    batch_id = new_batch_id("ods")
    df = load_landing(spark, conf["landing.event.dir"], biz_date, batch_id)
    n = merge_into_ods(spark, df)
    wm = df.agg(F.max("event_time").alias("hwm"), F.count("*").alias("cnt")).collect()[0]
    spark.sql(f"""
        INSERT INTO {WM_LOG}
        VALUES ('ODS', 'ALL', DATE'{biz_date}',
                timestamp'{wm['hwm'].strftime('%Y-%m-%d %H:%M:%S')}',
                {wm['cnt']}, '{batch_id}', current_timestamp(), 'SUCCESS')
    """)
    print(f"[ods_load] biz_date={biz_date} batch={batch_id} events_loaded={n}")
    return batch_id


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--biz-date", required=True)
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_ods_load", conf)
    try:
        run(spark, conf, args.biz_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
