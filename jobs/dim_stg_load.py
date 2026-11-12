"""维表落地作业：主数据 JSON Lines 每日快照写入 dim_stg.*_stg。

只做类型转换与快照分区落地，不做历史判断；拉链闭合由 dim_scd2_job 完成。
按 snapshot_date 动态分区覆盖，同一天重复执行幂等。

用法：
  spark-submit jobs/dim_stg_load.py --snapshot-date 2026-11-11 [--dim-name hub]
落地目录约定：
  ${landing.dim.dir}/biz_date=YYYY-MM-DD/<dim_name>.jsonl
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F

from common.spark_utils import (
    STG_TABLES, build_spark, load_conf, new_batch_id,
)

# 各 stg 表的业务列（与 03_dim_stg.sql 对齐，snapshot_date 由分区补齐）
STG_COLUMNS = {
    "hub": ["hub_code", "hub_name", "hub_type", "region_code",
            "parent_hub_code", "status", "merged_into"],
    "courier": ["courier_code", "courier_name", "hub_code",
                "service_area", "status"],
    "route": ["route_code", "route_name", "origin_hub_code",
              "dest_hub_code", "transit_hubs", "status"],
    "customer": ["customer_code", "customer_name", "customer_level",
                 "sales_owner", "status"],
}


def load_one(spark, conf: dict, dim_name: str, snapshot_date: str,
             batch_id: str) -> int:
    path = os.path.join(conf["landing.dim.dir"],
                        f"biz_date={snapshot_date}", f"{dim_name}.jsonl")
    if not os.path.exists(path):
        print(f"[dim_stg] 跳过 {dim_name}: 文件不存在 {path}")
        return 0
    src = spark.read.json(path)
    cols = STG_COLUMNS[dim_name]
    df = (
        src.select(*[F.col(c) for c in cols if c in src.columns])
        .withColumn("snapshot_date", F.to_date(F.lit(snapshot_date)))
        .withColumn("ingest_batch_id", F.lit(batch_id))
        .withColumn("ingest_time", F.current_timestamp())
    )
    # 对齐目标表列序，缺列补 null
    target_cols = spark.table(STG_TABLES[dim_name]).columns
    for c in target_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    df = df.select(*target_cols)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.createOrReplaceTempView(f"tmp_stg_{dim_name}")
    spark.sql(
        f"INSERT OVERWRITE TABLE {STG_TABLES[dim_name]} "
        f"SELECT * FROM tmp_stg_{dim_name}")
    n = df.count()
    print(f"[dim_stg] {dim_name} snapshot={snapshot_date} rows={n}")
    return n


def run(spark, conf: dict, snapshot_date: str, dim_name: str = "all") -> None:
    batch_id = new_batch_id("dimstg")
    names = list(STG_COLUMNS) if dim_name == "all" else [dim_name]
    for name in names:
        load_one(spark, conf, name, snapshot_date, batch_id)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot-date", required=True)
    ap.add_argument("--dim-name", default="all",
                    choices=["hub", "courier", "route", "customer", "all"])
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_dim_stg", conf)
    try:
        run(spark, conf, args.snapshot_date, args.dim_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
