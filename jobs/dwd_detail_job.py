"""DWD 明细作业：ODS -> 运单事件明细（去重 / 乱序排序 / 重放 / 代理键解析）。

核心口径（对应需求）：
  1. 去重：按 event_id 去重，重复上报保留最早入湖的一条
     （ingest_time 最小；并列时以 source_file 兜底确定顺序，保证可复现）。
  2. 乱序：event_seq = ROW_NUMBER() OVER (PARTITION BY waybill_no
            ORDER BY event_time, event_id)，迟到事件到达后整单重放即可重排。
  3. 重放粒度：按"受影响运单"重算该运单的全部事件，
     而不是只插一行——保证序号连续、异常链顺序正确。
  4. 多次异常：EXCEPTION 各自独立成行，不做覆盖/取末次。
  5. 代理键：自然键 -> 拉链维表稳定代理键；查不到记 -1（计入 DQ 维表未匹配率），
     报表时点关联在视图层按 event_date ∈ [eff_date, exp_date) 完成。
  6. 幂等与原子性：动态分区 INSERT OVERWRITE，未受影响运单原样保留，
     受影响分区在单次 Iceberg 提交内完成替换，不存在"删了没插成"的中间态。
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, StringType, StructField, StructType,
)
from pyspark.sql.window import Window

from common.spark_utils import (
    DIM_SPECS, DIM_TABLES, DWD_EVENT, ODS_EVENT, UNMATCHED_SK, WM_LOG,
    build_spark, load_conf, new_batch_id,
)

# raw_payload 中业务字段的统一解析模式（不同事件类型缺字段即得 NULL，不报错）
_PAYLOAD_SCHEMA = StructType([
    StructField("origin_hub_code", StringType()),
    StructField("hub_code", StringType()),
    StructField("next_hub_code", StringType()),
    StructField("route_code", StringType()),
    StructField("customer_code", StringType()),
    StructField("courier_code", StringType()),
    StructField("exception_code", StringType()),
    StructField("exception_reason", StringType()),
    StructField("appointment_window", StringType()),
    StructField("is_cold_chain", BooleanType()),
])


def _affected_waybills(spark, start_date: str | None, end_date: str | None,
                       waybill_nos: list[str] | None):
    """计算受影响运单集合。"""
    if waybill_nos:
        return (spark.table(ODS_EVENT)
                .filter(F.col("waybill_no").isin(waybill_nos))
                .select("waybill_no").distinct())
    q = spark.table(ODS_EVENT)
    if start_date:
        q = q.filter(F.col("biz_date") >= F.lit(start_date))
    if end_date:
        q = q.filter(F.col("biz_date") <= F.lit(end_date))
    return q.select("waybill_no").distinct()


def _dedup_ods(spark):
    """按 event_id 去重，保留最早入湖记录。"""
    w = Window.partitionBy("event_id").orderBy(
        F.col("ingest_time").asc(), F.col("source_file").asc())
    return (
        spark.table(ODS_EVENT)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_detail(spark, start_date=None, end_date=None,
                 waybill_nos=None, batch_id=None) -> tuple:
    """重算受影响运单的全部明细，返回 (明细DataFrame, 受影响运单DataFrame, 统计dict)。"""
    affected = _affected_waybills(spark, start_date, end_date, waybill_nos)
    affected.cache()
    if affected.count() == 0:
        return None, affected, {"waybills": 0, "rows": 0, "dup_rows": 0,
                                "unmatched": {k: 0 for k in DIM_SPECS}}

    deduped_all = _dedup_ods(spark)
    dup_rows = (
        spark.table(ODS_EVENT).groupBy("event_id").count()
        .filter(F.col("count") > 1)
        .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("d"))
        .collect()[0]["d"]
    )

    detail = (
        deduped_all.alias("o")
        .join(affected.alias("a"), on="waybill_no", how="inner")
        .withColumn("p", F.from_json(F.col("raw_payload"), _PAYLOAD_SCHEMA))
        .select(
            F.col("waybill_no"),
            F.col("event_id"),
            F.col("event_type"),
            F.col("event_time"),
            F.to_date(F.col("event_time")).alias("event_date"),
            F.col("report_time"),
            F.col("source_system"),
            F.col("p.origin_hub_code").alias("origin_hub_code"),
            # 报文未显式给 hub_code 时回退到发件网点，保证维表关联有自然键可用
            F.coalesce(F.col("p.hub_code"),
                       F.col("p.origin_hub_code")).alias("hub_code"),
            F.col("p.next_hub_code").alias("next_hub_code"),
            F.col("p.route_code").alias("waybill_route_code"),
            F.col("p.customer_code").alias("customer_code"),
            F.col("p.courier_code").alias("courier_code"),
            F.col("p.exception_code").alias("exception_code"),
            F.col("p.exception_reason").alias("exception_reason"),
            F.coalesce(F.col("p.appointment_window"), F.lit("NA")).alias("appointment_window"),
            F.coalesce(F.col("p.is_cold_chain"), F.lit(False)).alias("is_cold_chain"),
        )
    )

    # 代理键解析（四张维表），未匹配 -> -1（映射列加别名避免与明细列冲突）
    unmatched = {}
    for dim_name, spec in DIM_SPECS.items():
        nk, fact_nk, sk = spec["natural_key"], spec["fact_nk"], spec["sk"]
        mapping = (
            spark.table(DIM_TABLES[dim_name])
            .select(F.col(nk).alias("_m_nk"), F.col(sk).alias("_m_sk"))
            .distinct()
        )
        detail = (
            detail.join(mapping, F.col(fact_nk) == F.col("_m_nk"), "left")
            .withColumn(sk, F.coalesce(F.col("_m_sk"), F.lit(UNMATCHED_SK)))
            .drop("_m_nk", "_m_sk")
        )
        unmatched[dim_name] = (
            detail.filter(F.col(fact_nk).isNotNull()
                          & (F.col(sk) == UNMATCHED_SK))
            .count()
        )

    # 乱序排序生成事件序号（业务发生时间为准，event_id 兜底保证确定性）
    w_seq = Window.partitionBy("waybill_no").orderBy(
        F.col("event_time").asc(), F.col("event_id").asc())
    detail = (
        detail.withColumn("event_seq", F.row_number().over(w_seq))
        .withColumn("ingest_batch_id", F.lit(batch_id))
        .withColumn("ingest_time", F.current_timestamp())
    )

    # Schema 演进兜底：目标表新增列（只加列）若本作业尚未产出，按默认值补齐，
    # 保证加列后既有作业不中断；已知演进列给业务默认值，其余可空列给 NULL。
    defaults = {
        "appointment_window": F.lit("NA"),
        "is_cold_chain": F.lit(False),
        "station_code": F.lit("NA"),
        "weight_kg_v2": F.lit(-1.0),
        "evol_test_col": F.lit("NA"),
    }
    target_cols = spark.table(DWD_EVENT).columns
    for c in target_cols:
        if c not in detail.columns:
            detail = detail.withColumn(c, defaults.get(c, F.lit(None)))
    detail = detail.select(*target_cols)
    detail.cache()
    stats = {"waybills": affected.count(), "rows": detail.count(),
             "dup_rows": dup_rows, "unmatched": unmatched}
    return detail, affected, stats


def replace_affected(spark, detail, affected, target: str = DWD_EVENT) -> None:
    """受影响分区整体覆盖，无关分区物理不动（单事务原子提交）。

    步骤：
      1. 取受影响运单"新明细"与"现存旧明细"共同涉及的分区集合；
      2. 这些分区内剔除受影响运单的旧行，保留同分区其他运单；
      3. 拼上重算明细后 INSERT OVERWRITE——Iceberg 动态覆盖只替换
         数据中出现的分区，因此未受影响分区不会被重写，天然不整表重算。
    """
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    new_parts = detail.select("event_date", "source_system").distinct()
    old_parts = (
        spark.table(target).alias("t")
        .join(affected.alias("a"), on="waybill_no", how="inner")
        .select(F.col("event_date"), F.col("source_system")).distinct()
    )
    parts = new_parts.union(old_parts).distinct()

    rows_in_parts = (
        spark.table(target).alias("t")
        .join(parts.alias("p"),
              (F.col("t.event_date") == F.col("p.event_date"))
              & (F.col("t.source_system") == F.col("p.source_system")),
              how="left_semi")
    )
    kept = rows_in_parts.join(affected, on="waybill_no", how="left_anti")
    rebuilt_part = kept.unionByName(detail)
    rebuilt_part.createOrReplaceTempView("tmp_dwd_rebuilt_part")
    spark.sql(
        f"INSERT OVERWRITE TABLE {target} SELECT * FROM tmp_dwd_rebuilt_part")


def run_incremental(spark, conf: dict, biz_date: str) -> dict:
    batch_id = new_batch_id("dwd")
    detail, affected, stats = build_detail(
        spark, start_date=biz_date, end_date=biz_date, batch_id=batch_id)
    if detail is None:
        print(f"[dwd] biz_date={biz_date} 无受影响运单，跳过")
        return stats
    replace_affected(spark, detail, affected)
    hwm = (spark.table(ODS_EVENT)
           .filter(F.col("biz_date") == biz_date)
           .agg(F.max("event_time").alias("hwm"),
                F.count("*").alias("cnt")).collect()[0])
    spark.sql(f"""
        INSERT INTO {WM_LOG}
        VALUES ('DWD', 'ALL', DATE'{biz_date}',
                timestamp'{hwm['hwm'].strftime('%Y-%m-%d %H:%M:%S')}',
                {hwm['cnt']}, '{batch_id}', current_timestamp(), 'SUCCESS')
    """)
    print(f"[dwd] biz_date={biz_date} batch={batch_id} stats={stats}")
    return stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--biz-date", required=True)
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_dwd_detail", conf)
    try:
        run_incremental(spark, conf, args.biz_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
