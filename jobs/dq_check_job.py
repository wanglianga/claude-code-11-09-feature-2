"""数据质量日检作业：按天 × 来源系统 × 分拨中心产出五类核对指标。

覆盖指标（对应需求）：
  EVENT_MISSING    事件缺失率：关键节点序列缺口（PICKUP 后无 SIGN/REJECT 视为链路未闭环，
                   或关键节点中 ARRIVE 缺配对 DEPART 等），分母=应到节点数，分子=缺口数。
  OUT_OF_ORDER     乱序率：同运单内 event_time 早于 report 顺序期望的事件占比，
                   口径=存在 event_time 与上报顺序倒置的事件数 / 总事件数。
  DUPLICATE        重复率：ODS 中同 event_id 多条（重复上报）占比。
  DIM_UNMATCHED    维表未匹配率：DWD 自然键非空但代理键=-1 的事件占比，按四张维表分别记。
  PARTITION_LATENCY 分区产出时延（分钟）：biz_date 当日 23:59 到该分区首条入湖的间隔，
                   或按作业运行时刻衡量分区就绪时延；超阈值即告警。

异常可定位：结果表带 source_system 与 hub_code 两个下钻维度（ALL 为汇总行）。
用法：
  spark-submit jobs/dq_check_job.py --biz-date 2026-11-11
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.spark_utils import (
    DIM_SPECS, DQ_TABLE, DWD_EVENT, ODS_EVENT,
    build_spark, load_conf, new_batch_id,
)

METRICS = ["EVENT_MISSING", "OUT_OF_ORDER", "DUPLICATE",
           "DIM_UNMATCHED", "PARTITION_LATENCY"]
# 一条完整运单应覆盖的关键节点（链路闭环检查）
KEY_NODES = ["PICKUP", "DEPART", "ARRIVE", "DELIVER"]
TERMINAL_NODES = ["SIGN", "REJECT"]


def _emit(spark, rows: list[tuple], biz_date: str, batch_id: str) -> None:
    """rows: (source_system, hub_code, metric_code, value, total, bad, threshold, breach)

    同日重跑幂等：先清除该 biz_date 的旧结果再写入，避免重复计数。
    """
    spark.sql(f"DELETE FROM {DQ_TABLE} WHERE biz_date = DATE'{biz_date}'")
    df = spark.createDataFrame(rows, schema=(
        "source_system string, hub_code string, metric_code string, "
        "metric_value double, metric_total bigint, metric_bad bigint, "
        "threshold_value double, is_breach boolean"))
    (df.withColumn("biz_date", F.to_date(F.lit(biz_date)))
       .withColumn("ingest_batch_id", F.lit(batch_id))
       .withColumn("ingest_time", F.current_timestamp())
       .select("biz_date", "source_system", "hub_code", "metric_code",
               "metric_value", "metric_total", "metric_bad",
               "threshold_value", "is_breach", "ingest_batch_id", "ingest_time")
       .writeTo(DQ_TABLE).append())


def _rate(bad: int, total: int) -> float:
    return 0.0 if total == 0 else round(bad / total, 6)


def run_dq(spark, conf: dict, biz_date: str) -> dict:
    batch_id = new_batch_id("dq")
    th = {
        "EVENT_MISSING": float(conf["dq.threshold.event_missing_rate"]),
        "OUT_OF_ORDER": float(conf["dq.threshold.out_of_order_rate"]),
        "DUPLICATE": float(conf["dq.threshold.duplicate_rate"]),
        "DIM_UNMATCHED": float(conf["dq.threshold.dim_unmatched_rate"]),
        "PARTITION_LATENCY": float(conf["dq.threshold.partition_latency_minutes"]),
    }
    rows: list[tuple] = []

    def add_metric(ss, hub, code, value, total, bad):
        base_code = "DIM_UNMATCHED" if code.startswith("DIM_UNMATCHED_") else code
        threshold = th[base_code]
        breach = value > threshold
        rows.append((ss, hub, code, float(value), int(total), int(bad),
                     float(threshold), bool(breach)))

    ods = spark.table(ODS_EVENT).filter(F.col("biz_date") == biz_date)
    dwd = spark.table(DWD_EVENT).filter(F.col("event_date") == biz_date)

    # 下钻维度组合：全网 ALL/ALL + 各来源系统 + 来源系统×分拨
    dims = [("ALL", "ALL")]
    for r in ods.select("source_system").distinct().collect():
        dims.append((r.source_system, "ALL"))
    for r in dwd.select("source_system", "hub_code").distinct().collect():
        if r.hub_code:
            dims.append((r.source_system, r.hub_code))

    for ss, hub in dims:
        o = ods if ss == "ALL" else ods.filter(F.col("source_system") == ss)
        d = dwd if ss == "ALL" else dwd.filter(F.col("source_system") == ss)
        if hub != "ALL":
            d = d.filter(F.col("hub_code") == hub)

        total = o.count()

        # 1) 重复率：ODS 同 event_id 多条
        dup_bad = (o.groupBy("event_id").count()
                   .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)))
                   .collect()[0][0])
        add_metric(ss, hub, "DUPLICATE", _rate(dup_bad, total), total, dup_bad)

        # 2) 乱序率：按 report_time 排序的序号与按 event_time 排序的序号不一致
        w_r = Window.partitionBy("waybill_no").orderBy(
            F.col("report_time").asc(), F.col("event_id").asc())
        w_e = Window.partitionBy("waybill_no").orderBy(
            F.col("event_time").asc(), F.col("event_id").asc())
        seq = (d.withColumn("rn_report", F.row_number().over(w_r))
                 .withColumn("rn_event", F.row_number().over(w_e)))
        ooo_bad = seq.filter(F.col("rn_report") != F.col("rn_event")).count()
        dwd_total = d.count()
        add_metric(ss, hub, "OUT_OF_ORDER", _rate(ooo_bad, dwd_total),
                   dwd_total, ooo_bad)

        # 3) 事件缺失率：运单关键节点缺口
        present = (d.groupBy("waybill_no")
                   .agg(F.collect_set("event_type").alias("ets")))
        wb_total = present.count()
        missing_cnt = 0
        for r in present.collect():
            ets = set(r.ets)
            # 缺任一关键节点，或有关键节点但无签收/拒收闭环
            miss = sum(1 for n in KEY_NODES if n not in ets)
            if not (set(TERMINAL_NODES) & ets):
                miss += 1
            missing_cnt += miss
        expected = wb_total * (len(KEY_NODES) + 1)
        add_metric(ss, hub, "EVENT_MISSING", _rate(missing_cnt, expected),
                   expected, missing_cnt)

        # 4) 维表未匹配率（四张维表分别评价，hub 维度行只评网点维表）
        for dim_name, spec in DIM_SPECS.items():
            if hub != "ALL" and dim_name != "hub":
                continue
            fact_nk, sk = spec["fact_nk"], spec["sk"]
            denom = d.filter(F.col(fact_nk).isNotNull()).count()
            bad = d.filter(F.col(fact_nk).isNotNull()
                           & (F.col(sk) == -1)).count()
            add_metric(ss, hub, f"DIM_UNMATCHED_{dim_name.upper()}",
                       _rate(bad, denom), denom, bad)

        # 5) 分区产出时延：分区最早入湖时间相对 biz_date 当日 23:59 的延迟分钟
        first_ingest = o.agg(F.min("ingest_time")).collect()[0][0]
        if first_ingest is not None:
            deadline = datetime.strptime(biz_date, "%Y-%m-%d") + timedelta(hours=23, minutes=59)
            latency = max(0.0, (first_ingest.replace(tzinfo=None) - deadline).total_seconds() / 60.0)
            add_metric(ss, hub, "PARTITION_LATENCY", latency, 1,
                       1 if latency > th["PARTITION_LATENCY"] else 0)

    _emit(spark, rows, biz_date, batch_id)
    breaches = [r for r in rows if r[7]]
    result = {"biz_date": biz_date, "metric_rows": len(rows),
              "breach_count": len(breaches),
              "breaches": [
                  {"source_system": r[0], "hub_code": r[1], "metric": r[2],
                   "value": r[3], "threshold": r[6]} for r in breaches]}
    print("[dq] " + json.dumps(result, ensure_ascii=False))
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--biz-date", required=True)
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_dq", conf)
    try:
        run_dq(spark, conf, args.biz_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
