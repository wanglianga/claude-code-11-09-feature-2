"""冲突ID查询助手：按运单号（可选冲突字段/节点）打印 OPEN 冲突ID。

供本地脚本与运维查询使用；正式人工裁决也可直接从
app.v_waybill_conflict_open 视图复制 conflict_id。

用法：
  spark-submit jobs/print_conflict_id.py --waybill-no WB010
  spark-submit jobs/print_conflict_id.py --waybill-no WB010 --conflict-field EVENT_TIME
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from common.spark_utils import CONFLICT_TABLE, build_spark, load_conf


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--waybill-no", required=True)
    ap.add_argument("--conflict-field", default=None,
                    choices=["EVENT_TIME", "EVENT_STATUS"])
    ap.add_argument("--event-type", default=None)
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_conflict_id", conf)
    try:
        q = (spark.table(CONFLICT_TABLE)
             .filter(F.col("waybill_no") == args.waybill_no))
        if args.conflict_field:
            q = q.filter(F.col("conflict_field") == args.conflict_field)
        if args.event_type:
            q = q.filter(F.col("event_type") == args.event_type)
        rows = q.orderBy("conflict_field").collect()
        if not rows:
            raise SystemExit(
                f"运单 {args.waybill_no} 没有匹配的冲突条目")
        for r in rows:
            print(r.conflict_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
