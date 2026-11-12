"""旧宽表迁移作业冒烟验证（独立单号 WB8xx，不影响 test_pipeline 的断言）。

直接 spark-submit 运行：
  spark-submit tests/smoke_legacy_migration.py --conf conf/pipeline.local.conf
"""
from __future__ import annotations

import argparse
from datetime import date, datetime

from pyspark.sql.types import DateType, TimestampType

from common.spark_utils import DWD_EVENT, ODS_EVENT, build_spark, load_conf
from legacy_migrate_job import LEGACY_TABLE, run as legacy_run

BIZ = "2026-11-06"

# 与 ods.legacy_waybill_wide 列序一致的原始值（时间列为字符串/None）
_RAW_ROWS = [
    ("WB801", "K002", "R001",
     "2026-11-06 08:00:00", "2026-11-06 10:00:00", "2026-11-06 16:00:00",
     None, None, None,
     "2026-11-06 18:00:00", "2026-11-06 20:00:00", None, None,
     None, None, "HUB_SH", "上海华东分拨中心", "C001", "NA", "false"),
    ("WB802", "K001", "R001",
     "2026-11-06 09:00:00", None, None,
     None, None, None,
     None, "2026-11-06 15:00:00", None, None,
     "2026-11-06 12:00:00", "旧系统末次异常",
     "HUB_BJ", "北京一集配站", "C002", "NA", "true"),
]


def seed_legacy(spark) -> None:
    schema = spark.table(LEGACY_TABLE).schema
    typed_rows = []
    for raw in _RAW_ROWS:
        vals = list(raw) + [BIZ]  # 末列 biz_date
        typed = []
        for field, val in zip(schema.fields, vals):
            if val is None:
                typed.append(None)
            elif isinstance(field.dataType, TimestampType):
                typed.append(datetime.strptime(val, "%Y-%m-%d %H:%M:%S"))
            elif isinstance(field.dataType, DateType):
                typed.append(date.fromisoformat(val))
            else:
                typed.append(val)
        typed_rows.append(tuple(typed))
    spark.createDataFrame(typed_rows, schema).writeTo(LEGACY_TABLE).append()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_smoke_legacy", conf)
    try:
        seed_legacy(spark)
        legacy_run(spark, conf, BIZ, BIZ)

        # ODS 逆透视：WB801 有5个非空时间列 -> 5 条 LEGACY 事件
        n801 = (spark.table(ODS_EVENT)
                .where("waybill_no='WB801' AND source_system='LEGACY'").count())
        assert n801 == 5, f"WB801 应逆透视5条事件，实际 {n801}"

        # WB802 仅 PICKUP/SIGN/EXCEPTION 三个非空 -> 3 条，末次异常原因保留
        n802 = (spark.table(ODS_EVENT)
                .where("waybill_no='WB802' AND source_system='LEGACY'").count())
        assert n802 == 3, f"WB802 应逆透视3条事件，实际 {n802}"

        # DWD 复用统一重放：序号按 event_time 连续，可被时点视图查询
        d801 = (spark.table(DWD_EVENT).where("waybill_no='WB801'")
                .orderBy("event_seq").collect())
        assert [r.event_seq for r in d801] == list(range(1, 6))
        assert d801[0].event_type == "PICKUP" and d801[-1].event_type == "SIGN"

        # 重跑幂等：再跑一次迁移，LEGACY 事件数不翻倍
        legacy_run(spark, conf, BIZ, BIZ)
        n801_again = (spark.table(ODS_EVENT)
                      .where("waybill_no='WB801' AND source_system='LEGACY'").count())
        assert n801_again == 5, f"迁移重跑必须幂等，实际 {n801_again}"

        print("[smoke_legacy] 旧宽表迁移验证通过：逆透视、统一重放、幂等均正确")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
