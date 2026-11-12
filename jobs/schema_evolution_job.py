"""Schema 演进作业：只加列 + 历史分区回填默认值（幂等）。

为什么需要它：
  Spark 3.5 的 ADD COLUMN 不支持 IF NOT EXISTS，Iceberg 1.8 也不支持 SQL 列 DEFAULT。
  本作业把"带默认值加列"落成两步——
    1) 存在性检查后 ALTER TABLE ... ADD COLUMN（只加列、不改类型、不删列）
    2) UPDATE ... SET col=<default> WHERE col IS NULL（历史行回填，可重复执行）
  新数据的默认值由各写入作业在 select 中 coalesce 保证，因此全表最终无 NULL。

内置清单与 sql/07_evolution.sql 一一对应；也支持 --table/--column 临时加列。

用法：
  spark-submit jobs/schema_evolution_job.py --apply-builtin
  spark-submit jobs/schema_evolution_job.py \
      --table ltl.dwd.waybill_event_detail --column evol_test_col \
      --col-type STRING --default "'NA'"
"""
from __future__ import annotations

import argparse

from common.spark_utils import (
    DIM_TABLES, DWD_EVENT, add_column_if_absent, build_spark, load_conf,
)

# (表, 列, 类型, 默认值 SQL 字面量, 注释)
BUILTIN_COLUMNS = [
    (DWD_EVENT, "appointment_window", "STRING", "'NA'",
     "预约送达时段；历史数据回填NA"),
    (DWD_EVENT, "is_cold_chain", "BOOLEAN", "false",
     "是否冷链；历史数据回填false"),
    (DWD_EVENT, "station_code", "STRING", "'NA'",
     "驿站网点编码（STATION_IN/STATION_OUT 节点使用）"),
    (DWD_EVENT, "weight_kg_v2", "DECIMAL(12,3)", "CAST(-1 AS DECIMAL(12,3))",
     "重量高精度版；旧 weight_kg 保留不删，视图切换到本列"),
    (DIM_TABLES["hub"], "cold_storage_capable", "BOOLEAN", "false",
     "是否具备冷链仓储能力；历史回填false"),
]


def add_column_with_default(spark, table: str, column: str,
                            col_type: str, default_sql: str,
                            comment: str | None = None) -> None:
    added = add_column_if_absent(spark, table, column, col_type, comment)
    if not added:
        print(f"[schema_evolution] {table}.{column} 已存在，跳过加列")
    # 历史回填：仅更新 NULL 行，幂等；Iceberg COW 下为重写受影响文件
    spark.sql(
        f"UPDATE {table} SET {column} = {default_sql} "
        f"WHERE {column} IS NULL")
    print(f"[schema_evolution] {table}.{column} ({col_type}) "
          f"已确保存在并回填默认值 {default_sql}")


def run(spark, apply_builtin: bool, table=None, column=None,
        col_type=None, default_sql=None) -> None:
    if apply_builtin:
        for tbl, col, ctype, dft, comment in BUILTIN_COLUMNS:
            add_column_with_default(spark, tbl, col, ctype, dft, comment)
    else:
        missing = [n for n, v in
                   [("table", table), ("column", column),
                    ("col-type", col_type), ("default", default_sql)] if not v]
        if missing:
            raise ValueError(f"自定义加列缺少参数: {missing}")
        add_column_with_default(spark, table, column, col_type, default_sql)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply-builtin", action="store_true")
    ap.add_argument("--table"), ap.add_argument("--column"),
    ap.add_argument("--col-type"), ap.add_argument("--default")
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_schema_evolution", conf)
    try:
        run(spark, args.apply_builtin, args.table, args.column,
            args.col_type, args.default)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
