"""DDL 初始化作业：按编号顺序执行 sql/*.sql，支持一键建仓与补列。

用法：
  spark-submit jobs/ddl_init.py --conf conf/pipeline.conf
幂等：所有 DDL 均为 CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS /
CREATE OR REPLACE VIEW，可重复执行。
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

from common.spark_utils import build_spark, load_conf

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
# 视图脚本依赖表已存在；演进脚本最后执行（只加列）
SQL_ORDER = [
    "00_setup.sql",
    "01_ods.sql",
    "02_dwd.sql",
    "03_dim_stg.sql",
    "04_dim.sql",
    "05_meta.sql",
    "06_app_views.sql",
    "08_legacy_source.sql",
]
# 07_evolution.sql 是演进参考脚本，不在建仓时自动执行；
# 实际加列与历史回填由 jobs/schema_evolution_job.py 幂等完成。


def split_statements(sql_text: str) -> list[str]:
    """按分号切语句，去掉 -- 注释行与空语句。"""
    cleaned_lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        cleaned_lines.append(line)
    text = "\n".join(cleaned_lines)
    return [s.strip() for s in text.split(";") if s.strip()]


def _execute_view_safe(spark, stmt: str) -> None:
    """Iceberg 1.6 HadoopCatalog 支持 create/drop view 但不支持 replace view，
    因此把 CREATE OR REPLACE VIEW 拆成 DROP VIEW IF EXISTS + CREATE VIEW。"""
    m = re.match(r"\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+([^\s]+)\s+AS\s+(.*)",
                 stmt, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        spark.sql(stmt)
        return
    name, body = m.group(1), m.group(2)
    spark.sql(f"DROP VIEW IF EXISTS {name}")
    spark.sql(f"CREATE VIEW {name} AS {body}")


def run_ddl(spark, sql_dir: str | Path = SQL_DIR) -> None:
    sql_dir = Path(sql_dir)
    for name in SQL_ORDER:
        path = sql_dir / name
        if not path.exists():
            continue
        statements = split_statements(path.read_text(encoding="utf-8"))
        for stmt in statements:
            # 文档型"禁止项"等纯注释已在切句前剔除；防御性跳过以 -- 开头的残留
            if stmt.lstrip().startswith("--"):
                continue
            if re.match(r"\s*CREATE\s+OR\s+REPLACE\s+VIEW", stmt,
                        flags=re.IGNORECASE):
                _execute_view_safe(spark, stmt)
            else:
                spark.sql(stmt)
        print(f"[ddl_init] executed {name} ({len(statements)} statements)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_ddl_init", conf)
    try:
        run_ddl(spark)
        print("[ddl_init] schema ready")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
