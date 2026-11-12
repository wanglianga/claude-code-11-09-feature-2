"""口径版本发布作业：把 DRAFT 裁决规则打包发布为下一个口径版本。

不可变约束（对应需求）：
  * 人工裁决沉淀的规则始终先以 DRAFT 存在，对 reconcile 作业不可见；
  * 本作业创建新的 PUBLISHED 口径版本，把规则 rule_status 置 PUBLISHED 并回填
    effective_calibre_version，规则只对新版本下的后续作业生效；
  * 历史版本号、历史规则绑定关系与已发布的时效统计行均不修改、不重算，
    因此"人工裁决结果沉淀为规则"绝不影响已经发布的报表。

版本号规则：v<YYYY>.<M>.<n>，默认按已发布版本序号自增，也可用 --version 显式指定。

用法：
  # 发布当前全部 DRAFT 规则为新版本
  spark-submit jobs/calibre_job.py --effective-date 2026-11-10 \
      --by data_governance --summary "发布客服补录滞后优先采信扫描枪规则"
  # 只发布指定规则
  spark-submit jobs/calibre_job.py --rule-ids RULE_0001,RULE_0002 --by data_governance
"""
from __future__ import annotations

import argparse
import json
from datetime import date

from pyspark.sql import functions as F

from common.spark_utils import (
    CALIBRE_TABLE, RULE_TABLE, build_spark, load_conf, new_batch_id,
)


def _next_version(spark, effective_date: str) -> str:
    rows = (spark.table(CALIBRE_TABLE)
            .filter(F.col("version_status") == "PUBLISHED")
            .select("calibre_version").collect())
    year, month = effective_date[:4], effective_date[5:7]
    prefix = f"v{year}.{int(month)}."
    seq = [int(r.calibre_version[len(prefix):]) for r in rows
           if r.calibre_version.startswith(prefix)
           and r.calibre_version[len(prefix):].isdigit()]
    return f"{prefix}{(max(seq) + 1) if seq else 0}"


def publish_version(spark, effective_date: str, by: str, summary: str,
                    rule_ids: list[str] | None,
                    explicit_version: str | None = None) -> dict:
    batch_id = new_batch_id("cal")
    draft_q = spark.table(RULE_TABLE).filter(F.col("rule_status") == "DRAFT")
    if rule_ids:
        draft_q = draft_q.filter(F.col("rule_id").isin(rule_ids))
    drafts = draft_q.collect()
    missing = sorted(set(rule_ids or []) - {r.rule_id for r in drafts})
    if missing:
        raise ValueError(f"规则不存在或不是 DRAFT 状态，无法发布: {missing}")

    parent = (spark.table(CALIBRE_TABLE)
              .filter(F.col("version_status") == "PUBLISHED")
              .orderBy(F.col("published_time").desc())
              .select("calibre_version").first())
    parent_version = parent.calibre_version if parent else None
    version = explicit_version or _next_version(spark, effective_date)
    if spark.table(CALIBRE_TABLE).filter(
            F.col("calibre_version") == version).count() > 0:
        raise ValueError(f"口径版本已存在: {version}")

    published_ids = [r.rule_id for r in drafts]
    spark.sql(f"""
        INSERT INTO {CALIBRE_TABLE}
        VALUES (
            '{version}', 'PUBLISHED', DATE'{effective_date}',
            {("'" + parent_version + "'") if parent_version else "NULL"},
            '{json.dumps(published_ids, ensure_ascii=False).replace("'", "''")}',
            '{summary.replace("'", "''")}',
            '{by}', current_timestamp(), current_timestamp()
        )
    """)

    if published_ids:
        id_list = ",".join("'" + i + "'" for i in published_ids)
        spark.sql(f"""
            UPDATE {RULE_TABLE}
            SET rule_status = 'PUBLISHED',
                effective_calibre_version = '{version}',
                published_time = current_timestamp()
            WHERE rule_status = 'DRAFT' AND rule_id IN ({id_list})
        """)

    result = {"calibre_version": version, "parent_version": parent_version,
              "published_rules": published_ids, "batch_id": batch_id,
              "effective_date": effective_date}
    print("[calibre] " + json.dumps(result, ensure_ascii=False))
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--effective-date", default=date.today().isoformat(),
                    help="新版本生效日期，默认今天")
    ap.add_argument("--version", default=None,
                    help="显式版本号；缺省按 v<年>.<月>.<序号> 自增")
    ap.add_argument("--rule-ids", default="",
                    help="逗号分隔规则ID；缺省发布全部 DRAFT 规则")
    ap.add_argument("--by", required=True)
    ap.add_argument("--summary", default="发布人工裁决沉淀规则")
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_calibre", conf)
    try:
        ids = [x.strip() for x in args.rule_ids.split(",") if x.strip()] or None
        publish_version(spark, args.effective_date, args.by,
                        args.summary, ids, explicit_version=args.version)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
