"""人工裁决作业：对 OPEN 冲突做人工裁决，并可把裁决沉淀为 DRAFT 规则。

职责边界（对应需求）：
  * 裁决只修改 meta.waybill_conflict 的裁决列与状态，不修改 ODS/DWD 任何原始记录，
    全部来源原始报文继续保留可追溯。
  * 裁决后自动重跑该运单的 reconcile：队列从 PENDING 变 RESOLVED、
    节点主记录刷新冲突标志；但时效统计的【已发布版本行】不会被改写。
  * --promote-rule 把本次裁决沉淀为 meta.adjudication_rule 的 DRAFT 规则。
    DRAFT 规则对 reconcile 作业不可见，必须由 calibre_job 发布到下一个口径版本后
    才对后续冲突自动生效——已发布报表不受影响。

用法：
  # 采用某来源的观测值裁决单条冲突
  spark-submit jobs/adjudication_job.py --conflict-id <id> \
      --decision PREFER_SOURCE --prefer-source SCANNER --by zhangsan --note "扫描枪为准"
  # 驳回冲突（维持主记录取值）
  spark-submit jobs/adjudication_job.py --conflict-id <id> \
      --decision DISMISS --by zhangsan
  # 裁决运单下全部 OPEN 冲突，并把该裁决沉淀为 DRAFT 规则
  spark-submit jobs/adjudication_job.py --waybill-no WB010 \
      --decision PREFER_SOURCE --prefer-source SCANNER --by zhangsan --promote-rule
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F

from common.spark_utils import (
    CONFLICT_TABLE, REVIEW_QUEUE, RULE_TABLE, build_spark, load_conf,
    new_batch_id,
)
from reconcile_job import run_reconcile

# source_values 的结构化 schema（与 reconcile_job 写出的 JSON 对齐）
_SV_SCHEMA = (
    "array<struct<source_system:string,event_id:string,event_time:string,"
    "report_time:string,field_value:string,delta_seconds_vs_master:bigint>>"
)


def _pick_source_value(spark, conflict_id: str, prefer_source: str):
    """从冲突条目的 source_values 中取指定来源的事件ID与取值。"""
    rows = (
        spark.table(CONFLICT_TABLE)
        .filter(F.col("conflict_id") == conflict_id)
        .select(F.explode(F.from_json(F.col("source_values"), _SV_SCHEMA))
                .alias("sv")).collect()
    )
    for r in rows:
        if r.sv.source_system == prefer_source:
            value = r.sv.field_value or r.sv.event_time
            return r.sv.event_id, value
    avail = [r.sv.source_system for r in rows]
    raise ValueError(
        f"冲突 {conflict_id} 的来源取值中没有 {prefer_source}，可用来源: {avail}")


def _next_rule_id(spark) -> str:
    n = spark.table(RULE_TABLE).count()
    return f"RULE_{n + 1:04d}"


def promote_rule(spark, conflict_row, prefer_source: str | None,
                 decision: str, by: str, note: str) -> str:
    """把裁决沉淀为 DRAFT 规则（不生效，等待口径版本发布）。"""
    rule_id = _next_rule_id(spark)
    if decision == "PREFER_SOURCE":
        decision_value = json.dumps({"prefer_source": prefer_source},
                                    ensure_ascii=False)
    else:
        decision_value = json.dumps({"keep": "master"}, ensure_ascii=False)
    pattern = json.dumps({
        "event_type": conflict_row.event_type,
        "conflict_field": conflict_row.conflict_field,
        "outlier_source_system": conflict_row.outlier_source_system,
        "reason_code": conflict_row.reason_code,
    }, ensure_ascii=False)
    spark.sql(f"""
        INSERT INTO {RULE_TABLE}
        VALUES (
            '{rule_id}', 'DRAFT',
            '{conflict_row.event_type}', '{conflict_row.conflict_field}',
            {("'" + conflict_row.outlier_source_system + "'")
                if conflict_row.outlier_source_system else "NULL"},
            {("'" + conflict_row.reason_code + "'")
                if conflict_row.reason_code else "NULL"},
            '{pattern.replace("'", "''")}',
            '{decision}', '{decision_value.replace("'", "''")}',
            '{conflict_row.conflict_id}', '{by}', current_timestamp(),
            NULL, NULL,
            '{(note or "人工裁决沉淀，待下一口径版本发布").replace("'", "''")}'
        )
    """)
    print(f"[adjudicate] 冲突 {conflict_row.conflict_id} 已沉淀 DRAFT 规则 {rule_id}"
          "（发布下一口径版本前不生效）")
    return rule_id


def adjudicate_one(spark, conflict_id: str, decision: str,
                   prefer_source: str | None, by: str, note: str | None,
                   promote: bool) -> dict:
    row = spark.table(CONFLICT_TABLE).filter(
        F.col("conflict_id") == conflict_id).first()
    if row is None:
        raise ValueError(f"冲突条目不存在: {conflict_id}")
    if row.status != "OPEN":
        print(f"[adjudicate] 冲突 {conflict_id} 状态={row.status}，跳过")
        return {"conflict_id": conflict_id, "status": row.status}

    if decision == "PREFER_SOURCE":
        if not prefer_source:
            raise ValueError("PREFER_SOURCE 必须提供 --prefer-source")
        event_id, value = _pick_source_value(spark, conflict_id, prefer_source)
    else:  # DISMISS：维持主记录
        event_id, value = row.master_event_id, row.master_value

    note_sql = ("'" + note.replace("'", "''") + "'") if note else "NULL"
    spark.sql(f"""
        UPDATE {CONFLICT_TABLE}
        SET status = 'ADJUDICATED',
            adjudicated_value = '{value.replace("'", "''")}',
            adjudicated_event_id = '{event_id}',
            adjudicated_by = '{by}',
            adjudicated_time = current_timestamp(),
            adjudication_note = {note_sql}
        WHERE conflict_id = '{conflict_id}' AND status = 'OPEN'
    """)
    rule_id = None
    if promote:
        fresh = spark.table(CONFLICT_TABLE).filter(
            F.col("conflict_id") == conflict_id).first()
        rule_id = promote_rule(spark, fresh, prefer_source, decision, by,
                               note or "")
    print(f"[adjudicate] 冲突 {conflict_id} 裁决完成 decision={decision} "
          f"value={value} event_id={event_id}")
    return {"conflict_id": conflict_id, "status": "ADJUDICATED",
            "waybill_no": row.waybill_no, "rule_id": rule_id}


def run(spark, conf: dict, conflict_ids: list[str], waybill_no: str | None,
        decision: str, prefer_source: str | None, by: str,
        note: str | None, promote: bool) -> dict:
    if waybill_no:
        conflict_ids = [
            r.conflict_id for r in spark.table(CONFLICT_TABLE)
            .filter((F.col("waybill_no") == waybill_no)
                    & (F.col("status") == "OPEN"))
            .collect()]
        if not conflict_ids:
            print(f"[adjudicate] 运单 {waybill_no} 没有 OPEN 冲突")
    if not conflict_ids:
        raise ValueError("必须提供 --conflict-id 或 --waybill-no（且存在 OPEN 冲突）")

    results, waybills = [], set()
    for cid in conflict_ids:
        r = adjudicate_one(spark, cid, decision, prefer_source, by, note,
                           promote)
        results.append(r)
        if r.get("waybill_no"):
            waybills.add(r["waybill_no"])

    # 重跑受影响运单的 reconcile：队列置 RESOLVED、节点主记录冲突标志刷新。
    # 已 ADJUDICATED 的冲突行不会被作业覆盖（MERGE 只更新 OPEN 行）。
    if waybills:
        run_reconcile(spark, conf, waybill_nos=list(waybills))
        for wb in waybills:
            q = spark.table(REVIEW_QUEUE).filter(
                F.col("waybill_no") == wb).first()
            print(f"[adjudicate] 运单 {wb} 队列状态 -> "
                  f"{q.queue_status if q else '(无队列行)'}")
    return {"adjudicated": len(results),
            "rules_drafted": [r["rule_id"] for r in results if r.get("rule_id")]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--conflict-id", action="append", default=[],
                    help="可重复传入多条冲突ID")
    ap.add_argument("--waybill-no", default=None,
                    help="一次裁决该运单下全部 OPEN 冲突")
    ap.add_argument("--decision", choices=["PREFER_SOURCE", "DISMISS"],
                    required=True)
    ap.add_argument("--prefer-source", default=None,
                    help="decision=PREFER_SOURCE 时指定采用的来源系统")
    ap.add_argument("--by", required=True, help="裁决人账号")
    ap.add_argument("--note", default=None)
    ap.add_argument("--promote-rule", action="store_true",
                    help="把裁决沉淀为 DRAFT 规则（下一口径版本才生效）")
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_adjudicate", conf)
    try:
        run(spark, conf, args.conflict_id, args.waybill_no, args.decision,
            args.prefer_source, args.by, args.note, args.promote_rule)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
