"""冲突治理 DAG（手动触发）：人工裁决 -> 沉淀规则 -> 口径版本发布 -> 重新出报。

三个任务均为手动触发，按治理顺序串联；Airflow UI 传参示例：
  1) adjudicate（裁决 OPEN 冲突，可选沉淀 DRAFT 规则）
     {"conflict_id": "<sha256>", "decision": "PREFER_SOURCE",
      "prefer_source": "SCANNER", "by": "zhangsan",
      "note": "扫描枪时间为准", "promote_rule": true}
     或按运单一次裁决：{"waybill_no": "WB010", "decision": "DISMISS", "by": "lisi"}
  2) publish_calibre（发布 DRAFT 规则为下一口径版本；已发布报表不受影响）
     {"effective_date": "2026-11-10", "by": "data_governance",
      "summary": "客服补录滞后采信扫描枪", "rule_ids": ""}
  3) timeliness（按新版本重新出报，新版本分区追加，旧版本分区保持不变）
     {}

安全设计：
  * 裁决作业只改冲突表裁决列，ODS/DWD 原始记录一行不动；
  * 不发布口径版本，DRAFT 规则永远不会被 reconcile 自动应用；
  * timeliness 对已有统计行的版本直接拒绝重写（作业内强校验）。
"""
from __future__ import annotations

import subprocess

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from dag_common import spark_submit


def run_adjudicate(**_context) -> None:
    p = get_current_context()["params"]
    args = ["--decision", p["decision"], "--by", p["by"]]
    if p.get("conflict_id"):
        args += ["--conflict-id", p["conflict_id"]]
    if p.get("waybill_no"):
        args += ["--waybill-no", p["waybill_no"]]
    if p.get("prefer_source"):
        args += ["--prefer-source", p["prefer_source"]]
    if p.get("note"):
        args += ["--note", p["note"]]
    if p.get("promote_rule"):
        args.append("--promote-rule")
    subprocess.run(spark_submit("adjudication_job.py", *args),
                   shell=True, check=True)


def run_publish_calibre(**_context) -> None:
    p = get_current_context()["params"]
    args = ["--effective-date", p["effective_date"], "--by", p["by"],
            "--summary", p.get("summary", "发布人工裁决沉淀规则")]
    if p.get("rule_ids"):
        args += ["--rule-ids", p["rule_ids"]]
    subprocess.run(spark_submit("calibre_job.py", *args),
                   shell=True, check=True)


def run_timeliness(**_context) -> None:
    subprocess.run(spark_submit("timeliness_job.py"),
                   shell=True, check=True)


with DAG(
    dag_id="ltl_conflict_governance",
    description="多来源冲突人工裁决、规则沉淀与口径版本发布",
    default_args={"owner": "data_platform", "retries": 0},
    start_date=pendulum.datetime(2026, 11, 10, tz="Asia/Shanghai"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ltl", "event_warehouse", "conflict", "governance"],
    params={
        "conflict_id": "",
        "waybill_no": "",
        "decision": "PREFER_SOURCE",
        "prefer_source": "SCANNER",
        "by": "data_governance",
        "note": "",
        "promote_rule": False,
        "effective_date": "2026-11-10",
        "rule_ids": "",
        "summary": "发布人工裁决沉淀规则",
    },
) as dag:
    adjudicate = PythonOperator(task_id="adjudicate",
                                python_call=run_adjudicate)
    publish_calibre = PythonOperator(task_id="publish_calibre",
                                     python_call=run_publish_calibre)
    timeliness = PythonOperator(task_id="timeliness_republish",
                                python_call=run_timeliness)
    # 治理顺序：裁决（可沉淀 DRAFT）-> 发布新版本 -> 新版本出报；
    # 也可单独触发任一任务（任务间依赖仅表达推荐顺序，手动清依赖即可单跑）
    adjudicate >> publish_calibre >> timeliness
