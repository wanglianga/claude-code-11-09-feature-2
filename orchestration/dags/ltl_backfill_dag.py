"""补数 DAG（手动触发 / 独立入口）：只重跑受影响分区，先对比后切换。

触发方式（Airflow UI / CLI 传参）：
  # 按运单号补数，只出对比，不生效
  {"scope": "WAYBILL", "waybill_nos": "WB001,WB002"}
  # 按时间区间补数并生效
  {"scope": "DATE_RANGE", "start_date": "2026-10-20",
   "end_date": "2026-10-25", "switch": true}
  # 全量回刷唯一允许路径：独立表重建 + 换表
  {"scope": "DATE_RANGE", "start_date": "2026-10-01",
   "end_date": "2026-11-11", "switch": true, "full_rebuild": true}

安全设计：
  * switch 默认 false，第一次触发只产出前后对比（DIFF_READY）；
  * 复核 diff 文件与 backfill_job_log 后，再带 switch=true 触发第二次；
  * 无任何整表覆盖生产分区的路径。

实现说明：两个任务都用 PythonOperator 在【任务运行期】读取 params 并拼
spark-submit，避免在 DAG 解析期调用 get_current_context（解析期没有任务上下文）。
"""
from __future__ import annotations

import subprocess

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from dag_common import spark_submit


def run_backfill(**_context) -> None:
    p = get_current_context()["params"]
    args = ["--scope", p["scope"]]
    if p["scope"] == "WAYBILL":
        args += ["--waybill-nos", p["waybill_nos"]]
    else:
        args += ["--start-date", p["start_date"], "--end-date", p["end_date"]]
    if p.get("switch"):
        args.append("--switch")
    if p.get("full_rebuild"):
        args.append("--full-rebuild")
    subprocess.run(
        spark_submit("backfill_job.py", *args), shell=True, check=True)


def run_dq_recheck(**_context) -> None:
    ctx = get_current_context()
    p = ctx["params"]
    biz_date = p["start_date"] if p["scope"] == "DATE_RANGE" else ctx["ds"]
    subprocess.run(
        spark_submit("dq_check_job.py", "--biz-date", biz_date),
        shell=True, check=True)


with DAG(
    dag_id="ltl_backfill",
    description="事件仓库补数：按运单号/时间区间重放，影子表对比后受控切换",
    default_args={"owner": "data_platform", "retries": 0},
    start_date=pendulum.datetime(2026, 11, 10, tz="Asia/Shanghai"),
    schedule=None,  # 仅手动触发
    catchup=False,
    max_active_runs=1,
    tags=["ltl", "event_warehouse", "backfill"],
    params={
        "scope": "DATE_RANGE",
        "waybill_nos": "",
        "start_date": "2026-11-01",
        "end_date": "2026-11-10",
        "switch": False,
        "full_rebuild": False,
    },
) as dag:
    backfill = PythonOperator(task_id="backfill_run", python_callable=run_backfill)
    dq_recheck = PythonOperator(task_id="dq_recheck", python_callable=run_dq_recheck)
    backfill >> dq_recheck
