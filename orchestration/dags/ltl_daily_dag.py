"""每日增量 DAG：ODS 接入 -> 维表快照落地 -> SCD2 拉链 -> DWD 明细 -> DQ 日检。

调度时间：每日 02:17（错峰），业务日期取前一天（data_interval_start 对齐 T-1）。
补数与全量回刷走独立 DAG ltl_backfill，不与本 DAG 混用。
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from dag_common import spark_submit

DAG_ARGS = {
    "owner": "data_platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

with DAG(
    dag_id="ltl_daily_incremental",
    description="零担事件仓库每日增量：ODS/维表拉链/DWD/DQ",
    default_args=DAG_ARGS,
    start_date=pendulum.datetime(2026, 11, 10, tz="Asia/Shanghai"),
    schedule="17 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ltl", "event_warehouse", "incremental"],
) as dag:
    start = EmptyOperator(task_id="start")

    # T-1 业务日期：每日 02:17 触发时，Airflow 逻辑日期 ds 即前一天
    biz = "{{ ds }}"
    snap = biz

    ods_load = BashOperator(
        task_id="ods_load",
        bash_command=spark_submit("ods_load.py", "--biz-date", biz),
    )
    dim_stg_load = BashOperator(
        task_id="dim_stg_load",
        bash_command=spark_submit("dim_stg_load.py",
                                  "--snapshot-date", snap, "--dim-name", "all"),
    )
    # 拉链前先跑影响预览，结果进任务日志，供值班确认组织调整影响面
    scd2_preview = BashOperator(
        task_id="scd2_preview",
        bash_command=spark_submit("dim_scd2_job.py",
                                  "--snapshot-date", snap, "--dim-name", "all",
                                  "--preview"),
    )
    scd2_apply = BashOperator(
        task_id="scd2_apply",
        bash_command=spark_submit("dim_scd2_job.py",
                                  "--snapshot-date", snap, "--dim-name", "all"),
    )
    dwd_detail = BashOperator(
        task_id="dwd_detail",
        bash_command=spark_submit("dwd_detail_job.py", "--biz-date", biz),
    )
    dq_check = BashOperator(
        task_id="dq_check",
        bash_command=spark_submit("dq_check_job.py", "--biz-date", biz),
    )
    end = EmptyOperator(task_id="end")

    # ODS 与维表落地互不依赖可并行；拉链必须在 DWD 代理键解析前完成
    start >> [ods_load, dim_stg_load]
    dim_stg_load >> scd2_preview >> scd2_apply
    [ods_load, scd2_apply] >> dwd_detail >> dq_check >> end
