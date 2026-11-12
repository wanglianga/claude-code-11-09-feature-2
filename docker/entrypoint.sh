#!/usr/bin/env bash
# 统一入口：standalone(airflow 一体) / webserver / scheduler / spark-submit / spark-sql / bash
set -euo pipefail

case "${1:-standalone}" in
  standalone)
    export AIRFLOW__CORE__LOAD_EXAMPLES=${AIRFLOW__CORE__LOAD_EXAMPLES:-false}
    export AIRFLOW__CORE__DAGS_FOLDER=/opt/ltl/orchestration/dags
    export AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true
    # 首次启动自动建仓
    if [ ! -f /opt/warehouse/.initialized ]; then
      spark-submit --master local[2] /opt/ltl/jobs/ddl_init.py --conf "$PIPELINE_CONF"
      touch /opt/warehouse/.initialized
    fi
    exec airflow standalone
    ;;
  webserver)
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  spark-submit)
    shift
    exec spark-submit --master local[2] "$@"
    ;;
  spark-sql)
    shift
    exec spark-sql "$@"
    ;;
  bash|sh)
    exec bash
    ;;
  *)
    exec "$@"
    ;;
esac
