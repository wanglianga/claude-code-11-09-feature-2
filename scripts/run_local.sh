#!/usr/bin/env bash
# =====================================================================
# 本地一键验证脚本（无需 Docker）：
#   1. 使用本地 Spark（SPARK_HOME 环境变量，默认 /tmp/spark-3.5.9-bin-hadoop3）
#   2. 初始化本地 Iceberg 仓库 runtime/warehouse
#   3. 生成夹具 -> 跑完整链路 -> pytest 断言
# 用法：bash scripts/run_local.sh
# =====================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export SPARK_HOME="${SPARK_HOME:-/tmp/spark-3.5.9-bin-hadoop3}"
export PIPELINE_CONF="${ROOT}/conf/pipeline.local.conf"
export PYTHONPATH="${ROOT}/jobs:${PYTHONPATH:-}"
export PY="${SPARK_HOME}/bin/spark-submit"

echo "==> [1/5] 生成本地配置与夹具数据"
python3 "${ROOT}/tests/make_fixtures.py" --root "${ROOT}"

echo "==> [2/5] DDL 建仓"
"${PY}" --master "local[2]" "${ROOT}/jobs/ddl_init.py" --conf "${PIPELINE_CONF}"

echo "==> [3/5] 维表拉链（2026-10-31 初始快照 / 2026-11-02 改名换片区）"
"${PY}" --master "local[2]" "${ROOT}/jobs/dim_stg_load.py" --conf "${PIPELINE_CONF}" --snapshot-date 2026-10-31
"${PY}" --master "local[2]" "${ROOT}/jobs/dim_scd2_job.py" --conf "${PIPELINE_CONF}" --snapshot-date 2026-10-31
"${PY}" --master "local[2]" "${ROOT}/jobs/dim_stg_load.py" --conf "${PIPELINE_CONF}" --snapshot-date 2026-11-02
"${PY}" --master "local[2]" "${ROOT}/jobs/dim_scd2_job.py" --conf "${PIPELINE_CONF}" --snapshot-date 2026-11-02

echo "==> [4/5] 事件接入与明细（含乱序/重复/多次异常/迟到补数）"
"${PY}" --master "local[2]" "${ROOT}/jobs/ods_load.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-01
"${PY}" --master "local[2]" "${ROOT}/jobs/dwd_detail_job.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-01
"${PY}" --master "local[2]" "${ROOT}/jobs/ods_load.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-02
"${PY}" --master "local[2]" "${ROOT}/jobs/dwd_detail_job.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-02
"${PY}" --master "local[2]" "${ROOT}/jobs/dq_check_job.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-01

# 11-03/11-04 正常推进（WB009 新版本生效 / WB002 压货后发出）
"${PY}" --master "local[2]" "${ROOT}/jobs/ods_load.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-03
"${PY}" --master "local[2]" "${ROOT}/jobs/dwd_detail_job.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-03
"${PY}" --master "local[2]" "${ROOT}/jobs/ods_load.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-04
"${PY}" --master "local[2]" "${ROOT}/jobs/dwd_detail_job.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-04

# 11-05 分拨系统断连恢复：WB006 的 11-01 ARRIVE 迟到补报，走独立补数入口
"${PY}" --master "local[2]" "${ROOT}/jobs/ods_load.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-05
"${PY}" --master "local[2]" "${ROOT}/jobs/backfill_job.py" --conf "${PIPELINE_CONF}" \
    --scope WAYBILL --waybill-nos WB006 --switch
"${PY}" --master "local[2]" "${ROOT}/jobs/dq_check_job.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-02

# 旧宽表逆透视迁移冒烟（独立单号，含幂等重跑）
"${PY}" --master "local[2]" "${ROOT}/tests/smoke_legacy_migration.py" --conf "${PIPELINE_CONF}"

echo "==> [5/5] pytest 断言"
cd "${ROOT}"
PY4J="$(ls "${SPARK_HOME}"/python/lib/py4j-*.zip | head -1)"
PYTHONPATH="${ROOT}/jobs:${SPARK_HOME}/python:${PY4J}:${PYTHONPATH:-}" \
    "${ROOT}/.venv/bin/python" -m pytest tests/ -v
echo "ALL LOCAL STEPS DONE"
