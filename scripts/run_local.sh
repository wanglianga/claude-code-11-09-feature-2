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

echo "==> [1/7] 生成本地配置与夹具数据"
python3 "${ROOT}/tests/make_fixtures.py" --root "${ROOT}"

echo "==> [2/7] DDL 建仓"
"${PY}" --master "local[2]" "${ROOT}/jobs/ddl_init.py" --conf "${PIPELINE_CONF}"

echo "==> [3/7] 维表拉链（2026-10-31 初始快照 / 2026-11-02 改名换片区）"
"${PY}" --master "local[2]" "${ROOT}/jobs/dim_stg_load.py" --conf "${PIPELINE_CONF}" --snapshot-date 2026-10-31
"${PY}" --master "local[2]" "${ROOT}/jobs/dim_scd2_job.py" --conf "${PIPELINE_CONF}" --snapshot-date 2026-10-31
"${PY}" --master "local[2]" "${ROOT}/jobs/dim_stg_load.py" --conf "${PIPELINE_CONF}" --snapshot-date 2026-11-02
"${PY}" --master "local[2]" "${ROOT}/jobs/dim_scd2_job.py" --conf "${PIPELINE_CONF}" --snapshot-date 2026-11-02

echo "==> [4/7] 事件接入与明细（含乱序/重复/多次异常/迟到补数）"
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

# 11-05 分拨系统断连恢复：WB006 的 11-01 ARRIVE 迟到补报，走独立补数入口；
# 当日落地的正常事件（WB014 多来源签收）仍按标准 ODS->DWD 增量接入
"${PY}" --master "local[2]" "${ROOT}/jobs/ods_load.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-05
"${PY}" --master "local[2]" "${ROOT}/jobs/dwd_detail_job.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-05
"${PY}" --master "local[2]" "${ROOT}/jobs/backfill_job.py" --conf "${PIPELINE_CONF}" \
    --scope WAYBILL --waybill-nos WB006 --switch
"${PY}" --master "local[2]" "${ROOT}/jobs/dq_check_job.py" --conf "${PIPELINE_CONF}" --biz-date 2026-11-02

# 旧宽表逆透视迁移冒烟（独立单号，含幂等重跑）
"${PY}" --master "local[2]" "${ROOT}/tests/smoke_legacy_migration.py" --conf "${PIPELINE_CONF}"

echo "==> [5/7] 多来源可信度定主 + 关键节点交叉校验（全区间重算）"
"${PY}" --master "local[2]" "${ROOT}/jobs/reconcile_job.py" --conf "${PIPELINE_CONF}" \
    --start-date 2026-11-01 --end-date 2026-11-06

echo "==> [6/7] 首版口径(v2026.11.0)时效统计：未裁决冲突运单被排除"
"${PY}" --master "local[2]" "${ROOT}/jobs/timeliness_job.py" --conf "${PIPELINE_CONF}" \
    --calibre-version v2026.11.0

echo "==> [7/7] 人工裁决 -> 规则沉淀(DRAFT) -> 发布 v2 -> 新版本出报"
# WB010 人工裁决采信扫描枪，并把该裁决沉淀为 DRAFT 规则（SIGN 时间冲突且 CS 离群时采信 SCANNER）
WB010_CID="$("${PY}" --master "local[2]" "${ROOT}/jobs/print_conflict_id.py" \
    --conf "${PIPELINE_CONF}" --waybill-no WB010 --conflict-field EVENT_TIME \
    | grep -E '^[0-9a-f]{64}$')"
"${PY}" --master "local[2]" "${ROOT}/jobs/adjudication_job.py" --conf "${PIPELINE_CONF}" \
    --conflict-id "${WB010_CID}" --decision PREFER_SOURCE --prefer-source SCANNER \
    --by zhangsan --note "现场扫描枪时间为准，客服补录滞后" --promote-rule
# WB011 终态互斥人工裁决（不沉淀规则）
WB011_CID="$("${PY}" --master "local[2]" "${ROOT}/jobs/print_conflict_id.py" \
    --conf "${PIPELINE_CONF}" --waybill-no WB011 --conflict-field EVENT_STATUS \
    | grep -E '^[0-9a-f]{64}$')"
"${PY}" --master "local[2]" "${ROOT}/jobs/adjudication_job.py" --conf "${PIPELINE_CONF}" \
    --conflict-id "${WB011_CID}" --decision PREFER_SOURCE --prefer-source SCANNER \
    --by lisi --note "扫描枪签收闭环为准，拒收补录驳回"
# 发布下一口径版本：DRAFT 规则此时才生效，已发布的 v1 统计行保持不变
"${PY}" --master "local[2]" "${ROOT}/jobs/calibre_job.py" --conf "${PIPELINE_CONF}" \
    --effective-date 2026-11-10 --by data_governance \
    --summary "发布规则：SIGN时间冲突且客服补录离群时采信扫描枪"
# 全区间重算：WB014 命中已发布规则被自动裁决；WB013 路由离群无规则，保持 OPEN
"${PY}" --master "local[2]" "${ROOT}/jobs/reconcile_job.py" --conf "${PIPELINE_CONF}" \
    --start-date 2026-11-01 --end-date 2026-11-06
# v2 版本出报（新版本分区追加；v1 分区不可变，重复出 v1 会被作业拒绝）
"${PY}" --master "local[2]" "${ROOT}/jobs/timeliness_job.py" --conf "${PIPELINE_CONF}"

echo "==> [pytest] 端到端断言"
cd "${ROOT}"
PY4J="$(ls "${SPARK_HOME}"/python/lib/py4j-*.zip | head -1)"
PYTHONPATH="${ROOT}/jobs:${SPARK_HOME}/python:${PY4J}:${PYTHONPATH:-}" \
    "${ROOT}/.venv/bin/python" -m pytest tests/ -v
echo "ALL LOCAL STEPS DONE"
