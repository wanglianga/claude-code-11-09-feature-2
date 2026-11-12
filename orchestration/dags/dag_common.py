"""Airflow 编排公共配置：spark-submit 命令构造与仓库路径。

容器内：
  SPARK_HOME=/opt/spark，作业目录 /opt/ltl/jobs，配置 /opt/ltl/conf
本地裸机：设置环境变量 LTL_HOME 指向仓库根目录即可复用同一 DAG。
"""
from __future__ import annotations

import os

LTL_HOME = os.environ.get("LTL_HOME", "/opt/ltl")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")
CONF = os.path.join(LTL_HOME, "conf", "pipeline.conf")
JOBS_DIR = os.path.join(LTL_HOME, "jobs")

# local 模式即可驱动 Iceberg catalog；生产改 yarn/k8s master 与资源参数
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[2]")
# Iceberg/H2 运行时 jar 已在镜像内预置到 $SPARK_HOME/jars（见 docker/Dockerfile），
# 默认不再用 --packages 联网拉取；裸机缺 jar 时可设置该环境变量补充坐标。
ICEBERG_PACKAGE = os.environ.get("ICEBERG_PACKAGES", "").strip()


def spark_submit(job_file: str, *job_args: str) -> str:
    """生成 spark-submit 命令（BashOperator/SSHOperator 均可消费）。"""
    pkg = f"--packages {ICEBERG_PACKAGE} " if ICEBERG_PACKAGE else ""
    return (
        f"{SPARK_SUBMIT} --master {SPARK_MASTER} {pkg}"
        f"--conf spark.driver.extraJavaOptions=-Duser.timezone=Asia/Shanghai "
        f"{os.path.join(JOBS_DIR, job_file)} --conf {CONF} {' '.join(job_args)}"
    )
