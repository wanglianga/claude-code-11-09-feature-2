# =====================================================================
# 零担事件仓库统一镜像：Spark 3.5 + Iceberg 1.6 + Airflow 2.9
# 一个镜像同时承载：
#   - spark-submit 批处理作业（local 模式，hadoop catalog 写本地卷）
#   - Airflow standalone（webserver+scheduler+worker 一体，便于一键启动）
# 生产环境将 catalog 切换为 HiveCatalog、master 切换为 yarn/k8s 即可，镜像不变。
# =====================================================================
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.9
ARG ICEBERG_VERSION=1.8.1
ARG H2_VERSION=2.2.224
ARG AIRFLOW_VERSION=2.9.3
ARG SPARK_MIRROR=https://mirrors.aliyun.com/apache/spark

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    AIRFLOW_HOME=/opt/airflow \
    LTL_HOME=/opt/ltl \
    PIPELINE_CONF=/opt/ltl/conf/pipeline.conf \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYTHONPATH=/opt/ltl/jobs:$PYTHONPATH

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless bash curl tini procps \
    && rm -rf /var/lib/apt/lists/*

# Spark（含 PySpark）与 Iceberg 运行时 jar（预置进 jars，运行期无需联网拉包）
RUN curl -sL ${SPARK_MIRROR}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
        -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm /tmp/spark.tgz \
    && curl -sL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
        -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    && curl -sL https://repo1.maven.org/maven2/com/h2database/h2/${H2_VERSION}/h2-${H2_VERSION}.jar \
        -o /opt/spark/jars/h2-${H2_VERSION}.jar

# Airflow（仅编排层需要；DAG 不 import pyspark，通过 spark-submit 调作业）
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

WORKDIR /opt/ltl
COPY . /opt/ltl

RUN mkdir -p /opt/warehouse /opt/data/landing /opt/data/backfill \
    && chmod +x /opt/ltl/scripts/*.sh

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080 7077 4040
ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
CMD ["standalone"]
