"""公共工具：配置读取、Iceberg SparkSession、批次ID 与表名常量。

所有作业既可以用 spark-submit 提交（main 入口），
也可以在测试中 import 其函数并传入本地 SparkSession 复用同一套逻辑。
"""
from __future__ import annotations

import configparser
import os
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

# ---- 全仓库统一表名（与 sql/*.sql 完全一致）----
ODS_EVENT = "ltl.ods.waybill_event_raw"
DWD_EVENT = "ltl.dwd.waybill_event_detail"
STG_TABLES = {
    "hub": "ltl.dim_stg.hub_stg",
    "courier": "ltl.dim_stg.courier_stg",
    "route": "ltl.dim_stg.route_stg",
    "customer": "ltl.dim_stg.customer_stg",
}
DIM_TABLES = {
    "hub": "ltl.dim.dim_hub",
    "courier": "ltl.dim.dim_courier",
    "route": "ltl.dim.dim_route",
    "customer": "ltl.dim.dim_customer",
}
# 各维表：自然键列、该自然键在事实(DWD)中的对应列、代理键列、参与变化检测的属性列
DIM_SPECS = {
    "hub": {
        "natural_key": "hub_code",
        "fact_nk": "hub_code",
        "sk": "hub_sk",
        "attr_cols": ["hub_name", "hub_type", "region_code",
                      "parent_hub_code", "status", "merged_into"],
    },
    "courier": {
        "natural_key": "courier_code",
        "fact_nk": "courier_code",
        "sk": "courier_sk",
        "attr_cols": ["courier_name", "hub_code", "service_area", "status"],
    },
    "route": {
        "natural_key": "route_code",
        "fact_nk": "waybill_route_code",
        "sk": "route_sk",
        "attr_cols": ["route_name", "origin_hub_code", "dest_hub_code",
                      "transit_hubs", "status"],
    },
    "customer": {
        "natural_key": "customer_code",
        "fact_nk": "customer_code",
        "sk": "customer_sk",
        "attr_cols": ["customer_name", "customer_level", "sales_owner", "status"],
    },
}
WM_LOG = "ltl.meta.watermark_log"
DQ_TABLE = "ltl.meta.dq_daily_result"
BACKFILL_LOG = "ltl.meta.backfill_job_log"

UNMATCHED_SK = -1
FUTURE_DATE = "9999-12-31"
EVENT_TYPES = ["PICKUP", "DEPART", "ARRIVE", "TRANSFER",
               "DELIVER", "SIGN", "REJECT", "EXCEPTION", "POD"]


def load_conf(path: str | None = None) -> dict:
    """读取 conf/pipeline.conf（INI 风格，使用 DEFAULT 段）。

    configparser 的 DEFAULT 是特殊段：defaults() 可取，has_section 不可见。
    """
    path = path or os.environ.get("PIPELINE_CONF", "/opt/conf/pipeline.conf")
    cp = configparser.ConfigParser()
    if not Path(path).exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    cp.read(path, encoding="utf-8")
    if cp.defaults():
        return dict(cp.defaults())
    # 兼容使用显式 [pipeline] 段的写法
    section = cp.sections()[0]
    return dict(cp[section])


def new_batch_id(prefix: str) -> str:
    # 生产可替换为调度系统的 run_id；时间戳已保证同秒内不同作业前缀不冲突
    return f"{prefix}_{time.strftime('%Y%m%d%H%M%S')}_{os.getpid()}"


def build_spark(app_name: str, conf: dict | None = None,
                extra_packages: list[str] | None = None) -> SparkSession:
    """构建带 Iceberg catalog `ltl` 的 SparkSession。

    catalog.type 三种模式：
      jdbc（本地默认，视图可用）：Iceberg JdbcCatalog + H2 文件库，
        元数据持久化在 warehouse/catalog.mv.db，无 Hive 锁表问题，
        JdbcCatalog 实现 ViewCatalog，app 层视图可直接建在 ltl catalog 内。
      hive（生产推荐）：Iceberg HiveCatalog；catalog.uri 指向
        thrift://metastore:9083。留空时退回嵌入式 Derby（仅演示，需关锁）。
      hadoop（极简、无视图）：元数据随表目录，不支持 CREATE VIEW。
    """
    conf = conf or load_conf()
    warehouse = conf.get("warehouse.dir", "/opt/warehouse")
    catalog_type = conf.get("catalog.type", "jdbc")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ltl", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ltl.type", catalog_type)
        .config("spark.sql.catalog.ltl.warehouse", warehouse)
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        # 自适应分区数在本地小数据下收敛到 1，生产由 spark-defaults 覆盖
        .config("spark.sql.shuffle.partitions", "8")
    )

    if catalog_type == "jdbc":
        # H2 文件库；自动建 iceberg_tables/iceberg_namespace_properties 元数据表。
        # DATABASE_TO_UPPER=FALSE：Iceberg 的元数据探测用小写标识符（iceberg_type 等），
        # 关闭 H2 默认的大写折叠，避免跨进程大小写不匹配导致重复 ALTER。
        default_uri = (f"jdbc:h2:file:{os.path.join(warehouse, 'catalog')}"
                       ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE")
        jdbc_uri = conf.get("catalog.uri", "") or default_uri
        # 每个 spark-submit 都是新进程：仅在 H2 数据库文件尚不存在（全新仓库）时
        # 让 catalog 自动建表，已存在则关闭，避免重复 CREATE TABLE 报错。
        explicit_init = conf.get("jdbc.init-catalog-tables", "").strip().lower()
        if explicit_init in ("true", "false"):
            init_flag = explicit_init == "true"
        else:
            db_file = _h2_db_file(jdbc_uri)
            init_flag = not (db_file and Path(db_file).exists())
        builder = (
            builder
            .config("spark.sql.catalog.ltl.uri", jdbc_uri)
            .config("spark.sql.catalog.ltl.jdbc.user",
                    conf.get("jdbc.user", "sa"))
            .config("spark.sql.catalog.ltl.jdbc.password",
                    conf.get("jdbc.password", ""))
            .config("spark.sql.catalog.ltl.jdbc.init-catalog-tables",
                    "true" if init_flag else "false")
            # V1 schema 含 iceberg_type 列，启用视图（ViewCatalog）能力
            .config("spark.sql.catalog.ltl.jdbc.schema-version", "V1")
        )
    elif catalog_type == "hive":
        metastore_db = os.path.join(warehouse, "metastore_db")
        builder = (
            builder
            .config("spark.sql.warehouse.dir", warehouse)
            .config("spark.hadoop.hive.metastore.warehouse.dir", warehouse)
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={metastore_db};create=true")
            # 本地嵌入式 Derby：首次启动自动初始化 Hive 元数据 schema
            .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
            .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
            .config("spark.hadoop.datanucleus.fixedDatastore", "false")
            .config("spark.hadoop.hive.metastore.schema.verification", "false")
            .config("spark.hadoop.hive.metastore.event.db.notification.api.auth",
                    "false")
            .enableHiveSupport()
        )
        uri = conf.get("catalog.uri", "").strip()
        if uri:
            builder = builder.config("spark.sql.catalog.ltl.uri", uri)
        else:
            # 本地嵌入式 Derby 无事务/锁表（HIVE_LOCKS 不存在），关闭元数据锁
            builder = (
                builder
                .config("spark.sql.catalog.ltl.lock-impl",
                        "org.apache.iceberg.hive.NoLock")
                .config(
                    "spark.sql.catalog.ltl.iceberg.engine.hive.lock-enabled",
                    "false")
            )

    for pkg in extra_packages or []:
        builder.config("spark.jars.packages", pkg)
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
    return spark


@dataclass
class RunArgs:
    """统一参数容器，测试与 CLI 共用。"""
    biz_date: str | None = None          # YYYY-MM-DD，当日增量业务日期
    start_date: str | None = None        # 补数区间起
    end_date: str | None = None          # 补数区间止（含）
    waybill_nos: list[str] | None = None # 补数运单清单
    scope_type: str = "DATE_RANGE"       # WAYBILL / DATE_RANGE
    snapshot_date: str | None = None     # 维表拉链快照日
    dim_name: str | None = None          # hub/courier/route/customer/all
    preview: bool = False                # 维表变化预览 / 补数只对比不切换
    switch: bool = False                 # 补数对比通过后执行切换
    full_rebuild: bool = False           # 独立表重建 + 视图/表名切换
    conf_path: str | None = None


def _h2_db_file(jdbc_uri: str) -> str | None:
    """从 jdbc:h2:file:<path>;... 解析出 H2 数据文件完整路径（.mv.db）。"""
    marker = "jdbc:h2:file:"
    idx = jdbc_uri.find(marker)
    if idx < 0:
        return None
    rest = jdbc_uri[idx + len(marker):]
    base = rest.split(";", 1)[0]
    return base + ".mv.db"


def column_exists(spark, table: str, column: str) -> bool:
    """判断 Iceberg 表是否已有某列（用于 ADD COLUMN 幂等）。"""
    return column in spark.table(table).columns


def add_column_if_absent(spark, table: str, column: str,
                         col_type: str, comment: str | None = None) -> bool:
    """Spark 3.5 的 ADD COLUMN 不支持 IF NOT EXISTS，用存在性检查实现幂等。

    返回 True 表示本次实际加列。
    """
    if column_exists(spark, table, column):
        return False
    ddl = f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"
    if comment:
        ddl += f" COMMENT '{comment}'"
    spark.sql(ddl)
    return True
