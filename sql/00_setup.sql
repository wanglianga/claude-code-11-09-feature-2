-- =====================================================================
-- 00_setup.sql  命名空间与 Catalog 初始化
-- 方言：Spark 3.5 SQL + Iceberg（format-version=2，支持 MERGE/行级删除）
-- 本地：spark.sql.catalog.ltl.type=hadoop
-- 生产：改为 type=hive（HiveMetastore），表名与 DDL 保持不变
-- 分层：ods 原始层 / dwd 明细层 / dim 维表层 / dim_stg 维表落地层
--       app 应用层(含旧宽表兼容视图) / meta 元数据与质量层
-- =====================================================================

CREATE NAMESPACE IF NOT EXISTS ltl.ods;
CREATE NAMESPACE IF NOT EXISTS ltl.dwd;
CREATE NAMESPACE IF NOT EXISTS ltl.dim;
CREATE NAMESPACE IF NOT EXISTS ltl.dim_stg;
CREATE NAMESPACE IF NOT EXISTS ltl.app;
CREATE NAMESPACE IF NOT EXISTS ltl.meta;

-- 作业会话内执行 USE ltl; 之后，下文 DDL 均可直接用 schema.table 两段名
