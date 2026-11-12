-- =====================================================================
-- 03_dim_stg.sql  维表落地层（DIM_STG）
--
-- 上游主数据（网点/派件员/路由/客户）每日全量快照落地到 *_stg，
-- 只做类型转换，不做历史判断；拉链闭合由 dim_scd2_job 完成。
-- stg 表按 snapshot_date 分区，保留每日快照以便重放拉链。
-- =====================================================================

CREATE TABLE IF NOT EXISTS ltl.dim_stg.hub_stg (
    hub_code        STRING  COMMENT '网点业务键（自然键）',
    hub_name        STRING  COMMENT '网点名称（可能改名）',
    hub_type        STRING  COMMENT 'SALES营业部/HUB分拨/STATION驿站',
    region_code     STRING  COMMENT '所属大区编码',
    parent_hub_code STRING  COMMENT '上级网点编码（合并撤销时变化）',
    status          STRING  COMMENT 'ACTIVE/CLOSED/MERGED',
    merged_into     STRING  COMMENT '被合并撤销后的承接网点编码',
    snapshot_date   DATE    COMMENT '主数据快照日期（=拉链候选生效日）',
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP
)
USING iceberg
PARTITIONED BY (snapshot_date)
COMMENT '网点主数据每日快照落地'
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS ltl.dim_stg.courier_stg (
    courier_code    STRING  COMMENT '派件员工号（自然键）',
    courier_name    STRING  COMMENT '派件员姓名',
    hub_code        STRING  COMMENT '所属网点编码（换片区时变化）',
    service_area    STRING  COMMENT '负责片区（换片区时变化）',
    status          STRING  COMMENT 'ACTIVE/LEAVED',
    snapshot_date   DATE,
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP
)
USING iceberg
PARTITIONED BY (snapshot_date)
COMMENT '派件员主数据每日快照落地'
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS ltl.dim_stg.route_stg (
    route_code      STRING  COMMENT '路由编码（自然键）',
    route_name      STRING  COMMENT '路由名称',
    origin_hub_code STRING  COMMENT '起点网点',
    dest_hub_code   STRING  COMMENT '终点网点',
    transit_hubs    STRING  COMMENT '途经分拨有序列表(JSON数组)',
    status          STRING,
    snapshot_date   DATE,
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP
)
USING iceberg
PARTITIONED BY (snapshot_date)
COMMENT '路由主数据每日快照落地'
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS ltl.dim_stg.customer_stg (
    customer_code   STRING  COMMENT '客户编码（自然键）',
    customer_name   STRING  COMMENT '客户名称',
    customer_level  STRING  COMMENT 'KA/STD/SMB',
    sales_owner     STRING  COMMENT '归属销售',
    status          STRING,
    snapshot_date   DATE,
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP
)
USING iceberg
PARTITIONED BY (snapshot_date)
COMMENT '客户主数据每日快照落地'
TBLPROPERTIES ('format-version' = '2');
