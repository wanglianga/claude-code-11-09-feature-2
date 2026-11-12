-- =====================================================================
-- 04_dim.sql  维度层（DIM）——四张独立建设的 SCD2 拉链维表
--
-- 拉链规则（四张表完全一致）：
--   * 代理键 *_sk 由作业生成（自然键哈希取正长整型，稳定可复现），
--     DWD 事件明细只存代理键与事件发生时点，不存名称。
--   * 每条版本带生效区间 [eff_date, exp_date)：
--       新增自然键       -> 插入 eff_date=快照日, exp_date='9999-12-31', is_current=true
--       属性变化         -> 旧版本 exp_date=快照日 且 is_current=false，
--                           新版本 eff_date=快照日, exp_date='9999-12-31'
--       合并/撤销/离职   -> 同样以新版本表达（status/承接网点变化），不物理删除历史
--   * 时点关联（禁止用最新维表回刷历史）：
--       ON d.natural_key = f.natural_key
--      AND f.event_date >= d.eff_date AND f.event_date < d.exp_date
--   * 组织架构调整只在新日期分区生效：快照日之前的区间不会被改写，
--     重算影响预览见 jobs/dim_scd2_job.py --preview 与 docs/migration_guide.md。
--   * 只加列演进：维表新增属性列同样带默认值（见 06_evolution.sql）。
-- =====================================================================

-- ---------- 1. 网点/分拨维表 ----------
CREATE TABLE IF NOT EXISTS ltl.dim.dim_hub (
    hub_sk          BIGINT      COMMENT '代理键（稳定，跨版本不变，按自然键生成）',
    hub_code        STRING      COMMENT '自然键：网点编码',
    hub_name        STRING      COMMENT '网点名称（改名保留历史版本）',
    hub_type        STRING,
    region_code     STRING,
    parent_hub_code STRING,
    status          STRING      COMMENT 'ACTIVE/CLOSED/MERGED',
    merged_into     STRING      COMMENT '被合并撤销后的承接网点',
    eff_date        DATE        COMMENT '生效日（含）',
    exp_date        DATE        COMMENT '失效日（不含）；9999-12-31 表示当前版本',
    is_current      BOOLEAN,
    attr_hash       STRING      COMMENT '属性哈希，变化检测依据',
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP
)
USING iceberg
PARTITIONED BY (is_current)
COMMENT '网点/分拨拉链维表：改名/合并/撤销全程可回溯'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 2. 派件员维表 ----------
CREATE TABLE IF NOT EXISTS ltl.dim.dim_courier (
    courier_sk      BIGINT      COMMENT '代理键',
    courier_code    STRING      COMMENT '自然键：派件员工号',
    courier_name    STRING,
    hub_code        STRING      COMMENT '所属网点（换片区时产生新版本）',
    service_area    STRING      COMMENT '负责片区',
    status          STRING      COMMENT 'ACTIVE/LEAVED',
    eff_date        DATE,
    exp_date        DATE,
    is_current      BOOLEAN,
    attr_hash       STRING,
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP
)
USING iceberg
PARTITIONED BY (is_current)
COMMENT '派件员拉链维表：更换片区/网点保留历史，绩效按事件时点关联'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 3. 路由维表 ----------
CREATE TABLE IF NOT EXISTS ltl.dim.dim_route (
    route_sk        BIGINT      COMMENT '代理键',
    route_code      STRING      COMMENT '自然键：路由编码',
    route_name      STRING,
    origin_hub_code STRING,
    dest_hub_code   STRING,
    transit_hubs    STRING      COMMENT '途经分拨有序列表(JSON数组)',
    status          STRING,
    eff_date        DATE,
    exp_date        DATE,
    is_current      BOOLEAN,
    attr_hash       STRING,
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP
)
USING iceberg
PARTITIONED BY (is_current)
COMMENT '路由拉链维表'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 4. 客户维表 ----------
CREATE TABLE IF NOT EXISTS ltl.dim.dim_customer (
    customer_sk     BIGINT      COMMENT '代理键',
    customer_code   STRING      COMMENT '自然键：客户编码',
    customer_name   STRING,
    customer_level  STRING      COMMENT 'KA/STD/SMB（等级调整保留历史）',
    sales_owner     STRING,
    status          STRING,
    eff_date        DATE,
    exp_date        DATE,
    is_current      BOOLEAN,
    attr_hash       STRING,
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP
)
USING iceberg
PARTITIONED BY (is_current)
COMMENT '客户拉链维表：投诉归因按事件发生时点取当时版本'
TBLPROPERTIES ('format-version' = '2');
