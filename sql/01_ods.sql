-- =====================================================================
-- 01_ods.sql  原始数据层（ODS）
--
-- 设计约束（对应需求）：
--   1. 原样保存上游每次推送的运单事件，不做清洗与合并；
--      同一事件被重复上报时，原始报文全部保留，去重在 DWD 层完成。
--   2. 必留：事件ID、来源系统、上报时间、业务发生时间、原始报文。
--   3. 覆盖事件类型：揽收 PICKUP / 发件 DEPART / 到件 ARRIVE /
--      中转 TRANSFER / 派送 DELIVER / 签收 SIGN / 拒收 REJECT /
--      异常 EXCEPTION / 回单 POD。
--   4. 二级分区：biz_date（业务日期，取自 event_time）+ source_system，
--      单分区控制在可扫范围内，补数只重跑受影响分区。
--   5. Schema 演进：只加列且带默认值、不改类型、不删列（见 06_evolution.sql）。
-- 表格式：Iceberg v2（行级 MERGE、快照回滚、分区演进均依赖它）。
-- =====================================================================

CREATE TABLE IF NOT EXISTS ltl.ods.waybill_event_raw (
    event_id        STRING        COMMENT '上游事件唯一ID，重复上报时该值相同，DWD按此去重',
    waybill_no      STRING        COMMENT '运单号',
    event_type      STRING        COMMENT 'PICKUP/DEPART/ARRIVE/TRANSFER/DELIVER/SIGN/REJECT/EXCEPTION/POD',
    source_system   STRING        COMMENT '来源系统：SCANNER扫描枪/HUB分拨系统/ROUTE第三方路由回调',
    report_time     TIMESTAMP     COMMENT '上报时间（上游推送到平台的时间，乱序判定用）',
    event_time      TIMESTAMP     COMMENT '业务发生时间（扫描/作业实际发生时间，排序与水位线用）',
    raw_payload     STRING        COMMENT '原始报文全文(JSON)，原样保存不解析不裁剪',
    source_file     STRING        COMMENT '落地文件名，可追溯到上游批次文件',
    ingest_batch_id STRING        COMMENT '入湖批次ID',
    ingest_time     TIMESTAMP     COMMENT '入湖时间',
    biz_date        DATE          COMMENT '业务日期=to_date(event_time)，同时为一级分区键'
)
USING iceberg
PARTITIONED BY (biz_date, source_system)
COMMENT 'ODS 运单事件原始表：原样保存，不清洗不合并'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100',
    -- 快照保留 30 天，与补数窗口对齐；过期快照由维护作业 expire_snapshots 清理
    'history.expire.max-snapshot-age-ms' = '2592000000'
);

-- 说明：biz_date 同时作为列与分区字段（Iceberg identity transform），
-- 由加载作业按 event_time 落盘前派生：biz_date = to_date(event_time)。
-- source_system 显式冗余为列，便于 DQ 按来源系统下钻。
