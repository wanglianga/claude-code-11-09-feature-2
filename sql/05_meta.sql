-- =====================================================================
-- 05_meta.sql  元数据与数据质量层（META）
--
-- 三张治理表：
--   watermark_log   增量作业事件时间水位线（每日推进，可回退）
--   dq_daily_result 按天×来源系统×分拨中心的质量核对结果与阈值判定
--   backfill_job_log 补数作业台账（受影响分区、前后对比、视图切换状态）
-- =====================================================================

CREATE TABLE IF NOT EXISTS ltl.meta.watermark_log (
    layer           STRING    COMMENT 'ODS/DWD',
    source_system   STRING    COMMENT '来源系统；ALL 表示全源统一水位',
    biz_date        DATE      COMMENT '本次推进到的业务日期（含），下次增量从 biz_date+1 起',
    high_watermark  TIMESTAMP COMMENT '本批次最大 event_time',
    event_count     BIGINT    COMMENT '本批处理事件数',
    ingest_batch_id STRING,
    ingest_time     TIMESTAMP,
    status          STRING    COMMENT 'SUCCESS/FAILED；FAILED 不推进水位'
)
USING iceberg
COMMENT '增量水位线台账：增量按事件时间水位线每日推进'
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS ltl.meta.dq_daily_result (
    biz_date            DATE    COMMENT '业务日期',
    source_system       STRING  COMMENT '来源系统；ALL 为全源汇总',
    hub_code            STRING  COMMENT '分拨中心；ALL 为全网汇总，异常可下钻定位',
    metric_code         STRING  COMMENT 'EVENT_MISSING/OUT_OF_ORDER/DUPLICATE/DIM_UNMATCHED/PARTITION_LATENCY',
    metric_value        DOUBLE  COMMENT '指标值（比率类为0~1，时延类为分钟）',
    metric_total        BIGINT  COMMENT '指标分母（事件总数/应到节点数等）',
    metric_bad          BIGINT  COMMENT '异常分子',
    threshold_value     DOUBLE  COMMENT '当日生效阈值（来自 pipeline.conf）',
    is_breach           BOOLEAN COMMENT '是否超阈值',
    ingest_batch_id     STRING,
    ingest_time         TIMESTAMP
)
USING iceberg
PARTITIONED BY (biz_date)
COMMENT '数据质量日核对：缺失率/乱序率/重复率/维表未匹配率/分区产出时延'
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS ltl.meta.backfill_job_log (
    job_id              STRING  COMMENT '补数作业ID（入参：运单号或时间区间）',
    scope_type          STRING  COMMENT 'WAYBILL / DATE_RANGE',
    scope_value         STRING  COMMENT '运单号列表 或 [start,end] 区间',
    affected_partitions STRING  COMMENT '实际重跑的 event_date|source_system 分区清单(JSON)',
    rows_before         BIGINT  COMMENT '重放前受影响分区行数',
    rows_after          BIGINT  COMMENT '重放后影子表行数',
    diff_summary        STRING  COMMENT '前后对比结果(JSON：新增/删除/变化/序号重排)',
    shadow_table        STRING  COMMENT '影子表名',
    view_switched       BOOLEAN COMMENT '对比通过后是否已切换视图',
    status              STRING  COMMENT 'RUNNING/DIFF_READY/SWITCHED/ROLLED_BACK/FAILED',
    ingest_batch_id     STRING,
    ingest_time         TIMESTAMP
)
USING iceberg
COMMENT '补数台账：只重跑受影响分区，产出前后对比，禁止整表重算'
TBLPROPERTIES ('format-version' = '2');
