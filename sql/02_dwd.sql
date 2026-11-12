-- =====================================================================
-- 02_dwd.sql  明细数据层（DWD）
--
-- 粒度：运单号 × 事件序号 × 事件类型（一行即一个运单的一次作业事件）。
--
-- 关键机制（对应需求）：
--  1. 去重：按 event_id 去重，重复上报只保留最早入湖的一条（ingest_time 最小）。
--  2. 乱序：事件按业务发生时间 event_time 排序，
--     用 ROW_NUMBER() OVER (PARTITION BY waybill_no ORDER BY event_time, event_id)
--     生成 event_seq；迟到事件到达后重放受影响运单即可重排序号。
--  3. 重放：缺失关键节点在上游补齐后，按运单号或分区重跑即可重建，
--     逻辑全部由作业重算，不依赖旧宽表的"最后状态"。
--  4. 多次异常：EXCEPTION 事件按发生顺序独立成行，不再只保留最后一次。
--  5. 维表只存代理键 + 事件发生时点，禁止存最新名称：
--     hub_sk / courier_sk / route_sk / customer_sk，
--     报表按 event_date 关联当时生效的拉链版本（见 04_dim.sql、05_views.sql）。
--  6. 只加列演进：预约时段 appointment_window、冷链标志 is_cold_chain
--     为今年新增列，均带默认值，旧报表投影不受影响（见 06_evolution.sql）。
-- =====================================================================

CREATE TABLE IF NOT EXISTS ltl.dwd.waybill_event_detail (
    waybill_no        STRING      COMMENT '运单号（粒度键1）',
    event_seq         INT         COMMENT '事件序号：同一运单内按event_time排序生成（粒度键2）',
    event_type        STRING      COMMENT '事件类型（粒度键3）',
    event_id          STRING      COMMENT '来源事件ID，与ODS一致，用于幂等与血缘追溯',
    event_time        TIMESTAMP   COMMENT '业务发生时间',
    event_date        DATE        COMMENT '业务发生日期，时点关联维表的关联键之一',
    report_time       TIMESTAMP   COMMENT '上报时间，乱序率统计依据',
    source_system     STRING      COMMENT '来源系统，DQ下钻维度',

    -- 作业现场属性（来自原始报文解析，只放稳定业务键，不放易变名称）
    origin_hub_code   STRING      COMMENT '发件/揽收网点业务键',
    hub_code          STRING      COMMENT '事件发生分拨/网点业务键（压货定位用）',
    next_hub_code     STRING      COMMENT '下一中转分拨业务键（中转事件）',
    waybill_route_code STRING     COMMENT '路由业务键',
    customer_code     STRING      COMMENT '客户业务键',
    courier_code      STRING      COMMENT '派件员业务键（维表拉链前的自然键）',
    exception_code    STRING      COMMENT '异常代码；多次异常独立保留，不覆盖',
    exception_reason  STRING      COMMENT '异常原因文本（当次事件快照）',

    -- 维表代理键（时点关联入口；关联不上时为 -1，计入维表未匹配率）
    hub_sk            BIGINT      COMMENT '网点/分拨维表代理键',
    courier_sk        BIGINT      COMMENT '派件员维表代理键',
    route_sk          BIGINT      COMMENT '路由维表代理键',
    customer_sk       BIGINT      COMMENT '客户维表代理键',

    -- 今年新增的作业属性（只加列演进；默认值由写入作业 coalesce 保证，
    -- 历史行回填走 jobs/schema_evolution_job.py，明年新节点继续按此规则扩展）
    appointment_window STRING     COMMENT '预约送达时段，如 2026-11-12 09:00-11:00；历史数据回填NA',
    is_cold_chain      BOOLEAN    COMMENT '是否冷链；历史数据回填false',

    ingest_batch_id   STRING      COMMENT '最近一次写入/重放的批次',
    ingest_time       TIMESTAMP   COMMENT '最近一次写入时间'
)
USING iceberg
PARTITIONED BY (event_date, source_system)
COMMENT 'DWD 运单事件明细：去重+乱序排序+重放，多次异常独立保留，仅存维表代理键'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    -- MERGE 写入采用 copy-on-write，保证每日增量与补数重放的幂等
    'write.update.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
);

-- 幂等键：event_id 全局唯一（同运单同事件类型多次发生时上游 event_id 不同）。
-- event_seq 在每次重放受影响运单时整体重算，因此补数后序号连续且可复现。
