-- =====================================================================
-- 08_legacy_source.sql  旧宽表（迁移源）结构登记与迁移中转表
--
-- 旧系统 4.2 亿行宽表的代表性结构（三十余个时间列，此处列出核心列，
-- 实际迁移时按生产库真实列名映射到 jobs/legacy_migrate_job.py 的 COLUMN_MAP）。
-- 本脚本只在迁移期使用；迁移完成后旧宽表以 app.v_waybill_wide_compat
-- 兼容视图替代，原表可下线。
-- =====================================================================

CREATE TABLE IF NOT EXISTS ltl.ods.legacy_waybill_wide (
    waybill_no              STRING,
    customer_code           STRING,
    route_code              STRING,
    pickup_time             TIMESTAMP COMMENT '揽收时间',
    line_haul_depart_time   TIMESTAMP COMMENT '干线发出时间',
    arrive_hub_time         TIMESTAMP COMMENT '到达分拨时间',
    transfer_time_1         TIMESTAMP COMMENT '中转1时间',
    transfer_time_2         TIMESTAMP COMMENT '中转2时间',
    transfer_time_3         TIMESTAMP COMMENT '中转3时间',
    deliver_time            TIMESTAMP COMMENT '派送时间',
    sign_time               TIMESTAMP COMMENT '签收时间',
    reject_time             TIMESTAMP COMMENT '拒收时间',
    pod_time                TIMESTAMP COMMENT '回单时间',
    last_exception_time     TIMESTAMP COMMENT '末次异常时间（宽表只留最后一次）',
    last_exception_reason   STRING    COMMENT '末次异常原因',
    origin_hub_code         STRING,
    last_hub_name           STRING    COMMENT '最新网点名称（时点已丢失）',
    courier_code            STRING,
    appointment_window      STRING,
    is_cold_chain          STRING
)
USING iceberg
PARTITIONED BY (biz_date DATE)
COMMENT '迁移源：旧宽表登记结构（仅迁移期使用）'
TBLPROPERTIES ('format-version' = '2');
