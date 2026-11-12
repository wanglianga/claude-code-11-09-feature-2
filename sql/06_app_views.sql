-- =====================================================================
-- 06_app_views.sql  应用层（APP）与下游投影视图
--
-- 规则（对应需求）：
--   * 下游视图统一建在 DWD+DIM 之上做投影，报表只访问视图，不直接扫明细表。
--   * 所有维表关联严格走时点：event_date ∈ [eff_date, exp_date)，
--     禁止用最新维表回刷历史；关联不上时代理键为 -1，视图列显示为 NULL（UNKNOWN）。
--   * 旧宽表降级为兼容视图 v_waybill_wide_compat，支撑尚未改造的旧报表。
-- 方言：Spark 3.5 SQL（Iceberg 表上的视图）。
-- =====================================================================

-- ---------- 1. 事件富化主视图：时点关联四张维表 ----------
CREATE OR REPLACE VIEW ltl.app.v_waybill_event_enriched AS
SELECT
    d.waybill_no,
    d.event_seq,
    d.event_type,
    d.event_id,
    d.event_time,
    d.event_date,
    d.source_system,
    d.hub_code,
    h.hub_name        AS hub_name_at_event,      -- 事件发生当时的网点名称
    h.region_code     AS region_at_event,
    d.courier_code,
    c.courier_name    AS courier_name_at_event,  -- 当时的派件员
    c.service_area    AS service_area_at_event,  -- 当时负责的片区
    d.waybill_route_code,
    r.route_name      AS route_name_at_event,
    d.customer_code,
    cu.customer_name  AS customer_name_at_event,
    cu.customer_level AS customer_level_at_event,
    d.exception_code,
    d.exception_reason,
    d.appointment_window,
    d.is_cold_chain
FROM ltl.dwd.waybill_event_detail d
LEFT JOIN ltl.dim.dim_hub h
       ON h.hub_sk = d.hub_sk
      AND d.event_date >= h.eff_date AND d.event_date < h.exp_date
LEFT JOIN ltl.dim.dim_courier c
       ON c.courier_sk = d.courier_sk
      AND d.event_date >= c.eff_date AND d.event_date < c.exp_date
LEFT JOIN ltl.dim.dim_route r
       ON r.route_sk = d.route_sk
      AND d.event_date >= r.eff_date AND d.event_date < r.exp_date
LEFT JOIN ltl.dim.dim_customer cu
       ON cu.customer_sk = d.customer_sk
      AND d.event_date >= cu.eff_date AND d.event_date < cu.exp_date;

-- ---------- 2. 分拨压货/中转停留视图：回答"在哪个分拨压了两天" ----------
-- 逐段配对：同一网点(代理键)的 ARRIVE/TRANSFER 与其后第一次 DEPART 配对，
-- 多次中转逐段独立计算；尚未发出的在途件以当前时间估算停留时长。
CREATE OR REPLACE VIEW ltl.app.v_hub_dwell AS
WITH evt AS (
    SELECT waybill_no, event_seq, event_type, event_time, hub_code, hub_sk, source_system
    FROM ltl.dwd.waybill_event_detail
    WHERE event_type IN ('ARRIVE', 'TRANSFER', 'DEPART')
), paired AS (
    SELECT
        a.waybill_no,
        a.hub_code,
        a.hub_sk,
        a.event_time AS arrive_time,
        MIN(d.event_time) AS depart_time,
        a.source_system
    FROM evt a
    LEFT JOIN evt d
      ON d.waybill_no = a.waybill_no
     AND d.hub_sk = a.hub_sk
     AND d.event_type = 'DEPART'
     AND d.event_time > a.event_time
    WHERE a.event_type IN ('ARRIVE', 'TRANSFER')
    GROUP BY a.waybill_no, a.hub_code, a.hub_sk, a.event_time, a.source_system
)
SELECT
    p.waybill_no,
    p.hub_code,
    h.hub_name AS hub_name_at_event,
    p.arrive_time,
    p.depart_time,
    timestampdiff(MINUTE, p.arrive_time, COALESCE(p.depart_time, current_timestamp())) AS dwell_minutes,
    CASE WHEN timestampdiff(HOUR, p.arrive_time, COALESCE(p.depart_time, current_timestamp())) >= 48
         THEN true ELSE false END AS dwell_over_2d_flag,
    p.source_system
FROM paired p
LEFT JOIN ltl.dim.dim_hub h
       ON h.hub_sk = p.hub_sk
      AND to_date(p.arrive_time) >= h.eff_date
      AND to_date(p.arrive_time) <  h.exp_date;

-- ---------- 3. 派件员绩效基础视图（按签收/拒收事件时点关联派件员历史版本）----------
CREATE OR REPLACE VIEW ltl.app.v_courier_performance AS
SELECT
    d.event_date,
    d.courier_sk,
    c.courier_code,
    c.courier_name AS courier_name_at_event,
    c.hub_code     AS courier_hub_at_event,
    c.service_area AS service_area_at_event,
    d.event_type,
    count(*) AS event_cnt,
    sum(CASE WHEN d.event_type = 'SIGN'   THEN 1 ELSE 0 END) AS sign_cnt,
    sum(CASE WHEN d.event_type = 'REJECT' THEN 1 ELSE 0 END) AS reject_cnt
FROM ltl.dwd.waybill_event_detail d
JOIN ltl.dim.dim_courier c
  ON c.courier_sk = d.courier_sk
 AND d.event_date >= c.eff_date AND d.event_date < c.exp_date
WHERE d.event_type IN ('SIGN', 'REJECT', 'DELIVER')
GROUP BY d.event_date, d.courier_sk, c.courier_code, c.courier_name,
         c.hub_code, c.service_area, d.event_type;

-- ---------- 4. 客户投诉/异常归因视图（多次异常独立保留，按时点关联客户版本）----------
CREATE OR REPLACE VIEW ltl.app.v_customer_exception AS
SELECT
    d.event_date,
    d.customer_sk,
    cu.customer_code,
    cu.customer_name  AS customer_name_at_event,
    cu.customer_level AS customer_level_at_event,
    d.waybill_no,
    d.event_seq,
    d.event_time,
    d.exception_code,
    d.exception_reason,
    h.hub_name AS hub_name_at_event
FROM ltl.dwd.waybill_event_detail d
JOIN ltl.dim.dim_customer cu
  ON cu.customer_sk = d.customer_sk
 AND d.event_date >= cu.eff_date AND d.event_date < cu.exp_date
LEFT JOIN ltl.dim.dim_hub h
  ON h.hub_sk = d.hub_sk
 AND d.event_date >= h.eff_date AND d.event_date < h.exp_date
WHERE d.event_type = 'EXCEPTION';

-- ---------- 5. 旧宽表兼容视图（应用层，一行一运单）----------
-- 透视旧宽表的关键时间列；异常既给"末次异常"兼容列，也给按序号排序的完整异常链。
CREATE OR REPLACE VIEW ltl.app.v_waybill_wide_compat AS
WITH base AS (
    SELECT
        waybill_no,
        min(customer_code)       AS customer_code,
        min(waybill_route_code)  AS route_code,
        min(CASE WHEN event_type = 'PICKUP'   THEN event_time END) AS pickup_time,
        min(CASE WHEN event_type = 'DEPART'   THEN event_time END) AS line_haul_depart_time,
        min(CASE WHEN event_type = 'ARRIVE'   THEN event_time END) AS arrive_hub_time,
        min(CASE WHEN event_type = 'TRANSFER' THEN event_time END) AS first_transfer_time,
        min(CASE WHEN event_type = 'DELIVER'  THEN event_time END) AS deliver_time,
        min(CASE WHEN event_type = 'SIGN'     THEN event_time END) AS sign_time,
        min(CASE WHEN event_type = 'REJECT'   THEN event_time END) AS reject_time,
        min(CASE WHEN event_type = 'POD'      THEN event_time END) AS pod_time,
        max(CASE WHEN event_type = 'EXCEPTION' THEN event_time END) AS last_exception_time,
        sort_array(collect_list(
            CASE WHEN event_type = 'EXCEPTION'
                 THEN named_struct('event_seq', event_seq,
                                   'event_time', event_time,
                                   'exception_code', exception_code,
                                   'exception_reason', exception_reason)
            END)) AS exception_chain_raw,
        -- 聚合时忽略哨兵 'NA'，避免字符串 max 被 'NA' 污染；全为 NA 时回退 'NA'
        coalesce(max(CASE WHEN appointment_window <> 'NA'
                          THEN appointment_window END), 'NA') AS appointment_window,
        CASE WHEN max(CASE WHEN is_cold_chain THEN 1 ELSE 0 END) = 1
             THEN 'true' ELSE 'false' END AS is_cold_chain
    FROM ltl.dwd.waybill_event_detail
    GROUP BY waybill_no
)
SELECT
    waybill_no,
    customer_code,
    route_code,
    pickup_time,
    line_haul_depart_time,
    arrive_hub_time,
    first_transfer_time,
    deliver_time,
    sign_time,
    reject_time,
    pod_time,
    last_exception_time,
    -- 末次异常原因（兼容旧宽表的"最后一次异常原因"列，口径来自有序事件链而非覆盖写）
    element_at(filter(exception_chain_raw, x -> x IS NOT NULL), -1).exception_reason
        AS last_exception_reason,
    -- 完整异常链：多次异常按 event_seq 排序独立保留
    filter(exception_chain_raw, x -> x IS NOT NULL) AS exception_chain,
    appointment_window,
    is_cold_chain
FROM base;

-- DQ 结果浏览视图
CREATE OR REPLACE VIEW ltl.app.v_dq_daily AS
SELECT * FROM ltl.meta.dq_daily_result;
