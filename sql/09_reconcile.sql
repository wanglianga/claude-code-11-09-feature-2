-- =====================================================================
-- 09_reconcile.sql  多来源可信度 / 交叉校验 / 冲突裁决 / 口径版本层
--
-- 背景：同一运单的同一作业节点可能同时存在四个来源的观测：
--   SCANNER 扫描枪 / HUB 分拨系统 / ROUTE 第三方路由回调 / CS 客服补录
-- （LEGACY 为旧宽表迁移来源，可信度排在最后）。
-- 各来源的事件时间与状态经常互相矛盾，本文件定义治理该矛盾的六张表/两个视图。
--
-- 治理链路（对应需求）：
--   1. 来源优先级定主记录：dwd.waybill_node_master
--      优先级在 conf/pipeline.conf 的 reconcile.source.priority 配置，
--      缺省 SCANNER > HUB > ROUTE > CS > LEGACY。
--   2. 关键节点交叉校验：同运单同节点多来源观测做时间容差与状态一致性校验，
--      超容差即生成冲突条目 meta.waybill_conflict，原始记录在 ODS/DWD 全部保留，
--      冲突条目记录冲突字段、各来源取值、差异秒数与推断的可能原因。
--   3. 待核查队列：meta.waybill_review_queue
--      存在未裁决(OPEN)冲突的运单进 PENDING 队列；冲突未裁决前，
--      该运单不进入时效统计（app.waybill_timeliness），只能在队列中等待人工核查。
--   4. 人工裁决与规则沉淀：meta.adjudication_rule
--      人工裁决先作用于当前冲突条目；"沉淀为规则"只生成 DRAFT 规则，
--      规则必须随下一个口径版本(meta.calibre_version)发布后才对后续作业生效。
--   5. 已发布报表不可变：app.waybill_timeliness 按 calibre_version 分区追加，
--      同版本已出报运单幂等跳过；人工裁决只对裁决时间之后发布的版本生效，
--      裁决与新规则只影响新版本的统计，已 PUBLISHED 版本的统计行不可变。
-- 表格式：Iceberg v2（MERGE 幂等写入、行级更新依赖它）。
-- =====================================================================

-- ---------- 1. 关键节点主记录表（运单 × 关键节点 × 选定的主来源）----------
-- 一个节点被多个来源观测时只选一条主记录，但所有来源观测仍在 DWD/ODS 完整保留，
-- 并通过 meta.waybill_conflict.source_values 可回溯每个来源的取值。
CREATE TABLE IF NOT EXISTS ltl.dwd.waybill_node_master (
    waybill_no            STRING      COMMENT '运单号',
    event_type            STRING      COMMENT '关键节点事件类型 PICKUP/DEPART/ARRIVE/TRANSFER/DELIVER/SIGN/REJECT',
    master_event_id       STRING      COMMENT '主记录事件ID（按来源优先级选出）',
    master_source_system  STRING      COMMENT '主记录来源系统',
    master_event_time     TIMESTAMP   COMMENT '主记录业务发生时间（时效统计取该值）',
    master_report_time    TIMESTAMP   COMMENT '主记录上报时间',
    source_count          INT         COMMENT '观测到该节点的来源数（>=2 才可能触发交叉校验）',
    source_systems        STRING      COMMENT '全部观测来源JSON数组，如 ["SCANNER","HUB","ROUTE"]',
    conflict_flag         BOOLEAN     COMMENT '本次计算时是否存在超容差且未裁决的冲突',
    open_conflict_count   INT         COMMENT '该节点未裁决冲突条目数',
    adjudicated_flag      BOOLEAN     COMMENT '节点冲突是否已被人工/规则裁决',
    calibre_version       STRING      COMMENT '本次计算使用的口径版本',
    biz_date              DATE        COMMENT '主记录业务日期=to_date(master_event_time)，分区键',
    ingest_batch_id       STRING      COMMENT '最近一次写入批次',
    ingest_time           TIMESTAMP   COMMENT '最近一次写入时间'
)
USING iceberg
PARTITIONED BY (biz_date)
COMMENT 'DWD 关键节点主记录：来源优先级定主，多来源观测不合并只标注'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.update.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
);

-- ---------- 2. 冲突条目表：一次节点校验不通过 = 一条冲突 ----------
-- 幂等键 conflict_id = sha2(waybill_no|event_type|conflict_field)，
-- 重跑只刷新 last_seen，不改变 OPEN/ADJUDICATED 状态，裁决信息永不被作业覆盖。
CREATE TABLE IF NOT EXISTS ltl.meta.waybill_conflict (
    conflict_id                 STRING      COMMENT '冲突确定性ID: sha2(waybill_no|event_type|conflict_field)',
    waybill_no                  STRING      COMMENT '运单号',
    event_type                  STRING      COMMENT '冲突节点；终态状态冲突记 TERMINAL',
    conflict_field              STRING      COMMENT '冲突字段：EVENT_TIME 时间矛盾 / EVENT_STATUS 状态矛盾',
    master_event_id             STRING      COMMENT '主记录事件ID',
    master_source_system        STRING      COMMENT '主记录来源（优先级最高者）',
    master_value                STRING      COMMENT '主记录取值：时间字符串或状态码',
    source_values               STRING      COMMENT '各来源取值JSON数组：source_system/event_id/event_time/report_time/field_value/delta_seconds_vs_master 全量保留',
    max_diff_seconds            BIGINT      COMMENT '各来源取值两两最大差异秒数（EVENT_TIME）；状态冲突为 NULL',
    master_diff_seconds         BIGINT      COMMENT '其他来源与主记录的最大差异秒数',
    tolerance_seconds           BIGINT      COMMENT '判定时适用的时间容差秒数（来自 pipeline.conf）',
    reason_code                 STRING      COMMENT '推断原因码：CLOCK_SKEW/MANUAL_BACKFILL_LAG/ROUTE_CALLBACK_DELAY/OUTLIER_SOURCE/STATUS_MISRECORD',
    possible_reason             STRING      COMMENT '可能原因的中文说明，供核查人员参考（非定论）',
    outlier_source_system       STRING      COMMENT '偏离主记录最远的来源系统',
    status                      STRING      COMMENT 'OPEN 未裁决 / ADJUDICATED 已裁决 / INVALID 数据修正后冲突消失',
    adjudicated_value           STRING      COMMENT '人工裁决后的最终取值',
    adjudicated_event_id        STRING      COMMENT '人工裁决选定的来源事件ID（空表示未采用任一来源事件）',
    adjudicated_by              STRING      COMMENT '裁决人',
    adjudicated_time            TIMESTAMP   COMMENT '裁决时间',
    adjudication_note           STRING      COMMENT '裁决备注',
    rule_id                     STRING      COMMENT '若由已发布规则自动裁决，记录规则ID；人工裁决为空',
    effective_calibre_version   STRING      COMMENT '裁决生效口径版本：规则裁决=规则生效版本；人工/INVALID 置 NULL，语义为裁决时间之后发布的下一个版本',
    calibre_version_at_detect   STRING      COMMENT '冲突首次发现时的口径版本',
    detect_date                 DATE        COMMENT '首次发现日期，分区键',
    first_detected_batch_id     STRING      COMMENT '首次发现批次',
    first_detected_time         TIMESTAMP   COMMENT '首次发现时间',
    last_seen_batch_id          STRING      COMMENT '最近一次仍观测到该冲突的批次',
    last_seen_time              TIMESTAMP   COMMENT '最近一次观测时间',
    ingest_time                 TIMESTAMP
)
USING iceberg
PARTITIONED BY (detect_date)
COMMENT 'META 多来源冲突条目：保留全部来源取值与差异秒数，裁决状态独立维护'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 3. 待核查队列：运单级，只要有一条 OPEN 冲突即 PENDING ----------
-- 时效统计作业以本表为唯一排除依据：queue_status='PENDING' 的运单一律不参与统计。
CREATE TABLE IF NOT EXISTS ltl.meta.waybill_review_queue (
    waybill_no              STRING      COMMENT '运单号（队列主键）',
    queue_status            STRING      COMMENT 'PENDING 待核查 / RESOLVED 冲突已全部裁决或消失',
    open_conflict_count     INT         COMMENT '当前未裁决冲突数',
    conflict_ids            STRING      COMMENT '关联冲突ID的JSON数组',
    conflict_fields         STRING      COMMENT '冲突字段JSON数组，如 ["EVENT_TIME","EVENT_STATUS"]',
    reason_codes            STRING      COMMENT '原因码JSON数组',
    first_seen_time         TIMESTAMP   COMMENT '首次进入队列时间',
    last_seen_time          TIMESTAMP   COMMENT '最近刷新时间',
    resolved_time           TIMESTAMP   COMMENT '全部冲突解决的时间（重新 PENDING 时清空）',
    resolved_by             STRING      COMMENT '解决操作人（人工裁决/规则ID）',
    current_calibre_version STRING      COMMENT '最近一次刷新使用的口径版本',
    ingest_batch_id         STRING,
    ingest_time             TIMESTAMP
)
USING iceberg
COMMENT 'META 冲突待核查队列：未裁决运单只进队列，不进时效统计'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 4. 裁决规则库：人工裁决沉淀，DRAFT 不生效，随版本发布 ----------
-- 安全约束：DRAFT 规则对任何在跑作业不可见；只有 version_status=PUBLISHED
-- 且 effective_calibre_version 已回填后，reconcile 作业才在新版本中应用它。
CREATE TABLE IF NOT EXISTS ltl.meta.adjudication_rule (
    rule_id                    STRING      COMMENT '规则ID，如 RULE_0001',
    rule_status                STRING      COMMENT 'DRAFT 草稿 / PUBLISHED 已发布生效 / DISABLED 停用',
    event_type                 STRING      COMMENT '匹配节点；TERMINAL 表示终态节点',
    conflict_field             STRING      COMMENT '匹配冲突字段 EVENT_TIME/EVENT_STATUS',
    outlier_source_system      STRING      COMMENT '匹配的离群来源（可空，空表示不限制）',
    reason_code                STRING      COMMENT '匹配原因码（可空）',
    match_pattern              STRING      COMMENT '完整匹配条件JSON（来源/节点/字段/原因码），便于审计',
    decision                   STRING      COMMENT '裁决动作：PREFER_SOURCE 采用指定来源 / DISMISS 驳回冲突',
    decision_value             STRING      COMMENT '动作参数JSON，如 {"prefer_source":"SCANNER"}',
    origin_conflict_id         STRING      COMMENT '沉淀该规则的原始冲突条目ID',
    created_by                 STRING      COMMENT '创建人（裁决人）',
    created_time               TIMESTAMP,
    effective_calibre_version  STRING      COMMENT '生效口径版本；DRAFT 期间为 NULL，发布时回填，只能向后生效',
    published_time             TIMESTAMP,
    note                       STRING      COMMENT '规则说明'
)
USING iceberg
COMMENT 'META 裁决规则：人工经验沉淀，只在下一个口径版本生效，不回刷已发布报表'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 5. 口径版本登记 ----------
-- 任何口径/规则变更必须发新版本；已 PUBLISHED 版本行与对应时效统计行均不可变。
CREATE TABLE IF NOT EXISTS ltl.meta.calibre_version (
    calibre_version    STRING      COMMENT '口径版本号，如 v2026.11.0',
    version_status     STRING      COMMENT 'DRAFT / PUBLISHED',
    effective_date     DATE        COMMENT '生效日期（发布时指定）',
    parent_version     STRING      COMMENT '上一个版本号，形成版本链',
    rule_ids           STRING      COMMENT '本版本携带发布的规则ID JSON数组',
    change_summary     STRING      COMMENT '版本变更说明',
    published_by       STRING,
    published_time     TIMESTAMP,
    created_time       TIMESTAMP
)
USING iceberg
COMMENT 'META 口径版本：规则与统计口径的版本边界，已发布版本不可变'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 6. 时效统计结果表（按口径版本追加，已发布行不可变）----------
-- 只收录不在待核查队列中的运单；按 calibre_version 分区，
-- 作业只发 INSERT 不做 UPDATE/DELETE：同版本已出报运单计算前即剔除（幂等跳过），
-- 新业务日期闭环的新运单持续追加；规则变更必须发新版本重算。
CREATE TABLE IF NOT EXISTS ltl.app.waybill_timeliness (
    calibre_version      STRING      COMMENT '统计所依据的口径版本（分区键）',
    waybill_no           STRING      COMMENT '运单号',
    pickup_time          TIMESTAMP   COMMENT '揽收主记录时间',
    deliver_time         TIMESTAMP   COMMENT '派送主记录时间',
    sign_time            TIMESTAMP   COMMENT '签收主记录时间（拒收运单为 NULL）',
    reject_time          TIMESTAMP   COMMENT '拒收主记录时间（签收运单为 NULL）',
    transit_hours        DOUBLE      COMMENT '全程时效：PICKUP→SIGN 小时数，保留2位小数',
    deliver_hours        DOUBLE      COMMENT '干线时效：PICKUP→DELIVER 小时数',
    sign_leg_hours       DOUBLE      COMMENT '末端时效：DELIVER→SIGN 小时数',
    node_sources         STRING      COMMENT '各节点主来源JSON，如 {"PICKUP":"SCANNER","SIGN":"HUB"}，可血缘追溯',
    conflict_free_flag   BOOLEAN     COMMENT '入库时该运单无未裁决冲突（恒为 true，队列 PENDING 运单不允许入库）',
    calc_batch_id        STRING,
    calc_time            TIMESTAMP
)
USING iceberg
PARTITIONED BY (calibre_version)
COMMENT 'APP 时效统计：按口径版本不可变追加，冲突未裁决运单被排除'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 7. 应用层浏览视图 ----------

-- 待核查队列浏览器：队列行 + 运单未裁决冲突条数/字段（客服核查工作台的数据源）
CREATE OR REPLACE VIEW ltl.app.v_waybill_review_queue AS
SELECT
    q.waybill_no,
    q.queue_status,
    q.open_conflict_count,
    q.conflict_ids,
    q.conflict_fields,
    q.reason_codes,
    q.first_seen_time,
    q.last_seen_time,
    q.resolved_time,
    q.resolved_by,
    q.current_calibre_version
FROM ltl.meta.waybill_review_queue q;

-- 未裁决冲突明细浏览（保留各来源取值与差异秒数，供人工核查页面/查询直接消费）
CREATE OR REPLACE VIEW ltl.app.v_waybill_conflict_open AS
SELECT
    conflict_id, waybill_no, event_type, conflict_field,
    master_source_system, master_value, source_values,
    max_diff_seconds, master_diff_seconds, tolerance_seconds,
    reason_code, possible_reason, outlier_source_system,
    status, calibre_version_at_detect, detect_date,
    first_detected_time, last_seen_time
FROM ltl.meta.waybill_conflict
WHERE status = 'OPEN';

-- 已发布时效统计：只暴露 PUBLISHED 版本；DRAFT 版本对外不可见
CREATE OR REPLACE VIEW ltl.app.v_timeliness_published AS
SELECT t.*
FROM ltl.app.waybill_timeliness t
JOIN ltl.meta.calibre_version v
  ON v.calibre_version = t.calibre_version
WHERE v.version_status = 'PUBLISHED';
