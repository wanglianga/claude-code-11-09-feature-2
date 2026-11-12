# 迁移说明：运单宽表 → 事件型仓库

本文给出从旧宽表（4.2 亿行、三十余个时间列）迁移到四层事件型仓库的**执行顺序、
每步的验证点与回滚点**。所有作业均为幂等设计，可安全重跑。

## 0. 目标架构与目录

| 层 | 表/视图 | 职责 |
|---|---|---|
| ODS | `ods.waybill_event_raw` | 原样保存每次推送，不清洗不合并；分区 `biz_date × source_system` |
| DWD | `dwd.waybill_event_detail` | 去重(event_id)、乱序排序(event_seq)、重放、维表代理键 |
| DIM_STG | `dim_stg.*_stg` | 主数据每日快照落地 |
| DIM | `dim.dim_hub / dim_courier / dim_route / dim_customer` | SCD2 拉链，`[eff_date, exp_date)` |
| APP | `app.v_*` | 明细层之上的投影视图，含旧宽表兼容视图 |
| META | `meta.watermark_log / dq_daily_result / backfill_job_log` | 水位线、质量、补数台账 |

## 1. 环境准备（回滚点 R0：未动任何生产数据）

1. 准备 Spark 3.5 + Iceberg 1.8（`docker/Dockerfile` 已固化版本）。
2. 生产 Hive Metastore 建库命名空间：执行 `sql/00_setup.sql`。
3. 配置 `conf/pipeline.conf`：`catalog.type=hive`、`catalog.uri=thrift://<metastore>:9083`。
   - 本地/无 Metastore 时 `catalog.uri` 留空，自动用嵌入式 Derby（仅演示/测试，禁止生产）。
4. 建仓：`spark-submit jobs/ddl_init.py --conf conf/pipeline.conf`
   - 幂等：`CREATE TABLE IF NOT EXISTS`、`ADD COLUMN IF NOT EXISTS`、视图 `DROP+CREATE`。
- **回滚点 R0**：此时只新建了空表/空库，直接 `DROP NAMESPACE` 即可完全撤销。

## 2. 维表历史初始化（先于事实，保证代理键可解析）

1. 把网点/派件员/路由/客户的**历史主数据**整理成每日快照，落地到
   `${landing.dim.dir}/biz_date=<date>/<dim>.jsonl`。
2. 按快照日从最早到最新依次执行：
   ```bash
   spark-submit jobs/dim_stg_load.py  --snapshot-date <date> --dim-name all
   spark-submit jobs/dim_scd2_job.py --snapshot-date <date> --dim-name all
   ```
3. 代理键由自然键稳定生成（`xxhash64` 取正），跨版本不变。
- **验证点**：每个自然键的版本区间 `[eff_date, exp_date)` 连续不重叠；
  `is_current=true` 仅一条；改名/合并/换片区均产生新版本而非覆盖。
- **回滚点 R1**：维表独立于事实，重建只需 `TRUNCATE` 四张 dim 表后按快照重跑。

## 3. 历史宽表一次性迁移（迁移期专用）

1. 旧宽表结构登记在 `sql/08_legacy_source.sql`，数据装载到 `ods.legacy_waybill_wide`。
2. 执行逆透视迁移（按时间区间分批，建议按月）：
   ```bash
   spark-submit jobs/legacy_migrate_job.py --start-date 2023-01-01 --end-date 2026-11-10
   ```
   - 每个非空时间列 → 一条 ODS 事件，`event_id=LEGACY_<单号>_<类型>`，天然幂等。
   - 随后复用 `dwd_detail_job` 的同一套重放逻辑生成明细，口径与新数据完全一致。
- **能力边界（务必告知业务）**：宽表只存“最后一次异常”，历史多次异常链**无法还原**；
  迁移后新产生的 EXCEPTION 才会完整保留。旧宽表的“最新网点名称”时点也已丢失，
  历史事实只能关联到当前可得的最早维表版本。
- **回滚点 R2**：迁移只写新表，旧宽表全程只读；撤销时删除 `source_system='LEGACY'`
  的 ODS 事件并重放对应运单即可，旧宽表不受任何影响。

## 4. 切换到每日增量（双跑对账期）

1. 部署 Airflow DAG `ltl_daily_incremental`（`orchestration/dags/`）。
   执行链：`ods_load ∥ dim_stg_load → scd2_preview → scd2_apply → dwd_detail → dq_check`。
2. 增量按事件时间水位线 `meta.watermark_log` 每日推进；失败不写 SUCCESS、不推进水位。
3. 旧报表临时双跑：旧宽表口径 vs `app.v_waybill_wide_compat`，按天对账。
- **验证点**：连续 N 天双跑口径一致（差异仅来自宽表已知缺陷：末次异常、最新名称）。
- **回滚点 R3**：DAG 可随时暂停，回退到旧宽表；新表数据保留不删。

## 5. 旧宽表降级为兼容视图、下线原表

1. 对账通过后，旧报表改连 `app.v_waybill_wide_compat`（一行一运单，含末次异常兼容列
   与完整 `exception_chain`）。
2. 全部旧报表切换完成后，再下线物理旧宽表（建议先归档备份再 DROP）。
- **回滚点 R4**：视图与物理表同名切换，保留物理表一个对账周期；
  出问题立即把连接串指回物理表。

## 6. 补数标准操作（独立入口，禁止整表重算）

```bash
# 第一步：只产出前后对比，生产数据零改动（status=DIFF_READY）
spark-submit jobs/backfill_job.py --scope DATE_RANGE \
    --start-date 2026-10-20 --end-date 2026-10-25
# 查看 data/backfill/<job_id>_diff.json 与 meta.backfill_job_log

# 第二步：复核通过后生效（受影响分区原子动态覆盖，无关分区不动）
spark-submit jobs/backfill_job.py --scope DATE_RANGE \
    --start-date 2026-10-20 --end-date 2026-10-25 --switch

# 全量回刷的唯一允许路径：独立表重建 + 换表（旧表保留为 backup 回滚点）
spark-submit jobs/backfill_job.py --scope DATE_RANGE \
    --start-date 2026-10-01 --end-date 2026-11-11 --full-rebuild --switch
```
- 按运单补数：`--scope WAYBILL --waybill-nos WB001,WB002`。
- **回滚点 R5**：常规补数用 Iceberg 快照
  `CALL ltl.system.rollback_to_snapshot('dwd.waybill_event_detail', <snapshot_id>)`；
  全量重建用保留的 `*_backup_*` 表换名回退。

## 7. 组织架构调整（维表变更）治理

1. 调整在主数据快照日生效，`dim_scd2_job.py --preview` 先输出**重算影响预览**：
   受影响键数、快照日及以后受影响事实行数、历史不受影响行数。
2. 确认后正式跑拉链：旧版本 `exp_date=快照日` 闭合，新版本 `eff_date=快照日`。
3. **历史不回刷**：`event_date < 快照日` 的事实永远关联旧版本，去年报表数字不变；
   组织调整只在新日期分区生效。

## 8. Schema 演进规则（强制）

- 只加列、不改类型、不删列；加列与历史回填由 `jobs/schema_evolution_job.py` 统一完成
  （Iceberg 1.8 的 Hadoop/Derby 环境不依赖 SQL 列 DEFAULT）：
  ```bash
  spark-submit jobs/schema_evolution_job.py --apply-builtin
  ```
- 新作业节点（如明年 `STATION_IN`）只是 `event_type` 新枚举，**无需加时间列**。
- 下游视图只做明细层投影，上游加列不影响既有报表（测试 `test_12` 固化此约束）。

## 9. 数据质量与运维

- 每日 `dq_check_job.py` 写 `meta.dq_daily_result`，覆盖：
  事件缺失率、乱序率、重复率、维表未匹配率（四张维表分别评价）、分区产出时延；
  结果带 `source_system × hub_code`，异常可直接定位来源系统与分拨中心。
- 阈值在 `conf/pipeline.conf` 调整，超阈值行 `is_breach=true`。
- Iceberg 定期维护：`expire_snapshots`（保留 30 天，与补数窗口对齐）、
  `rewrite_data_files` 压实小文件。

## 10. 回滚点汇总

| 回滚点 | 阶段 | 回滚动作 |
|---|---|---|
| R0 | 建空仓 | DROP NAMESPACE |
| R1 | 维表初始化 | TRUNCATE dim 表后按快照重跑 |
| R2 | 历史迁移 | 删 LEGACY 事件并重放，旧宽表只读未动 |
| R3 | 双跑期 | 暂停 DAG，连接串指回旧宽表 |
| R4 | 视图切换 | 指回保留的物理旧宽表 |
| R5 | 补数/回刷 | Iceberg 快照回滚 或 backup 表换名 |
