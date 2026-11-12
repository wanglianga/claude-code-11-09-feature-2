# 零担物流事件型数据仓库（LTL Event Warehouse）

把"三十多个时间列、4.2 亿行"的运单宽表改造为**四层事件型仓库**：原始层原样保存每次
推送，明细层以"运单 × 事件序号 × 事件类型"为粒度并支持乱序/去重/重放，维表独立拉链
按时点关联，应用层以视图替代旧宽表。本仓库**没有业务界面、没有常驻服务**，交付物是
建表语句、Spark 批处理作业、Airflow 编排、补数脚本与迁移说明。

## 1. 解决的核心问题

| 旧宽表痛点 | 本方案对应机制 |
|---|---|
| 只留最后一次异常，无法回答"在哪个分拨压了两天" | 事件明细逐行保留多次异常；`app.v_hub_dwell` 逐段计算分拨停留，>48h 打标 |
| 网点改名/派件员换片区，用今年架构重算去年报表对不上 | 4 张 SCD2 拉链维表 + 严格时点关联，禁止最新维表回刷历史 |
| 上游丢包/断连/回调丢失要补 30 天数据，一改带偏整月汇总 | 独立补数入口，按单号/区间只重跑受影响分区，影子表对比后受控切换 |
| 新增预约时段、冷链、明年新作业节点，加字段阻力大 | 只加列+默认值回填的演进规则；新节点只是 `event_type` 新枚举，无需加时间列 |
| 同一运单四来源（扫描枪/分拨/路由/客服补录）时间与状态互相矛盾 | 来源可信度优先级定主记录 + 关键节点交叉校验，冲突条目保留全部来源取值与差异秒数；未裁决只进待核查队列、不进时效统计；裁决规则随下一口径版本生效，已发布报表不可变 |

## 2. 分层与目录结构

```
sql/                         四层建表与视图（Spark 3.5 SQL + Iceberg 1.8）
  00_setup.sql               命名空间
  01_ods.sql                 ODS 原始事件表（分区 biz_date × source_system）
  02_dwd.sql                 DWD 事件明细表（去重/乱序序号/代理键）
  03_dim_stg.sql             维表每日快照落地
  04_dim.sql                 网点/派件员/路由/客户 4 张 SCD2 拉链维表
  05_meta.sql                水位线 / DQ 结果 / 补数台账
  06_app_views.sql           应用层视图（含旧宽表兼容视图、压货、绩效、投诉归因）
  07_evolution.sql           Schema 演进规则参考（实际由作业幂等执行）
  08_legacy_source.sql       旧宽表迁移源登记
  09_reconcile.sql           多来源定主/冲突条目/待核查队列/裁决规则/口径版本/时效表
jobs/                        PySpark 作业
  ddl_init.py                一键建仓（幂等）
  ods_load.py                原始接入（原样保存、文件级幂等）
  dwd_detail_job.py          去重/乱序排序/重放/代理键解析
  reconcile_job.py           来源优先级定主 + 关键节点交叉校验 + 冲突/队列 + 已发布规则自动裁决
  adjudication_job.py        人工裁决（--promote-rule 沉淀为 DRAFT 规则，下版本才生效）
  calibre_job.py             口径版本发布：DRAFT 规则打包进下一个 PUBLISHED 版本
  timeliness_job.py          时效统计：排除待核查运单，按口径版本只追加、禁止重写
  print_conflict_id.py       冲突ID查询助手（人工裁决命令行取数用）
  dim_stg_load.py            维表快照落地
  dim_scd2_job.py            SCD2 拉链（--preview 出重算影响预览）
  backfill_job.py            补数（影子表+前后对比+受控切换/回滚）
  dq_check_job.py            五类质量指标按天×来源×分拨
  schema_evolution_job.py    只加列+历史回填（幂等）
  legacy_migrate_job.py      旧宽表时间列逆透视迁移
orchestration/dags/          Airflow DAG（每日增量、手动补数、冲突治理）
docker/                      Dockerfile + entrypoint
docker-compose.yml           一键启动 Airflow+Spark
conf/pipeline.conf           全局配置（catalog、阈值、目录）
scripts/run_local.sh         本地无 Docker 一键验证
tests/                       夹具生成、26 项端到端断言、旧宽表迁移冒烟
docs/migration_guide.md      迁移步骤、执行顺序与回滚点（R0–R5）
```

## 3. 技术栈与 Catalog

- Spark 3.5（PySpark / Spark SQL）+ Iceberg 1.8（format-version=2，行级 MERGE/OVERWRITE）。
- Catalog（`conf/pipeline.conf` 的 `catalog.type`）：
  - **jdbc（本地默认）**：Iceberg JdbcCatalog + H2 文件库，免外部依赖且**支持视图**；
  - **hive（生产推荐）**：`catalog.uri=thrift://<metastore>:9083`；
  - **hadoop**：极简模式，元数据随表目录，不支持视图。
- 编排：Airflow 2.9（本地 standalone；生产可换 DolphinScheduler，作业命令不变）。

> 项目类型说明：本项目是**纯批处理数据仓库工程**，没有 HTTP 接口、没有常驻业务服务、
> 也没有前端页面（与需求"最终没有界面也没有常驻服务"一致），因此不提供
> OpenAPI/Swagger UI；对外的"服务"就是 Airflow 编排下的批处理作业与可查询的表/视图。

## 4. 一键启动（Docker，推荐）

前置：Docker 24+、Compose v2。

```bash
docker compose up -d --build
# Airflow UI: http://localhost:8080
```

容器首次启动自动执行 `ddl_init.py` 建仓。两个服务：
- `ltl-airflow`：Airflow standalone（含本地 Spark，DAG 通过 spark-submit 调作业）；
- `ltl-spark`（profiles=tools）：手动跑作业的一次性容器，例如
  ```bash
  docker compose run --rm ltl-spark \
    spark-submit /opt/ltl/jobs/ddl_init.py --conf /opt/ltl/conf/pipeline.conf
  ```

### 测试账号

| 系统 | 地址 | 用户名 | 密码 |
|---|---|---|---|
| Airflow UI | http://localhost:8080 | `ltl_admin` | `ltl_admin_2026` |

账号可通过 `.env` 或 compose 的 `AIRFLOW_USER/AIRFLOW_PASSWORD` 覆盖。
本项目无业务前端、无常驻 API 服务，因此不需要其他账号；Airflow 仅作编排器。

## 5. 本地开发与验证（无需 Docker）

前置：Python 3.11、JDK 17、Spark 3.5（脚本默认取 `/tmp/spark-3.5.9-bin-hadoop3`，
可用 `SPARK_HOME` 覆盖），Iceberg/H2 jar 已放入 `$SPARK_HOME/jars`（版本见 Dockerfile）。

```bash
python3 -m venv .venv && .venv/bin/pip install duckdb pytest
bash scripts/run_local.sh
```

该脚本会：生成夹具 → 建仓 → 两期维表拉链 → 四天事件增量 → DQ →
11-05 迟到事件补数 → 多来源交叉校验 → 首版时效出报 → 人工裁决/规则沉淀/
口径版本发布/新版本出报 → 跑 `pytest`。结束打印 `ALL LOCAL STEPS DONE` 与
`26 passed`（`test_pipeline.py` 13 项 + `test_reconcile.py` 13 项）即通过。

也可单步执行（均为幂等）：

```bash
export SPARK_HOME=/path/to/spark PYTHONPATH=$PWD/jobs \
       PIPELINE_CONF=$PWD/conf/pipeline.local.conf
$SPARK_HOME/bin/spark-submit --master local[2] jobs/ddl_init.py            --conf $PIPELINE_CONF
$SPARK_HOME/bin/spark-submit --master local[2] jobs/dim_scd2_job.py        --conf $PIPELINE_CONF --snapshot-date 2026-11-02 --preview
$SPARK_HOME/bin/spark-submit --master local[2] jobs/backfill_job.py        --conf $PIPELINE_CONF --scope WAYBILL --waybill-nos WB006 --switch
```

## 6. 验证方式与覆盖点

`tests/test_pipeline.py` 的 13 项断言（已在本地 Spark+Iceberg 实际执行通过）覆盖：

1. ODS 原样保存重复上报与原始报文，不清洗不合并；
2. DWD 按 `event_id` 去重；
3. 乱序事件按**业务发生时间**而非上报时间生成连续 `event_seq`；
4. 多次异常独立保留，兼容视图末次原因取最后一次、异常链完整；
5. 缺失节点迟到补报后整单重放、序号重排、业务日期仍取事件时间；
6. SCD2 时点关联：改名/换片区/客户降级前后，历史事实关联到当时版本；合并撤销不物理删除；
7. 分拨压货 >48 小时可定位（`v_hub_dwell`，带当时网点名）；
8. 未知网点代理键 = -1；
9. DQ 五类指标齐全、可下钻到来源系统×分拨、超阈值带 breach；
10. 补数只动受影响运单，台账含分区清单/前后对比/差异，对比文件落盘；
11. 水位线按 ODS/DWD 分层按日推进；
12. Schema 只加列+回填，既有视图与报表不受影响、加列幂等；
13. 旧宽表兼容视图一行一运单、关键时间列与新列正确透视。

另有 `tests/smoke_legacy_migration.py` 验证旧宽表逆透视迁移与重跑幂等；
`tests/test_reconcile.py` 的 13 项多来源治理断言见第 9 节。

> 说明：以上结论均来自本机实际执行（Spark 3.5.9 + Iceberg 1.8.1 + H2 JdbcCatalog）。
> 未在本环境构建/启动 Docker 镜像（沙箱无 Docker daemon），Dockerfile/compose 已按
> 相同版本与命令编写，需在具备 Docker 的环境执行第 4 节验证。

## 7. 关键作业口径速查

- **增量**：`meta.watermark_log` 记事件时间水位，每日推进；失败不写 SUCCESS、不推进。
- **补数**：默认只产出对比（`DIFF_READY`），加 `--switch` 才生效；
  全量回刷只能 `--full-rebuild --switch`（独立表重建+换表，旧表留 backup）。
- **维表变更**：先 `--preview` 看重算影响（历史不受影响行数），再正式拉链；
  调整只在快照日新分区生效。
- **质量阈值**：`conf/pipeline.conf` 中 `dq.threshold.*`。

迁移的完整执行顺序与回滚点见 **[docs/migration_guide.md](docs/migration_guide.md)**（R0–R5）。

## 8. 多来源可信度与冲突裁决（四来源矛盾治理）

同一运单的同一节点可能被四个来源同时观测：`SCANNER` 扫描枪、`HUB` 分拨系统、
`ROUTE` 第三方路由回调、`CS` 客服补录（`LEGACY` 旧宽表来源排末位）。
治理链路分五步，全部由批处理作业与 Iceberg 表承载，无常驻服务：

1. **来源优先级定主记录**（`reconcile_job.py` → `dwd.waybill_node_master`）
   优先级在 `conf/pipeline.conf` 的 `reconcile.source.priority` 配置，
   缺省 `SCANNER(10) > HUB(20) > ROUTE(30) > CS(40) > LEGACY(90)`；
   同运单同节点取优先级最高来源为主记录，并列时按事件时间/事件ID兜底保证可复现。
   **多来源观测不合并**，ODS/DWD 原始事件一行不删。
2. **关键节点交叉校验**（`meta.waybill_conflict`）
   - `EVENT_TIME` 时间矛盾：同节点多来源事件时间两两最大差异超过
     `reconcile.time.tolerance.seconds`（缺省 300 秒）即冲突，记录
     各来源取值 JSON、与主记录的差异秒数、两两最大差异秒数、容差和离群来源；
   - `EVENT_STATUS` 状态矛盾：终态 `SIGN`/`REJECT` 互斥状态同时出现；
   - `possible_reason` 按离群来源与差异量级推断（客服补录滞后、路由回调延迟、
     时钟偏差、单点离群、终态误录），只作核查线索而非定论。
3. **待核查队列**（`meta.waybill_review_queue`，视图 `app.v_waybill_review_queue`）
   存在 OPEN 冲突的运单置 `PENDING`。**冲突未裁决前该运单不进入时效统计**，
   `timeliness_job.py` 以队列为第一排除依据；数据修正使冲突条件消失时，
   重跑自动把冲突置 `INVALID`、队列置 `RESOLVED`。
4. **人工裁决与规则沉淀**（`adjudication_job.py` → `meta.adjudication_rule`）
   裁决只改冲突表裁决列（采信某来源取值或驳回），可 `--promote-rule`
   沉淀为 **DRAFT** 规则。DRAFT 规则对 reconcile 作业不可见。
5. **口径版本发布与不可变报表**（`calibre_job.py` + `app.waybill_timeliness`）
   DRAFT 规则只能随下一个 PUBLISHED 口径版本（`meta.calibre_version`）生效；
   冲突表的 `effective_calibre_version` 记录裁决生效边界——规则裁决取规则生效版本，
   人工裁决置空（语义为裁决时间之后发布的下一个版本）。时效统计除排除 PENDING
   运单外，还按该边界过滤：**在某版本发布之后做出的裁决，重算该旧版本时仍被拦截**，
   已发布报表不会被后续裁决改变。统计按 `calibre_version` 分区**只追加不重写**：
   作业只发 INSERT 不做 UPDATE/DELETE，同一版本下已出报运单重跑自动幂等跳过，
   新日期闭环的新运单持续追加。已发布结果通过 `app.v_timeliness_published` 视图对外，
   DRAFT 版本对外不可见。

常用治理命令：

```bash
# 每日增量后自动执行交叉校验；也可按运单/区间手动重算（幂等）
spark-submit jobs/reconcile_job.py --scope WAYBILL --waybill-nos WB010
# 查看 OPEN 冲突（含冲突字段、差异秒数、各来源取值、可能原因）
spark-sql -e "SELECT * FROM ltl.app.v_waybill_conflict_open WHERE waybill_no='WB010'"
# 人工裁决：采信扫描枪，并把该裁决沉淀为 DRAFT 规则
CID=$(spark-submit jobs/print_conflict_id.py --waybill-no WB010 | tail -1)
spark-submit jobs/adjudication_job.py --conflict-id "$CID" \
    --decision PREFER_SOURCE --prefer-source SCANNER --by zhangsan --promote-rule
# 发布下一口径版本（DRAFT 规则此时才生效），随后按新版本出报
spark-submit jobs/calibre_job.py --effective-date 2026-11-10 --by data_governance
spark-submit jobs/timeliness_job.py
```

## 9. 验证方式与覆盖点（多来源治理）

`tests/test_reconcile.py` 的 13 项断言（编号 14–26，与 `test_pipeline.py` 接续）覆盖：

14. 来源优先级定主记录（四来源 SIGN 主记录取 SCANNER）；
15. 冲突运单的全部来源原始记录在 ODS/DWD 完整保留；
16. 时间冲突条目差异秒数/容差/离群来源/可能原因/确定性冲突ID齐全；
17. 签收/拒收终态互斥状态冲突，人工采信扫描枪 SIGN；
18. 容差内（180s ≤ 300s）差异不生成冲突；
19. 待核查队列 PENDING→RESOLVED 生命周期；
20. 首版时效统计排除全部未裁决运单，正常运单时效与节点主来源可追溯；
21. 已发布版本不可变：同版本重跑幂等跳过，已发布行数与内容逐行不变；
22. 裁决沉淀的规则只在下一口径版本生效，并自动裁决同类新冲突；
23. 人工裁决不动任何原始记录，节点主记录冲突标志同步刷新；
24. 新版本出报为追加而非覆盖，已裁决运单纳入、仍 OPEN 的运单继续排除；
25. 节点主记录冲突标志、离群来源与最大差异秒数正确。
26. 人工裁决不回灌旧版本：v1 发布之后的裁决在重算 v1 时仍被版本边界拦截，
    规则裁决带生效版本号、人工裁决生效版本为空（语义为下一个发布版本）。
