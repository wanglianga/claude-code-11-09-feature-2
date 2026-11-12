"""维表 SCD2 拉链加载作业（网点/派件员/路由/客户通用）。

拉链口径（对应需求）：
  * 代理键按自然键稳定生成，跨版本不变；新自然键才生成新代理键。
  * 新增自然键：插入 eff=快照日、exp=9999-12-31、is_current=true。
  * 属性变化：旧当前版本置 exp=快照日、is_current=false；
    同事务插入新版本 eff=快照日、exp=9999-12-31。
  * 合并/撤销/改名/换片区都只是"属性变化产生新版本"，历史版本永不物理删除。
  * 组织架构调整只在新日期分区生效：所有新版本 eff_date = 快照日，
    快照日之前的 [eff, exp) 区间不变，历史事实的时点关联结果不受影响。
  * --preview 只输出变化清单与重算影响（受影响事实行数、历史不受影响行数），不写表。
  * 幂等：MERGE 按 (自然键, eff_date) 匹配，同日重跑结果不变。

用法：
  spark-submit jobs/dim_scd2_job.py --snapshot-date 2026-11-11 --dim-name hub
  spark-submit jobs/dim_scd2_job.py --snapshot-date 2026-11-11 --dim-name all --preview
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F

from common.spark_utils import (
    DIM_SPECS, DIM_TABLES, DWD_EVENT, FUTURE_DATE, STG_TABLES,
    build_spark, load_conf, new_batch_id,
)

# 事实表中与各维表自然键对应的列（用于重算影响预览）
_FACT_NK_COL = {
    "hub": "hub_code",
    "courier": "courier_code",
    "route": "waybill_route_code",
    "customer": "customer_code",
}


def _attr_hash(attr_cols):
    return F.xxhash64(
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>"))
                           for c in attr_cols])
    )


def _stable_sk(spark, dim_name: str, stg, natural_key: str, sk_col: str):
    """自然键 -> 稳定代理键：已存在沿用，新键 pmod(xxhash64) 取正（避开保留值 -1）。"""
    existing = spark.table(DIM_TABLES[dim_name]).select(
        F.col(natural_key).alias("_nk_ex"), F.col(sk_col).alias("_sk_ex")).distinct()
    fresh = stg.select(F.col(natural_key).alias("_nk")).dropDuplicates(["_nk"])
    return (
        fresh.join(existing, F.col("_nk") == F.col("_nk_ex"), how="left")
        .withColumn("_gen", F.pmod(F.xxhash64(F.col("_nk")),
                                   F.lit(9_000_000_000_000_000_000)))
        .select(F.col("_nk"), F.coalesce(F.col("_sk_ex"), F.col("_gen")).alias(sk_col))
    )


def build_scd2(spark, dim_name: str, snapshot_date: str, batch_id: str):
    """返回 (staged_full 全量目标版本 DataFrame, 变化摘要 dict)。"""
    spec = DIM_SPECS[dim_name]
    nk, sk, attr_cols = spec["natural_key"], spec["sk"], spec["attr_cols"]
    target_cols = spark.table(DIM_TABLES[dim_name]).columns

    stg = (spark.table(STG_TABLES[dim_name])
           .filter(F.col("snapshot_date") == F.lit(snapshot_date))
           .dropDuplicates([nk]))
    if stg.count() == 0:
        return None, {"new": [], "changed": [], "unchanged": 0}

    stg_h = stg.withColumn("attr_hash", _attr_hash(attr_cols))
    sk_map = _stable_sk(spark, dim_name, stg_h, nk, sk)
    stg_h = stg_h.join(sk_map, stg_h[nk] == sk_map["_nk"], how="left").drop("_nk")

    cur = spark.table(DIM_TABLES[dim_name]).filter(
        F.col("is_current") == True)  # noqa: E712
    old_versions = spark.table(DIM_TABLES[dim_name]).filter(
        F.col("is_current") == False)  # noqa: E712

    # 当前版本哈希，用于区分新增/变化/未变化
    cur_hash = cur.select(F.col(nk).alias("_nk_cur"),
                          F.col("attr_hash").alias("_cur_hash"))
    cmp = (stg_h.join(cur_hash, stg_h[nk] == F.col("_nk_cur"), how="left"))
    new_df = cmp.filter(F.col("_cur_hash").isNull())
    changed_df = cmp.filter(
        F.col("_cur_hash").isNotNull() & (F.col("attr_hash") != F.col("_cur_hash")))
    unchanged_df = cmp.filter(
        F.col("_cur_hash").isNotNull() & (F.col("attr_hash") == F.col("_cur_hash")))

    changed_keys = [r[nk] for r in changed_df.select(nk).collect()]
    new_keys = [r[nk] for r in new_df.select(nk).collect()]

    # 保留：全部历史版本 + 与今日快照无关自然键的当前版本
    today_keys = stg_h.select(nk).distinct()
    kept_cur = cur.join(today_keys, on=nk, how="left_anti")
    kept = old_versions.unionByName(kept_cur)

    # 未变化：沿用旧当前版本（取目标表现存行，eff/exp 不动）
    unchanged_rows = cur.join(
        unchanged_df.select(F.col(nk).alias("_k")),
        cur[nk] == F.col("_k"), how="inner").drop("_k").select(*target_cols)

    # 变化：闭合旧当前版本
    closed_rows = (
        cur.join(
            changed_df.select(F.col(nk).alias("_k")),
            cur[nk] == F.col("_k"), how="inner")
        .drop("_k")
        .withColumn("exp_date", F.to_date(F.lit(snapshot_date)))
        .withColumn("is_current", F.lit(False))
        .select(*target_cols)
    )

    # 新版本（变化 + 新增），列序与目标表一致
    new_version = (
        changed_df.unionByName(new_df)
        .select(
            F.col(sk), F.col(nk), *[F.col(c) for c in attr_cols],
            F.to_date(F.lit(snapshot_date)).alias("eff_date"),
            F.to_date(F.lit(FUTURE_DATE)).alias("exp_date"),
            F.lit(True).alias("is_current"),
            F.col("attr_hash"),
            F.lit(batch_id).alias("ingest_batch_id"),
            F.current_timestamp().alias("ingest_time"),
        )
        .select(*target_cols)
    )

    staged = (kept.unionByName(unchanged_rows)
              .unionByName(closed_rows)
              .unionByName(new_version))

    summary = {"new": new_keys, "changed": changed_keys,
               "unchanged": unchanged_df.count()}
    return staged, summary


def impact_preview(spark, dim_name: str, snapshot_date: str,
                   changed_keys: list[str], new_keys: list[str]) -> dict:
    """重算影响预览：新版本只影响快照日及以后的事实，历史事实行不受影响。"""
    nk_fact = _FACT_NK_COL[dim_name]
    affected = list(set(changed_keys) | set(new_keys))
    if not affected:
        return {"affected_keys": 0, "future_fact_rows_impacted": 0,
                "historical_fact_rows_unaffected": 0}
    fact = spark.table(DWD_EVENT)
    future_cnt = (fact.filter(F.col(nk_fact).isin(affected))
                  .filter(F.col("event_date") >= F.lit(snapshot_date)).count())
    hist_cnt = (fact.filter(F.col(nk_fact).isin(affected))
                .filter(F.col("event_date") < F.lit(snapshot_date)).count())
    return {
        "affected_keys": len(affected),
        "effective_date": snapshot_date,
        "future_fact_rows_impacted": future_cnt,
        "historical_fact_rows_unaffected": hist_cnt,
        "rule": "历史区间不回刷：event_date < 快照日 的事实仍关联旧版本",
    }


def run_one(spark, conf: dict, dim_name: str, snapshot_date: str,
            preview: bool = False) -> dict:
    batch_id = new_batch_id("scd2")
    staged, summary = build_scd2(spark, dim_name, snapshot_date, batch_id)
    if staged is None:
        print(f"[scd2] {dim_name} 快照 {snapshot_date} 无数据，跳过")
        return {"dim": dim_name, "snapshot_date": snapshot_date, "skipped": True}

    impact = impact_preview(spark, dim_name, snapshot_date,
                            summary["changed"], summary["new"])
    result = {"dim": dim_name, "snapshot_date": snapshot_date,
              "changes": summary, "impact": impact}

    if preview:
        print("[scd2][PREVIEW] " + json.dumps(result, ensure_ascii=False, default=str))
        return result

    staged.createOrReplaceTempView(f"tmp_scd2_{dim_name}")
    spec = DIM_SPECS[dim_name]
    nk = spec["natural_key"]
    spark.sql(f"""
        MERGE INTO {DIM_TABLES[dim_name]} t
        USING tmp_scd2_{dim_name} s
        ON t.{nk} = s.{nk} AND t.eff_date = s.eff_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"[scd2] {dim_name} 快照 {snapshot_date} 闭合 {len(summary['changed'])} 个，"
          f"新增 {len(summary['new'])} 个，未变化 {summary['unchanged']} 个")
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot-date", required=True)
    ap.add_argument("--dim-name", default="all",
                    choices=["hub", "courier", "route", "customer", "all"])
    ap.add_argument("--preview", action="store_true",
                    help="只输出变化与重算影响预览，不写维表")
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_dim_scd2", conf)
    try:
        names = list(DIM_SPECS) if args.dim_name == "all" else [args.dim_name]
        results = [run_one(spark, conf, n, args.snapshot_date, args.preview)
                   for n in names]
        print(json.dumps(results, ensure_ascii=False, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
