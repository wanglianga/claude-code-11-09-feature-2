"""本地端到端夹具生成器（仅用标准库）。

产出：
  conf/pipeline.local.conf        本地仓库配置
  data/landing/events/biz_date=*  上游事件 JSON Lines（按到达日分区落地）
  data/landing/dim/biz_date=*     维表每日快照

场景覆盖：
  WB001 正常冷链件（预约时段/冷链新列）
  WB002 分拨压货 >48 小时（v_hub_dwell 验证）
  WB003 乱序到达（report_time 与 event_time 顺序倒置，序号仍按 event_time）
  WB004 同 event_id 跨文件重复上报（ODS 留两份，DWD 去重留一份）
  WB005 多次异常独立保留（破损 -> 温控异常，兼容视图末次原因取后者）
  WB006 关键节点缺失后迟到补报（11-05 才补 11-01 的 ARRIVE，补数重放）
  WB007 未知网点 HUB_GHOST（代理键 -1，DQ 维表未匹配率）
  WB009 网点改名/派件员换片区/客户等级调整的时点关联验证
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

# ---------- 事件构造助手 ----------
def ev(event_id, waybill, etype, source, event_time, report_time, **payload):
    row = {
        "event_id": event_id, "waybill_no": waybill, "event_type": etype,
        "source_system": source, "event_time": event_time,
        "report_time": report_time,
    }
    row.update(payload)
    return row


def build_event_files() -> dict[str, list[dict]]:
    f: dict[str, list[dict]] = {d: [] for d in
                                ["2026-11-01", "2026-11-02", "2026-11-03",
                                 "2026-11-04", "2026-11-05"]}
    common = dict(route_code="R001")

    # ---- WB001 正常冷链件 ----
    f["2026-11-01"] += [
        ev("E101", "WB001", "PICKUP", "SCANNER",
           "2026-11-01 08:00:00", "2026-11-01 08:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH", courier_code="C001",
           customer_code="K001", appointment_window="2026-11-02 09:00-11:00",
           is_cold_chain=True, **common),
        ev("E102", "WB001", "DEPART", "HUB",
           "2026-11-01 10:00:00", "2026-11-01 10:10:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           next_hub_code="HUB_BJ", customer_code="K001", **common),
        ev("E103", "WB001", "ARRIVE", "HUB",
           "2026-11-01 20:00:00", "2026-11-01 20:15:00",
           hub_code="HUB_BJ", customer_code="K001", **common),
    ]
    f["2026-11-02"] += [
        ev("E104", "WB001", "DELIVER", "SCANNER",
           "2026-11-02 09:30:00", "2026-11-02 09:35:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", is_cold_chain=True, **common),
        ev("E105", "WB001", "SIGN", "SCANNER",
           "2026-11-02 12:00:00", "2026-11-02 12:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
    ]

    # ---- WB002 压货 >48h（11-01 21:00 到 HUB_BJ，11-04 08:00 才发）----
    f["2026-11-01"] += [
        ev("E201", "WB002", "PICKUP", "SCANNER",
           "2026-11-01 07:00:00", "2026-11-01 07:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K002", **common),
        ev("E202", "WB002", "DEPART", "HUB",
           "2026-11-01 09:00:00", "2026-11-01 09:10:00",
           hub_code="HUB_SH", next_hub_code="HUB_BJ",
           customer_code="K002", **common),
        ev("E203", "WB002", "ARRIVE", "HUB",
           "2026-11-01 21:00:00", "2026-11-01 21:20:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
    ]
    f["2026-11-04"] += [
        ev("E204", "WB002", "DEPART", "HUB",
           "2026-11-04 08:00:00", "2026-11-04 08:10:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
        ev("E205", "WB002", "DELIVER", "SCANNER",
           "2026-11-04 10:00:00", "2026-11-04 10:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E206", "WB002", "SIGN", "SCANNER",
           "2026-11-04 12:00:00", "2026-11-04 12:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
    ]

    # ---- WB003 乱序：ARRIVE 业务时间更早却更晚上报 ----
    f["2026-11-01"] += [
        ev("E301", "WB003", "PICKUP", "SCANNER",
           "2026-11-01 06:00:00", "2026-11-01 06:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K002", **common),
        ev("E302", "WB003", "DEPART", "HUB",
           "2026-11-01 18:00:00", "2026-11-01 18:05:00",
           hub_code="HUB_SH", next_hub_code="HUB_GZ",
           customer_code="K002", **common),
        ev("E303", "WB003", "ARRIVE", "HUB",
           "2026-11-01 15:00:00", "2026-11-01 18:40:00",
           hub_code="HUB_GZ", customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E304", "WB003", "SIGN", "SCANNER",
           "2026-11-02 11:00:00", "2026-11-02 11:10:00",
           hub_code="HUB_GZ", courier_code="C002",
           customer_code="K002", **common),
    ]

    # ---- WB004 重复上报：E401 出现在两个落地文件中（不同 source_file）----
    f["2026-11-01"] += [
        ev("E401", "WB004", "PICKUP", "SCANNER",
           "2026-11-01 08:30:00", "2026-11-01 08:35:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001", **common),
    ]
    f["2026-11-02"] += [
        ev("E402", "WB004", "SIGN", "SCANNER",
           "2026-11-02 10:00:00", "2026-11-02 10:05:00",
           hub_code="HUB_SH", courier_code="C001",
           customer_code="K001", **common),
    ]

    # ---- WB005 多次异常 ----
    f["2026-11-01"] += [
        ev("E501", "WB005", "PICKUP", "SCANNER",
           "2026-11-01 08:00:00", "2026-11-01 08:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001",
           is_cold_chain=True, **common),
        ev("E502", "WB005", "EXCEPTION", "HUB",
           "2026-11-01 14:00:00", "2026-11-01 14:10:00",
           hub_code="HUB_SH", exception_code="EX_DAMAGE",
           exception_reason="外包装破损", customer_code="K001", **common),
    ]
    f["2026-11-02"] += [
        ev("E503", "WB005", "EXCEPTION", "SCANNER",
           "2026-11-02 09:00:00", "2026-11-02 09:05:00",
           hub_code="HUB_BJ", exception_code="EX_COLD_TEMP",
           exception_reason="冷链温度超标", customer_code="K001",
           is_cold_chain=True, **common),
        ev("E504", "WB005", "SIGN", "SCANNER",
           "2026-11-02 15:00:00", "2026-11-02 15:10:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
    ]

    # ---- WB006 缺失 ARRIVE，11-05 迟到补报 11-01 的节点 ----
    f["2026-11-01"] += [
        ev("E601", "WB006", "PICKUP", "SCANNER",
           "2026-11-01 08:00:00", "2026-11-01 08:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K002", **common),
        ev("E602", "WB006", "DEPART", "HUB",
           "2026-11-01 11:00:00", "2026-11-01 11:10:00",
           hub_code="HUB_SH", next_hub_code="HUB_BJ",
           customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E603", "WB006", "DELIVER", "SCANNER",
           "2026-11-02 16:00:00", "2026-11-02 16:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E604", "WB006", "SIGN", "SCANNER",
           "2026-11-02 18:00:00", "2026-11-02 18:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
    ]
    f["2026-11-05"] += [
        # 分拨系统夜间断连补传：业务时间 11-01，到达落地日 11-05
        ev("E605", "WB006", "ARRIVE", "HUB",
           "2026-11-01 17:00:00", "2026-11-05 09:00:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
    ]

    # ---- WB007 未知网点（维表未匹配）----
    f["2026-11-01"] += [
        ev("E701", "WB007", "PICKUP", "SCANNER",
           "2026-11-01 09:00:00", "2026-11-01 09:05:00",
           origin_hub_code="HUB_GHOST", hub_code="HUB_GHOST",
           courier_code="C001", customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E702", "WB007", "SIGN", "SCANNER",
           "2026-11-02 13:00:00", "2026-11-02 13:05:00",
           hub_code="HUB_GHOST", courier_code="C001",
           customer_code="K002", **common),
    ]

    # ---- WB009 时点关联：11-01 用旧网点名/旧片区/KA，11-03 用新版本 ----
    f["2026-11-01"] += [
        ev("E901", "WB009", "PICKUP", "SCANNER",
           "2026-11-01 10:00:00", "2026-11-01 10:05:00",
           origin_hub_code="HUB_BJ", hub_code="HUB_BJ",
           courier_code="C001", customer_code="K001", **common),
    ]
    f["2026-11-03"] += [
        ev("E902", "WB009", "ARRIVE", "HUB",
           "2026-11-03 09:00:00", "2026-11-03 09:10:00",
           hub_code="HUB_BJ", courier_code="C001",
           customer_code="K001", **common),
        ev("E903", "WB009", "SIGN", "SCANNER",
           "2026-11-03 15:00:00", "2026-11-03 15:05:00",
           hub_code="HUB_BJ", courier_code="C001",
           customer_code="K001", **common),
    ]
    return f


def build_dim_files() -> dict[str, dict[str, list[dict]]]:
    return {
        "2026-10-31": {
            "hub": [
                {"hub_code": "HUB_SH", "hub_name": "上海华东分拨中心",
                 "hub_type": "HUB", "region_code": "EC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_BJ", "hub_name": "北京一集配站",
                 "hub_type": "HUB", "region_code": "NC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_GZ", "hub_name": "广州南分拨",
                 "hub_type": "HUB", "region_code": "SC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_OLD", "hub_name": "苏州老站",
                 "hub_type": "STATION", "region_code": "EC",
                 "parent_hub_code": "HUB_SH", "status": "ACTIVE",
                 "merged_into": None},
            ],
            "courier": [
                {"courier_code": "C001", "courier_name": "张伟",
                 "hub_code": "HUB_SH", "service_area": "浦东A片区", "status": "ACTIVE"},
                {"courier_code": "C002", "courier_name": "李娜",
                 "hub_code": "HUB_BJ", "service_area": "朝阳片区", "status": "ACTIVE"},
            ],
            "route": [
                {"route_code": "R001", "route_name": "沪京干线",
                 "origin_hub_code": "HUB_SH", "dest_hub_code": "HUB_BJ",
                 "transit_hubs": json.dumps(["HUB_GZ"], ensure_ascii=False),
                 "status": "ACTIVE"},
            ],
            "customer": [
                {"customer_code": "K001", "customer_name": "华美食品",
                 "customer_level": "KA", "sales_owner": "王销售", "status": "ACTIVE"},
                {"customer_code": "K002", "customer_name": "恒建商贸",
                 "customer_level": "SMB", "sales_owner": "赵销售", "status": "ACTIVE"},
            ],
        },
        # 11-02 组织调整：HUB_BJ 改名、HUB_OLD 合并撤销、C001 换片区、K001 降级
        "2026-11-02": {
            "hub": [
                {"hub_code": "HUB_SH", "hub_name": "上海华东分拨中心",
                 "hub_type": "HUB", "region_code": "EC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_BJ", "hub_name": "北京华北转运中心",
                 "hub_type": "HUB", "region_code": "NC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_GZ", "hub_name": "广州南分拨",
                 "hub_type": "HUB", "region_code": "SC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_OLD", "hub_name": "苏州老站",
                 "hub_type": "STATION", "region_code": "EC",
                 "parent_hub_code": "HUB_SH", "status": "MERGED",
                 "merged_into": "HUB_SH"},
            ],
            "courier": [
                {"courier_code": "C001", "courier_name": "张伟",
                 "hub_code": "HUB_SH", "service_area": "浦东B片区", "status": "ACTIVE"},
                {"courier_code": "C002", "courier_name": "李娜",
                 "hub_code": "HUB_BJ", "service_area": "朝阳片区", "status": "ACTIVE"},
            ],
            "route": [
                {"route_code": "R001", "route_name": "沪京干线",
                 "origin_hub_code": "HUB_SH", "dest_hub_code": "HUB_BJ",
                 "transit_hubs": json.dumps(["HUB_GZ"], ensure_ascii=False),
                 "status": "ACTIVE"},
            ],
            "customer": [
                {"customer_code": "K001", "customer_name": "华美食品",
                 "customer_level": "STD", "sales_owner": "王销售",
                 "status": "ACTIVE"},
                {"customer_code": "K002", "customer_name": "恒建商贸",
                 "customer_level": "SMB", "sales_owner": "赵销售",
                 "status": "ACTIVE"},
            ],
        },
    }


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    args = ap.parse_args()
    root = Path(args.root)

    for sub in ["runtime/warehouse", "data/landing/events",
                "data/landing/dim", "data/backfill"]:
        p = root / sub
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    # 重复上报：E401 再次出现在 scanner_retry 文件（不同 source_file）
    event_files = build_event_files()
    for biz_date, rows in event_files.items():
        write_jsonl(root / f"data/landing/events/biz_date={biz_date}/scanner_hub.jsonl",
                    rows)
    dup_row = next(r for r in event_files["2026-11-01"] if r["event_id"] == "E401")
    write_jsonl(root / "data/landing/events/biz_date=2026-11-01/scanner_retry.jsonl",
                [dup_row])

    for snap_date, dims in build_dim_files().items():
        for dim_name, rows in dims.items():
            write_jsonl(
                root / f"data/landing/dim/biz_date={snap_date}/{dim_name}.jsonl",
                rows)

    conf = f"""[DEFAULT]
warehouse.dir={root}/runtime/warehouse
catalog.name=ltl
catalog.type=jdbc
catalog.uri=
jdbc.user=sa
jdbc.password=
catalog.impl=org.apache.iceberg.spark.SparkCatalog
landing.event.dir={root}/data/landing/events
landing.dim.dir={root}/data/landing/dim
watermark.lookback.days=30
dq.threshold.event_missing_rate=0.02
dq.threshold.out_of_order_rate=0.05
dq.threshold.duplicate_rate=0.01
dq.threshold.dim_unmatched_rate=0.01
dq.threshold.partition_latency_minutes=180
backfill.shadow.suffix=_shadow
backfill.output.dir={root}/data/backfill
"""
    (root / "conf/pipeline.local.conf").write_text(conf, encoding="utf-8")
    print(f"[fixtures] root={root} 事件落地日={list(event_files)} "
          f"维表快照日={list(build_dim_files())}")


if __name__ == "__main__":
    main()
