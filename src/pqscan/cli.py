"""命令行入口。

用法：
    python -m pqscan.cli run --files fixtures/files.json --queries fixtures/queries.json \
        --out output
    python -m pqscan.cli verify --files ... --queries ... --out output   # 只跑独立核对
    # 业务入口一：重建占位区间 + 冲突清单
    python -m pqscan.cli occupancy --events e1.parquet e2.parquet \
        --rules rules.parquet --out output/occupancy
    # 业务入口二：基于冲突清单做交付选择（可反复更换价值/必须项重新报价）
    python -m pqscan.cli deliver --conflicts output/occupancy/occupancy.json \
        --values values.json --required A --out output/delivery
"""
from __future__ import annotations

import argparse
import json
import sys

from .errors import DeliveryRejected, PqScanError
from .expr import parse_queries, validate, validate_projection
from .reference import reference_scan
from .runner import run


def _cmd_run(args: argparse.Namespace) -> int:
    doc = run(args.files, args.queries, args.out)
    all_ok = all(q["passed"] for q in doc["verification"]["queries"])
    for q in doc["verification"]["queries"]:
        flag = "PASS" if q["passed"] else "FAIL"
        print(f"[verify:{flag}] {q['query_id']}: {q['detail']} "
              f"(engine={q['engine_rows']}, reference={q['reference_rows']})")
    print(f"\n计划与结果已写入 {args.out}/ (plans/、results/、run_record.json、verification.json)")
    return 0 if all_ok else 2


def _cmd_verify(args: argparse.Namespace) -> int:
    import os
    import pyarrow.parquet as pq

    fixture_dir = os.path.dirname(os.path.abspath(args.files))
    with open(args.files, encoding="utf-8") as fh:
        files = [os.path.join(fixture_dir, n) for n in json.load(fh)]
    with open(args.queries, encoding="utf-8") as fh:
        queries = parse_queries(json.load(fh))

    failed = 0
    for query in queries:
        for path in files:
            validate(query.where, pq.ParquetFile(path).schema_arrow)
            validate_projection(query.columns, pq.ParquetFile(path).schema_arrow)
        ref = reference_scan(query, files)
        result_path = os.path.join(args.out, "results", f"{query.id}.parquet")
        got = pq.read_table(result_path)
        same = got.schema == ref.schema and got.to_pylist() == ref.to_pylist()
        print(f"[{'PASS' if same else 'FAIL'}] {query.id}: {got.num_rows} 行")
        failed += 0 if same else 1
    return 0 if failed == 0 else 2


def _cmd_occupancy(args: argparse.Namespace) -> int:
    import os

    from .occupancy import build_model, load_events, load_rules

    events = load_events(args.events)
    rules = load_rules(args.rules)
    model = build_model(events, rules)

    os.makedirs(args.out, exist_ok=True)
    doc = {
        "events_files": args.events,
        "rules_files": args.rules,
        "work_orders": model.work_orders,
        "eligible_work_orders": model.eligible_orders,
        "malformed_work_orders": sorted(model.bad_orders),
        "intervals": [iv.to_dict() for iv in model.intervals],
        "malformed": [m.to_dict() for m in model.malformed],
        "conflicts": [c.to_dict() for c in model.conflicts],
        "interfering_pairs": [
            {"work_order_a": a, "work_order_b": b}
            for a, b in model.interfering_pairs()
        ],
    }
    out_path = os.path.join(args.out, "occupancy.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)

    print(f"工单 {len(model.work_orders)} 张；合格 {len(model.eligible_orders)} 张；"
          f"配对错误 {len(model.bad_orders)} 张")
    if model.bad_orders:
        print(f"  整单排除（配对错误）: {', '.join(sorted(model.bad_orders))}")
    print(f"占位区间 {len(model.intervals)} 段；冲突证据 {len(model.conflicts)} 条；"
          f"冲突工单对 {len(model.interfering_pairs())} 组")
    for a, b in model.interfering_pairs():
        print(f"  冲突: {a} × {b}")
    print(f"冲突清单已写入 {out_path}")
    return 0


def _cmd_deliver(args: argparse.Namespace) -> int:
    import os

    from .delivery import select_delivery

    with open(args.conflicts, encoding="utf-8") as fh:
        occ = json.load(fh)
    with open(args.values, encoding="utf-8") as fh:
        values = json.load(fh)
    if not isinstance(values, dict):
        raise PqScanError("values 必须是 {工单号: 正整数} 的对象")

    try:
        result = select_delivery(
            work_orders=occ["eligible_work_orders"],
            conflict_records=occ["conflicts"],
            values=values,
            required=args.required or [],
        )
    except DeliveryRejected as exc:
        print(f"拒绝: {exc}", file=sys.stderr)
        os.makedirs(args.out, exist_ok=True)
        with open(os.path.join(args.out, "rejection.json"), "w", encoding="utf-8") as fh:
            json.dump({"rejected": True, "reason": str(exc), "evidence": exc.evidence},
                      fh, ensure_ascii=False, indent=2)
        return 3

    os.makedirs(args.out, exist_ok=True)
    doc = result.to_dict()
    with open(os.path.join(args.out, "delivery.json"), "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)

    print(f"交付工单: {', '.join(result.chosen) if result.chosen else '(空)'}")
    print(f"总价值: {result.total_value}")
    if result.required:
        print(f"必须项: {', '.join(result.required)}")
    print(f"交付明细已写入 {os.path.join(args.out, 'delivery.json')}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pqscan", description="Parquet footer 统计驱动的保守 plan/scan")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="计划 + 扫描 + 独立核对")
    p_run.add_argument("--files", required=True)
    p_run.add_argument("--queries", required=True)
    p_run.add_argument("--out", default="output")
    p_run.set_defaults(func=_cmd_run)

    p_ver = sub.add_parser("verify", help="对已产出的结果再跑一次无剪枝全扫描核对")
    p_ver.add_argument("--files", required=True)
    p_ver.add_argument("--queries", required=True)
    p_ver.add_argument("--out", default="output")
    p_ver.set_defaults(func=_cmd_verify)

    p_occ = sub.add_parser("occupancy", help="重建真实占位区间并生成独立冲突清单（业务入口一）")
    p_occ.add_argument("--events", required=True, nargs="+", help="一个或多个进/退站事件 Parquet")
    p_occ.add_argument("--rules", required=True, nargs="+", help="一个或多个互斥工位规则 Parquet")
    p_occ.add_argument("--out", default="output/occupancy")
    p_occ.set_defaults(func=_cmd_occupancy)

    p_del = sub.add_parser("deliver", help="基于冲突清单选择最大价值可同时交付集合（业务入口二）")
    p_del.add_argument("--conflicts", required=True, help="occupancy 产出的 occupancy.json")
    p_del.add_argument("--values", required=True, help="{工单号: 正整数} 的 JSON")
    p_del.add_argument("--required", nargs="*", default=[], help="必须交付的工单号")
    p_del.add_argument("--out", default="output/delivery")
    p_del.set_defaults(func=_cmd_deliver)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except PqScanError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
