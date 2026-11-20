"""生成占位冲突/交付的 Parquet 夹具（可本地复跑）。

三个场景，各自一个目录，含 events*.parquet、rules.parquet、values.json：

1. main/       —— 题目主例：F1/S1 上
                 A 占 [0,2) 与 [6,8)，B 占 [1,3)，C 占 [5,7)；
                 另含三个配对错误工单 M1/M2/M3（缺端 / 同端矛盾 / 整单排除）。
                 事件跨两个文件、多个行组、乱序存放。
                 期望：价值 A=5,B=3,C=3 -> 交付 B、C；A 列为必须 -> 交付 A。

2. shifted/    —— 把 A 的第二段改到 [7,8)（恰好接在 C 之后），
                 期望原价值下交付 A、C。

3. transitivity/ —— 夹具 F2 的三个工位 X/Y/Z，只声明 X-Y、Y-Z 互斥；
                 X 与 Z 不传递互斥，可同时交付。

直接运行：python tools/make_occupancy_fixtures.py
"""
from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq

EVENT_SCHEMA = pa.schema(
    [
        pa.field("work_order", pa.string()),
        pa.field("fixture", pa.string()),
        pa.field("station", pa.string()),
        pa.field("pair_id", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("ts", pa.int64()),
    ]
)
RULE_SCHEMA = pa.schema(
    [
        pa.field("fixture", pa.string()),
        pa.field("station_a", pa.string()),
        pa.field("station_b", pa.string()),
    ]
)


def _write_events(path: str, groups: list[list[tuple]]) -> None:
    writer = pq.ParquetWriter(path, EVENT_SCHEMA, write_statistics=True)
    try:
        for rows in groups:
            table = pa.table(
                {
                    "work_order": [r[0] for r in rows],
                    "fixture": [r[1] for r in rows],
                    "station": [r[2] for r in rows],
                    "pair_id": [r[3] for r in rows],
                    "kind": [r[4] for r in rows],
                    "ts": [r[5] for r in rows],
                },
                schema=EVENT_SCHEMA,
            )
            writer.write_table(table)
    finally:
        writer.close()


def _write_rules(path: str, groups: list[list[tuple]]) -> None:
    writer = pq.ParquetWriter(path, RULE_SCHEMA, write_statistics=True)
    try:
        for rows in groups:
            table = pa.table(
                {
                    "fixture": [r[0] for r in rows],
                    "station_a": [r[1] for r in rows],
                    "station_b": [r[2] for r in rows],
                },
                schema=RULE_SCHEMA,
            )
            writer.write_table(table)
    finally:
        writer.close()


def _write_json(path: str, obj) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def make_main(base: str) -> None:
    os.makedirs(base, exist_ok=True)
    f = "F1"
    s = "S1"
    # 事件故意乱序、跨两个文件、多个行组。
    events1 = [
        [("A", f, s, "A1", "OUT", 2), ("B", f, s, "B1", "IN", 1), ("C", f, s, "C1", "OUT", 7)],
        [("A", f, s, "A2", "IN", 6), ("M1", "F9", "S9", "M1-p", "IN", 0),
         ("M2", "F9", "S9", "M2-p", "IN", 0)],
        [("M2", "F9", "S9", "M2-p", "IN", 1), ("M3", "F9", "S9", "M3-good", "IN", 0),
         ("A", f, s, "A1", "IN", 0)],
    ]
    events2 = [
        [("B", f, s, "B1", "OUT", 3), ("C", f, s, "C1", "IN", 5),
         ("M3", "F9", "S9", "M3-good", "OUT", 2)],
        [("A", f, s, "A2", "OUT", 8), ("M3", "F9", "S9", "M3-bad", "IN", 5),
         ("M2", "F9", "S9", "M2-p", "OUT", 5)],
    ]
    _write_events(os.path.join(base, "events1.parquet"), events1)
    _write_events(os.path.join(base, "events2.parquet"), events2)
    # 一条与 S1 无关的规则，证明同台同工位的冲突无需规则声明。
    _write_rules(os.path.join(base, "rules.parquet"), [[("F1", "S2", "S3")]])
    _write_json(os.path.join(base, "values.json"), {"A": 5, "B": 3, "C": 3})


def make_shifted(base: str) -> None:
    os.makedirs(base, exist_ok=True)
    f = "F1"
    s = "S1"
    events = [
        [("A", f, s, "A1", "IN", 0), ("A", f, s, "A1", "OUT", 2),
         ("A", f, s, "A2", "OUT", 8), ("A", f, s, "A2", "IN", 7)],
        [("B", f, s, "B1", "IN", 1), ("B", f, s, "B1", "OUT", 3)],
        [("C", f, s, "C1", "IN", 5), ("C", f, s, "C1", "OUT", 7)],
    ]
    _write_events(os.path.join(base, "events.parquet"), events)
    _write_rules(os.path.join(base, "rules.parquet"), [[("F1", "S2", "S3")]])
    _write_json(os.path.join(base, "values.json"), {"A": 5, "B": 3, "C": 3})


def make_transitivity(base: str) -> None:
    os.makedirs(base, exist_ok=True)
    f = "F2"
    events = [
        [("WX", f, "X", "p1", "IN", 0), ("WX", f, "X", "p1", "OUT", 10)],
        [("WY", f, "Y", "p2", "IN", 0), ("WY", f, "Y", "p2", "OUT", 10)],
        [("WZ", f, "Z", "p3", "IN", 0), ("WZ", f, "Z", "p3", "OUT", 10)],
    ]
    _write_events(os.path.join(base, "events.parquet"), events)
    # 只声明 X-Y 与 Y-Z，故意不声明 X-Z。
    _write_rules(os.path.join(base, "rules.parquet"), [[("F2", "X", "Y")], [("F2", "Y", "Z")]])
    _write_json(os.path.join(base, "values.json"), {"WX": 1, "WY": 1, "WZ": 1})


def main() -> None:
    root = os.path.join("fixtures", "occupancy")
    make_main(os.path.join(root, "main"))
    make_shifted(os.path.join(root, "shifted"))
    make_transitivity(os.path.join(root, "transitivity"))
    print("wrote fixtures/occupancy/{main,shifted,transitivity}/")


if __name__ == "__main__":
    main()
