"""占位区间重建与冲突清单（业务入口一，纯离线、不涉及统计剪枝）。

输入是两份不同粒度的 Parquet：
1. 工件进站/退站事件（events）：每行 = 一个事件，带工单号、夹具、工位、
   进退配对号、事件类型（IN/OUT）、时刻。进入/离开可能跨文件、跨行组、乱序存放。
2. 每台夹具的互斥工位规则（rules）：每行声明同一夹具上两个互斥的工位。

重建规则（与需求逐条对应）：
- 一张工单可能中途退站又返回，因此按 (工单号, 进退配对号) 分组重建“每一段”
  真实占位，绝不把第一次进入拉到最后一次离开。
- 每一对必须恰好一个 IN、一个 OUT，且离开时刻严格晚于进入时刻。缺端（只有 IN 或
  只有 OUT）、同一端内容矛盾（同端多行且时刻不一致）、时刻倒置等一律视为配对错误：
  单列进 `malformed`、带全部出处，并且该工单整单不参加交付。
- 占用区间 = [进入时刻, 离开时刻)，“时间相接不算冲突”（半开区间）。
- 两个区间冲突：仅当它们所在夹具相同、工位属于同一互斥对（含同工位），
  且区间内部相交（严格重叠，长度 > 0）。
- 互斥对是无向的且不传递：只按声明逐一判断，X-Y 与 Y-Z 互斥并不推出 X-Z 互斥。

所有判断都只依赖事件与规则本身的字段值；出处（文件名、行组、组内物理行号）
仅用于溯源与证据展示，不影响语义。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

import pyarrow.parquet as pq

from .errors import OccupancyError

EVENT_COLUMNS = ("work_order", "fixture", "station", "pair_id", "kind", "ts")
RULE_COLUMNS = ("fixture", "station_a", "station_b")

EVENT_KIND_IN = "IN"
EVENT_KIND_OUT = "OUT"


# ---------------------------------------------------------------------------
# 数据载体
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Event:
    """一条进/退站事件（保留完整溯源）。"""

    file: str
    row_group: int
    row_in_group: int
    work_order: str
    fixture: str
    station: str
    pair_id: str
    kind: str            # "IN" | "OUT"
    ts: int

    @property
    def provenance(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "row_group": self.row_group,
            "row_in_group": self.row_in_group,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "work_order": self.work_order,
            "fixture": self.fixture,
            "station": self.station,
            "pair_id": self.pair_id,
            "kind": self.kind,
            "ts": self.ts,
            "source": self.provenance,
        }


@dataclass(frozen=True)
class Rule:
    """一条夹具互斥工位规则（无向）。"""

    file: str
    row_group: int
    row_in_group: int
    fixture: str
    station_a: str
    station_b: str

    @property
    def provenance(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "row_group": self.row_group,
            "row_in_group": self.row_in_group,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "fixture": self.fixture,
            "station_a": self.station_a,
            "station_b": self.station_b,
            "source": self.provenance,
        }


@dataclass(frozen=True)
class Interval:
    """工单在 (夹具, 工位) 上的一段真实占位，半开区间 [start, end)。"""

    work_order: str
    fixture: str
    station: str
    pair_id: str
    start: int
    end: int
    in_event: Event
    out_event: Event

    def to_dict(self) -> dict[str, Any]:
        return {
            "work_order": self.work_order,
            "fixture": self.fixture,
            "station": self.station,
            "pair_id": self.pair_id,
            "start": self.start,
            "end": self.end,
            "in_source": self.in_event.provenance,
            "out_source": self.out_event.provenance,
        }


@dataclass
class Malformed:
    """无法重建为唯一合法区间的配对（整单排除的直接原因）。"""

    work_order: str
    pair_id: str
    reason: str
    records: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "work_order": self.work_order,
            "pair_id": self.pair_id,
            "reason": self.reason,
            "records": list(self.records),
        }


# ---------------------------------------------------------------------------
# 读取 Parquet（含溯源）
# ---------------------------------------------------------------------------
def _require_columns(schema_names: Iterable[str], required: tuple[str, ...], path: str) -> None:
    names = set(schema_names)
    missing = [c for c in required if c not in names]
    if missing:
        raise OccupancyError(f"{path}: 缺少必需列 {missing}（需要 {list(required)}）")


def _read_rows_with_provenance(path: str) -> list[tuple[int, int, dict[str, Any]]]:
    """逐行组读取，返回 (row_group, row_in_group, row) —— 保留物理行溯源。"""
    pf = pq.ParquetFile(path)
    out: list[tuple[int, int, dict[str, Any]]] = []
    for rgi in range(pf.num_row_groups):
        tbl = pf.read_row_group(rgi, use_threads=False)
        for ri, row in enumerate(tbl.to_pylist()):
            out.append((rgi, ri, row))
    return out


def _as_str(value: Any, column: str, path: str, rgi: int, ri: int) -> str:
    if not isinstance(value, str):
        raise OccupancyError(
            f"{path} rg={rgi} row={ri}: 列 {column} 必须是字符串，实际 {value!r}"
        )
    return value


def _as_int(value: Any, column: str, path: str, rgi: int, ri: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise OccupancyError(
            f"{path} rg={rgi} row={ri}: 列 {column} 必须是整数，实际 {value!r}"
        )
    return value


def load_events(paths: list[str]) -> list[Event]:
    events: list[Event] = []
    for path in paths:
        pf = pq.ParquetFile(path)
        _require_columns(pf.schema_arrow.names, EVENT_COLUMNS, path)
        for rgi, ri, row in _read_rows_with_provenance(path):
            kind = _as_str(row["kind"], "kind", path, rgi, ri)
            if kind not in (EVENT_KIND_IN, EVENT_KIND_OUT):
                raise OccupancyError(
                    f"{path} rg={rgi} row={ri}: kind 必须是 {EVENT_KIND_IN!r} 或 "
                    f"{EVENT_KIND_OUT!r}，实际 {kind!r}"
                )
            events.append(
                Event(
                    file=path,
                    row_group=rgi,
                    row_in_group=ri,
                    work_order=_as_str(row["work_order"], "work_order", path, rgi, ri),
                    fixture=_as_str(row["fixture"], "fixture", path, rgi, ri),
                    station=_as_str(row["station"], "station", path, rgi, ri),
                    pair_id=_as_str(row["pair_id"], "pair_id", path, rgi, ri),
                    kind=kind,
                    ts=_as_int(row["ts"], "ts", path, rgi, ri),
                )
            )
    return events


def load_rules(paths: list[str]) -> list[Rule]:
    rules: list[Rule] = []
    for path in paths:
        pf = pq.ParquetFile(path)
        _require_columns(pf.schema_arrow.names, RULE_COLUMNS, path)
        for rgi, ri, row in _read_rows_with_provenance(path):
            rules.append(
                Rule(
                    file=path,
                    row_group=rgi,
                    row_in_group=ri,
                    fixture=_as_str(row["fixture"], "fixture", path, rgi, ri),
                    station_a=_as_str(row["station_a"], "station_a", path, rgi, ri),
                    station_b=_as_str(row["station_b"], "station_b", path, rgi, ri),
                )
            )
    return rules


# ---------------------------------------------------------------------------
# 重建占位区间
# ---------------------------------------------------------------------------
def reconstruct_intervals(events: list[Event]) -> tuple[list[Interval], list[Malformed]]:
    """按 (工单号, 进退配对号) 重建区间；不合法配对进入 malformed。"""
    # 先按工单聚合，再按配对分组；保持稳定的首次出现顺序，便于人读。
    by_order: dict[str, dict[str, list[Event]]] = {}
    order_of_first_seen: list[str] = []
    for ev in events:
        if ev.work_order not in by_order:
            by_order[ev.work_order] = {}
            order_of_first_seen.append(ev.work_order)
        by_order[ev.work_order].setdefault(ev.pair_id, []).append(ev)

    intervals: list[Interval] = []
    malformed: list[Malformed] = []

    for work_order in order_of_first_seen:
        for pair_id, group in by_order[work_order].items():
            ins = [e for e in group if e.kind == EVENT_KIND_IN]
            outs = [e for e in group if e.kind == EVENT_KIND_OUT]
            records = [e.to_dict() for e in group]

            if len(ins) == 0 and len(outs) == 0:  # 理论上不可达；group 非空
                malformed.append(Malformed(work_order, pair_id, "配对没有任何事件", records))
                continue
            if len(ins) == 0:
                malformed.append(
                    Malformed(work_order, pair_id, "缺进入端（只有 OUT）", records)
                )
                continue
            if len(outs) == 0:
                malformed.append(
                    Malformed(work_order, pair_id, "缺离开端（只有 IN）", records)
                )
                continue

            # 每对必须恰好一个 IN 与一个 OUT（"只有一个进入和一个离开"）。
            # 同一端出现多行：内容不一致即"同一端内容矛盾"，内容一致也算违反唯一性。
            contradictory = None
            for label, rows in (("IN", ins), ("OUT", outs)):
                if len(rows) > 1:
                    signatures = {(e.fixture, e.station, e.ts) for e in rows}
                    if len(signatures) > 1:
                        contradictory = (
                            f"同一端 {label} 出现多行且内容矛盾（夹具/工位/时刻不一致）"
                        )
                        break
            if contradictory is not None:
                malformed.append(Malformed(work_order, pair_id, contradictory, records))
                continue
            if len(ins) > 1 or len(outs) > 1:
                malformed.append(
                    Malformed(
                        work_order, pair_id,
                        "同一端出现重复行（违反每对一个进入、一个离开的唯一性）",
                        records,
                    )
                )
                continue

            in_ev, out_ev = ins[0], outs[0]
            if (in_ev.fixture, in_ev.station) != (out_ev.fixture, out_ev.station):
                malformed.append(
                    Malformed(
                        work_order, pair_id,
                        "进入端与离开端的夹具/工位不一致",
                        records,
                    )
                )
                continue
            if out_ev.ts <= in_ev.ts:
                malformed.append(
                    Malformed(
                        work_order, pair_id,
                        f"离开时刻 {out_ev.ts} 不晚于进入时刻 {in_ev.ts}（区间为空或倒置）",
                        records,
                    )
                )
                continue

            intervals.append(
                Interval(
                    work_order=work_order,
                    fixture=in_ev.fixture,
                    station=in_ev.station,
                    pair_id=pair_id,
                    start=in_ev.ts,
                    end=out_ev.ts,
                    in_event=in_ev,
                    out_event=out_ev,
                )
            )

    return intervals, malformed


def malformed_work_orders(malformed: list[Malformed]) -> set[str]:
    return {m.work_order for m in malformed}


# ---------------------------------------------------------------------------
# 冲突清单
# ---------------------------------------------------------------------------
def exclusivity_index(rules: list[Rule]) -> dict[tuple[str, str], list[Rule]]:
    """(夹具, 工位) -> 与该工位互斥的规则；无向展开但不做传递闭包。"""
    index: dict[tuple[str, str], list[Rule]] = {}
    for rule in rules:
        index.setdefault((rule.fixture, rule.station_a), []).append(rule)
        if (rule.fixture, rule.station_b) != (rule.fixture, rule.station_a):
            index.setdefault((rule.fixture, rule.station_b), []).append(rule)
    return index


def _overlap(a: Interval, b: Interval) -> tuple[int, int] | None:
    start, end = max(a.start, b.start), min(a.end, b.end)
    if start < end:  # 半开区间内部相交；相接（start==end）不算冲突
        return start, end
    return None


def _interval_exclusive(a: Interval, b: Interval,
                        index: dict[tuple[str, str], list[Rule]]) -> list[Rule]:
    """返回能证明 a、b 两个区间互斥的规则（空列表 = 不冲突）。"""
    if a.fixture != b.fixture:
        return []
    if a.station == b.station:
        # 同台同一工位：天然互斥，无需规则；证据里说明这一点。
        return []
    supporting = []
    for rule in index.get((a.fixture, a.station), []):
        pair = {rule.station_a, rule.station_b}
        if pair == {a.station, b.station}:
            supporting.append(rule)
    return supporting


@dataclass
class Conflict:
    work_order_a: str
    work_order_b: str
    fixture: str
    station_a: str
    station_b: str
    basis: str                                  # "same_station" | "mutex_rule"
    overlap_start: int
    overlap_end: int
    interval_a: Interval
    interval_b: Interval
    rules: list[Rule]

    def to_dict(self) -> dict[str, Any]:
        return {
            "work_order_a": self.work_order_a,
            "work_order_b": self.work_order_b,
            "fixture": self.fixture,
            "station_a": self.station_a,
            "station_b": self.station_b,
            "basis": self.basis,
            "overlap": {"start": self.overlap_start, "end": self.overlap_end},
            "pair_a": self.interval_a.pair_id,
            "pair_b": self.interval_b.pair_id,
            "rules": [r.to_dict() for r in self.rules],
        }


def compute_conflicts(intervals: list[Interval],
                      rules: list[Rule]) -> list[Conflict]:
    """两两比较不同工单的区间，产出全部冲突证据。"""
    index = exclusivity_index(rules)
    conflicts: list[Conflict] = []
    for i in range(len(intervals)):
        for j in range(i + 1, len(intervals)):
            a, b = intervals[i], intervals[j]
            if a.work_order == b.work_order:
                continue  # 同一工单内部不构成“工单对冲突”
            if a.fixture != b.fixture:
                continue
            supporting = _interval_exclusive(a, b, index)
            if a.station != b.station and not supporting:
                continue
            overlap = _overlap(a, b)
            if overlap is None:
                continue
            # 稳定：工单号按升序作为 a/b，便于核查。
            if b.work_order < a.work_order:
                a, b = b, a
                supporting = _interval_exclusive(a, b, index)
            basis = "same_station" if a.station == b.station else "mutex_rule"
            conflicts.append(
                Conflict(
                    work_order_a=a.work_order,
                    work_order_b=b.work_order,
                    fixture=a.fixture,
                    station_a=a.station,
                    station_b=b.station,
                    basis=basis,
                    overlap_start=overlap[0],
                    overlap_end=overlap[1],
                    interval_a=a,
                    interval_b=b,
                    rules=supporting,
                )
            )
    conflicts.sort(key=lambda c: (c.work_order_a, c.work_order_b,
                                  c.overlap_start, c.overlap_end,
                                  c.station_a, c.station_b, c.interval_a.pair_id,
                                  c.interval_b.pair_id))
    return conflicts


# ---------------------------------------------------------------------------
# 汇总结果（供 deliver 直接复用）
# ---------------------------------------------------------------------------
@dataclass
class OccupancyModel:
    work_orders: list[str]                 # 出现过事件的全部工单（升序）
    intervals: list[Interval]
    malformed: list[Malformed]
    conflicts: list[Conflict]
    rules: list[Rule]

    @property
    def malformed_orders(self) -> set[str]:
        return malformed_work_orders(self.malformed)

    @property
    def bad_orders(self) -> set[str]:
        return self.malformed_orders

    @property
    def eligible_orders(self) -> list[str]:
        bad = self.bad_orders
        return [w for w in self.work_orders if w not in bad]

    def interfering_pairs(self) -> list[tuple[str, str]]:
        """所有冲突工单对（去重、升序）。"""
        pairs = {(c.work_order_a, c.work_order_b) for c in self.conflicts}
        return sorted(pairs)

    def conflict_pairs_among(self, orders: Iterable[str]) -> set[frozenset[str]]:
        keep = set(orders)
        return {
            frozenset((a, b))
            for a, b in self.interfering_pairs()
            if a in keep and b in keep
        }


def build_model(events: list[Event], rules: list[Rule]) -> OccupancyModel:
    intervals, malformed = reconstruct_intervals(events)
    conflicts = compute_conflicts(intervals, rules)
    work_orders = sorted({e.work_order for e in events})
    return OccupancyModel(
        work_orders=work_orders,
        intervals=intervals,
        malformed=malformed,
        conflicts=conflicts,
        rules=rules,
    )
