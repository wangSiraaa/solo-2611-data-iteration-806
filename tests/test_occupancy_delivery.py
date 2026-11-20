"""占位区间重建、冲突清单与交付选择的测试。

运行：PYTHONPATH=src .venv/bin/python -m pytest tests/ -q
"""
from __future__ import annotations

import itertools

import pytest

from pqscan.delivery import select_delivery
from pqscan.errors import DeliveryError, DeliveryRejected
from pqscan.occupancy import (
    Event,
    Rule,
    build_model,
)

F = "F1"
S = "S1"


def _ev(wo, station, pair, kind, ts, fixture=F, file="f.parquet", rg=0, row=0):
    return Event(
        file=file, row_group=rg, row_in_group=row,
        work_order=wo, fixture=fixture, station=station, pair_id=pair,
        kind=kind, ts=ts,
    )


def _intervals(model):
    return {(iv.work_order, iv.pair_id): (iv.start, iv.end)
            for iv in model.intervals}


# ---------------------------------------------------------------------------
# 区间重建
# ---------------------------------------------------------------------------
def test_multiple_segments_not_merged():
    """一张工单中途退站又返回：按配对重建两段，不能拉成 [0,8)。"""
    events = [
        _ev("A", S, "A1", "IN", 0), _ev("A", S, "A1", "OUT", 2),
        _ev("A", S, "A2", "IN", 6), _ev("A", S, "A2", "OUT", 8),
    ]
    model = build_model(events, [])
    assert _intervals(model) == {("A", "A1"): (0, 2), ("A", "A2"): (6, 8)}
    assert not model.malformed


def test_out_of_order_and_cross_file_pairing():
    events = [
        _ev("A", S, "A1", "OUT", 5, file="b.parquet", rg=1, row=0),
        _ev("A", S, "A1", "IN", 1, file="a.parquet", rg=0, row=2),
    ]
    model = build_model(events, [])
    assert _intervals(model) == {("A", "A1"): (1, 5)}
    iv = model.intervals[0]
    assert iv.in_event.file == "a.parquet" and iv.in_event.row_in_group == 2
    assert iv.out_event.file == "b.parquet" and iv.out_event.row_group == 1


def test_missing_endpoint_is_malformed_with_source():
    events = [_ev("M", S, "p", "IN", 0, rg=3, row=1)]
    model = build_model(events, [])
    assert model.malformed and model.malformed[0].work_order == "M"
    assert "缺离开端" in model.malformed[0].reason
    assert model.malformed[0].records[0]["source"]["row_group"] == 3
    assert model.malformed[0].records[0]["source"]["row_in_group"] == 1


def test_contradictory_endpoint_is_malformed():
    events = [
        _ev("M", S, "p", "IN", 0),
        _ev("M", S, "p", "IN", 5),   # 同端第二个 IN，时刻矛盾
        _ev("M", S, "p", "OUT", 9),
    ]
    model = build_model(events, [])
    assert len(model.malformed) == 1
    assert "内容矛盾" in model.malformed[0].reason


def test_empty_or_inverted_interval_is_malformed():
    model = build_model([_ev("M", S, "p", "IN", 5), _ev("M", S, "p", "OUT", 5)], [])
    assert "不晚于" in model.malformed[0].reason


def test_one_bad_pair_excludes_whole_order():
    """整单排除：工单只要有一段配对错误，全部区间都不参加交付。"""
    events = [
        _ev("M", S, "good", "IN", 0), _ev("M", S, "good", "OUT", 2),
        _ev("M", S, "bad", "IN", 4),  # 缺 OUT
    ]
    model = build_model(events, [])
    assert model.malformed_orders == {"M"}
    assert "M" not in model.eligible_orders


# ---------------------------------------------------------------------------
# 冲突判定：半开区间、同工位、无向且不传递
# ---------------------------------------------------------------------------
def _pair(events, rules=()):
    return build_model(events, list(rules)).interfering_pairs()


def test_touching_intervals_do_not_conflict():
    events = [
        _ev("A", S, "a", "IN", 0), _ev("A", S, "a", "OUT", 2),
        _ev("B", S, "b", "IN", 2), _ev("B", S, "b", "OUT", 4),
    ]
    assert _pair(events) == []


def test_same_station_overlap_conflicts():
    events = [
        _ev("A", S, "a", "IN", 0), _ev("A", S, "a", "OUT", 2),
        _ev("B", S, "b", "IN", 1), _ev("B", S, "b", "OUT", 3),
    ]
    model = build_model(events, [])
    assert model.interfering_pairs() == [("A", "B")]
    c = model.conflicts[0]
    assert c.basis == "same_station"
    assert (c.overlap_start, c.overlap_end) == (1, 2)
    assert c.interval_a.pair_id == "a" and c.interval_b.pair_id == "b"


def test_rule_is_undirected():
    # 规则只写 X,Y；Y 与 X 反向同样互斥。
    rules = [Rule("r.parquet", 0, 0, F, "X", "Y")]
    events = [
        _ev("A", "X", "a", "IN", 0), _ev("A", "X", "a", "OUT", 5),
        _ev("B", "Y", "b", "IN", 1), _ev("B", "Y", "b", "OUT", 6),
    ]
    model = build_model(events, rules)
    assert model.interfering_pairs() == [("A", "B")]
    assert model.conflicts[0].basis == "mutex_rule"


def test_rule_is_not_transitive():
    """只声明 X-Y、Y-Z：X 与 Z 可同时交付。"""
    rules = [Rule("r.parquet", 0, 0, F, "X", "Y"), Rule("r.parquet", 1, 0, F, "Y", "Z")]
    events = [
        _ev("A", "X", "a", "IN", 0), _ev("A", "X", "a", "OUT", 5),
        _ev("B", "Y", "b", "IN", 0), _ev("B", "Y", "b", "OUT", 5),
        _ev("C", "Z", "c", "IN", 0), _ev("C", "Z", "c", "OUT", 5),
    ]
    model = build_model(events, rules)
    assert model.interfering_pairs() == [("A", "B"), ("B", "C")]
    result = select_delivery(model.eligible_orders, [c.to_dict() for c in model.conflicts],
                             {"A": 1, "B": 1, "C": 1})
    assert result.chosen == ["A", "C"]


def test_different_stations_without_rule_not_conflict():
    events = [
        _ev("A", "X", "a", "IN", 0), _ev("A", "X", "a", "OUT", 5),
        _ev("B", "Y", "b", "IN", 0), _ev("B", "Y", "b", "OUT", 5),
    ]
    assert _pair(events) == []


# ---------------------------------------------------------------------------
# 交付选择：题目三个示例
# ---------------------------------------------------------------------------
def _main_model():
    events = [
        _ev("A", S, "A1", "IN", 0), _ev("A", S, "A1", "OUT", 2),
        _ev("A", S, "A2", "IN", 6), _ev("A", S, "A2", "OUT", 8),
        _ev("B", S, "B1", "IN", 1), _ev("B", S, "B1", "OUT", 3),
        _ev("C", S, "C1", "IN", 5), _ev("C", S, "C1", "OUT", 7),
    ]
    model = build_model(events, [])
    return model, [c.to_dict() for c in model.conflicts]


def test_example_delivers_b_and_c():
    model, conflicts = _main_model()
    r = select_delivery(model.eligible_orders, conflicts, {"A": 5, "B": 3, "C": 3})
    assert r.chosen == ["B", "C"] and r.total_value == 6


def test_example_required_a_force_delivers_a():
    model, conflicts = _main_model()
    r = select_delivery(model.eligible_orders, conflicts, {"A": 5, "B": 3, "C": 3},
                        required=["A"])
    assert r.chosen == ["A"] and r.total_value == 5


def test_example_shifted_a2_touching_c_delivers_a_and_c():
    events = [
        _ev("A", S, "A1", "IN", 0), _ev("A", S, "A1", "OUT", 2),
        _ev("A", S, "A2", "IN", 7), _ev("A", S, "A2", "OUT", 8),
        _ev("B", S, "B1", "IN", 1), _ev("B", S, "B1", "OUT", 3),
        _ev("C", S, "C1", "IN", 5), _ev("C", S, "C1", "OUT", 7),
    ]
    model = build_model(events, [])
    r = select_delivery(model.eligible_orders, [c.to_dict() for c in model.conflicts],
                        {"A": 5, "B": 3, "C": 3})
    assert r.chosen == ["A", "C"] and r.total_value == 8


# ---------------------------------------------------------------------------
# 平局、必须项冲突、价值复用
# ---------------------------------------------------------------------------
def test_tie_break_lexicographically_smallest_ascending_list():
    # A-C 互斥；A 与 B 各值 1 -> 两种最优解 {A,B} 与 {B,C} 都值 2? 这里构造更直接：
    # 三张两两冲突、同价值 5 -> 应选字典序最小的单张：A。
    events = [
        _ev("A", S, "a", "IN", 0), _ev("A", S, "a", "OUT", 9),
        _ev("B", S, "b", "IN", 1), _ev("B", S, "b", "OUT", 8),
        _ev("C", S, "c", "IN", 2), _ev("C", S, "c", "OUT", 7),
    ]
    model = build_model(events, [])
    r = select_delivery(model.eligible_orders, [c.to_dict() for c in model.conflicts],
                        {"A": 5, "B": 5, "C": 5})
    assert r.chosen == ["A"]


def test_tie_break_prefers_lower_prefix():
    # 两组彼此独立的冲突对 A×B、C×D，各取一张、每张价值 1，四种最优解总价值均为 2；
    # 升序列表字典序最小者为 [A, C]。
    events = [
        _ev("A", S, "a", "IN", 0), _ev("A", S, "a", "OUT", 5),
        _ev("B", S, "b", "IN", 0), _ev("B", S, "b", "OUT", 5),   # A×B 冲突
        _ev("C", "S2", "c", "IN", 0), _ev("C", "S2", "c", "OUT", 5),
        _ev("D", "S2", "d", "IN", 0), _ev("D", "S2", "d", "OUT", 5),  # C×D 冲突
    ]
    model = build_model(events, [])
    r = select_delivery(model.eligible_orders, [c.to_dict() for c in model.conflicts],
                        {"A": 1, "B": 1, "C": 1, "D": 1})
    assert r.chosen == ["A", "C"]


def test_required_conflict_rejected_with_evidence():
    model, conflicts = _main_model()
    with pytest.raises(DeliveryRejected) as exc:
        select_delivery(model.eligible_orders, conflicts, {"A": 5, "B": 3, "C": 3},
                        required=["A", "B"])
    assert exc.value.evidence
    assert exc.value.evidence[0]["work_order_a"] == "A"
    assert exc.value.evidence[0]["work_order_b"] == "B"


def test_required_not_eligible_rejected():
    model, conflicts = _main_model()
    with pytest.raises(DeliveryError):
        select_delivery(model.eligible_orders, conflicts, {"A": 5, "B": 3, "C": 3},
                        required=["Z"])


def test_missing_or_nonpositive_value_rejected():
    model, conflicts = _main_model()
    with pytest.raises(DeliveryError):
        select_delivery(model.eligible_orders, conflicts, {"A": 5, "B": 3})
    with pytest.raises(DeliveryError):
        select_delivery(model.eligible_orders, conflicts, {"A": 5, "B": 3, "C": 0})


def test_conflict_list_reusable_when_values_change():
    """同一份冲突清单，只改价值即可重新报价，无需重建区间。"""
    model, conflicts = _main_model()
    r1 = select_delivery(model.eligible_orders, conflicts, {"A": 5, "B": 3, "C": 3})
    # B、C 不相互斥，合计 3+3=6 > 5：A 降至 4 时仍选 B、C。
    r2 = select_delivery(model.eligible_orders, conflicts, {"A": 4, "B": 3, "C": 3})
    # A 提到 7 时才压过 B+C=6。
    r3 = select_delivery(model.eligible_orders, conflicts, {"A": 7, "B": 3, "C": 3})
    assert r1.chosen == ["B", "C"]
    assert r2.chosen == ["B", "C"]
    assert r3.chosen == ["A"]


# ---------------------------------------------------------------------------
# 与暴力枚举一致（精确解）
# ---------------------------------------------------------------------------
def _brute_force(orders, conflict_pairs, values):
    best_key = None
    for k in range(len(orders) + 1):
        for subset in itertools.combinations(orders, k):
            chosen = set(subset)
            if any(a in chosen and b in chosen for a, b in conflict_pairs):
                continue
            value = sum(values[o] for o in chosen)
            asc = sorted(subset)
            key = (-value, asc)
            if best_key is None or key < best_key:
                best_key = key
    return sorted(best_key[1]), -best_key[0]


def test_matches_brute_force_on_random_graphs():
    import random

    rng = random.Random(20261120)
    for _ in range(25):
        n = rng.randint(1, 8)
        orders = [f"W{i}" for i in range(n)]
        values = {o: rng.randint(1, 9) for o in orders}
        pairs = [(a, b) for a, b in itertools.combinations(orders, 2)
                 if rng.random() < 0.35]
        records = [{"work_order_a": a, "work_order_b": b, "fixture": F,
                    "station_a": S, "station_b": S, "basis": "same_station",
                    "overlap": {"start": 0, "end": 1}} for a, b in pairs]
        got = select_delivery(orders, records, values)
        exp_orders, exp_value = _brute_force(orders, pairs, values)
        assert got.total_value == exp_value
        assert got.chosen == exp_orders


def test_twenty_work_orders_exact():
    orders = [f"W{i:02d}" for i in range(20)]
    values = {o: (i % 5) + 1 for i, o in enumerate(orders)}
    # 链式冲突 W00-W01-W02...：最优是取所有偶数下标。
    pairs = [(orders[i], orders[i + 1]) for i in range(19)]
    records = [{"work_order_a": a, "work_order_b": b, "fixture": F,
                "station_a": S, "station_b": S, "basis": "same_station",
                "overlap": {"start": 0, "end": 1}} for a, b in pairs]
    got = select_delivery(orders, records, values)
    exp_orders, exp_value = _brute_force(orders, pairs, values)
    assert got.chosen == exp_orders and got.total_value == exp_value


def test_over_twenty_rejected():
    orders = [f"W{i:02d}" for i in range(21)]
    with pytest.raises(DeliveryError):
        select_delivery(orders, [], {o: 1 for o in orders})
