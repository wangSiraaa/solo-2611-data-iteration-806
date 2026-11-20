"""交付选择（业务入口二）：在合格工单上求“总价值最大的可同时交付整单集合”。

输入：
- 上一入口（occupancy）产出的冲突清单：只用到其中“哪些合格工单两两冲突”；
- 每张工单的正整数保留价值；
- 一组必须交付的工单。

求解（精确，适用于最多 20 张合格工单，按批离线计算）：
- 冲突是“工单对”级别的硬约束（任一对冲突都不能同时交付），且不传递；
  因此这是最大权独立集（MWIS）/ 最大权团独立问题，用位掩码分支定界精确求解。
- 目标：最大化总价值；同价值时取升序工单号列表字典序最小者。
  Python 字符串比较即码位序；由于工单号互不相同，逐元素比较在“首个不同位置”
  就等价于“该处工单号较小者胜出”。
- 必须项：必须项之间若互相冲突，整单请求直接拒绝，并指出冲突证据（出处）。
- 权重或必须项改变时，只需重建价值表与必须集合，复用同一份冲突清单重新报价。

只依赖冲突清单中的 (工单, 工单) 对，不重新读取 Parquet。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .errors import DeliveryError, DeliveryRejected

MAX_EXACT_WORK_ORDERS = 20


@dataclass
class DeliveryResult:
    work_orders: list[str]                    # 本次参与选择的合格工单（升序）
    required: list[str]                       # 规范化后的必须项（升序）
    values: dict[str, int]
    conflicts: dict[frozenset[str], list[dict[str, Any]]]  # 合格工单间的冲突证据
    chosen: list[str]
    total_value: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "work_orders": list(self.work_orders),
            "required": list(self.required),
            "values": dict(self.values),
            "chosen": list(self.chosen),
            "total_value": self.total_value,
            "conflicts_considered": [
                {
                    "work_order_a": sorted(pair)[0],
                    "work_order_b": sorted(pair)[1],
                    "evidence_count": len(ev),
                    "evidence": ev,
                }
                for pair, ev in sorted(
                    self.conflicts.items(), key=lambda kv: tuple(sorted(kv[0]))
                )
            ],
        }


def _sorted_pair(a: str, b: str) -> frozenset[str]:
    return frozenset((a, b))


def select_delivery(
    work_orders: Iterable[str],
    conflict_records: list[dict[str, Any]],
    values: dict[str, int],
    required: Iterable[str] = (),
) -> DeliveryResult:
    """求最大价值可同时交付集合。

    work_orders: 合格工单（已排除配对错误工单）。
    conflict_records: occupancy 冲突清单（每条至少含 work_order_a/work_order_b）。
    values: 工单 -> 正整数价值。
    required: 必须交付的工单。
    """
    orders = sorted(set(work_orders))
    if len(orders) != len(set(work_orders)):
        raise DeliveryError("合格工单列表中存在重复项")
    if len(orders) > MAX_EXACT_WORK_ORDERS:
        raise DeliveryError(
            f"合格工单 {len(orders)} 张，超过精确求解上限 {MAX_EXACT_WORK_ORDERS}"
        )
    order_set = set(orders)

    # ---- 价值校验：每张合格工单都必须有正整数价值 ---------------------------
    clean_values: dict[str, int] = {}
    for wo in orders:
        if wo not in values:
            raise DeliveryError(f"缺少工单 {wo} 的保留价值")
        v = values[wo]
        if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
            raise DeliveryError(f"工单 {wo} 的保留价值必须是正整数，实际 {v!r}")
        clean_values[wo] = v

    # ---- 必须项校验 ---------------------------------------------------------
    req = sorted(set(required))
    for wo in req:
        if wo not in order_set:
            raise DeliveryError(
                f"必须交付工单 {wo} 不是合格工单（不存在或配对错误已整单排除）"
            )

    # ---- 只保留合格工单之间的冲突证据 --------------------------------------
    conflicts: dict[frozenset[str], list[dict[str, Any]]] = {}
    for rec in conflict_records:
        a, b = rec.get("work_order_a"), rec.get("work_order_b")
        if a not in order_set or b not in order_set or a == b:
            continue
        conflicts.setdefault(_sorted_pair(a, b), []).append(rec)

    # ---- 必须项之间若冲突：整单拒绝并指出出处 --------------------------------
    req_conflicts = [pair for pair in conflicts if pair <= frozenset(req)]
    if req_conflicts:
        evidence = []
        for pair in sorted(req_conflicts, key=lambda p: tuple(sorted(p))):
            evidence.append(
                {
                    "work_order_a": sorted(pair)[0],
                    "work_order_b": sorted(pair)[1],
                    "evidence": conflicts[pair],
                }
            )
        raise DeliveryRejected(
            "必须交付工单之间存在冲突，整单请求被拒绝：" + _describe_req_conflicts(evidence),
            evidence,
        )

    # ---- 精确求解 -----------------------------------------------------------
    index = {wo: i for i, wo in enumerate(orders)}
    n = len(orders)
    weights = [clean_values[wo] for wo in orders]
    adj = [0] * n
    for pair in conflicts:
        a, b = tuple(pair)
        ia, ib = index[a], index[b]
        adj[ia] |= 1 << ib
        adj[ib] |= 1 << ia

    # 必须项一律入选，其邻居被禁止。
    chosen_mask = 0
    forbidden = 0
    for wo in req:
        bit = 1 << index[wo]
        chosen_mask |= bit
        forbidden |= adj[index[wo]]
    forbidden &= ~chosen_mask  # 必须项彼此不冲突（前面已校验）

    free_bits = [i for i in range(n) if not (chosen_mask >> i) & 1 and not (forbidden >> i) & 1]
    best_extra_mask, best_extra_value = _solve_free(free_bits, weights, adj)

    final_mask = chosen_mask | best_extra_mask
    chosen = [orders[i] for i in range(n) if (final_mask >> i) & 1]
    total = sum(clean_values[wo] for wo in chosen)
    return DeliveryResult(
        work_orders=orders,
        required=req,
        values=clean_values,
        conflicts=conflicts,
        chosen=chosen,
        total_value=total,
    )


def _solve_free(free_bits: list[int], weights: list[int], adj: list[int]) -> tuple[int, int]:
    """在自由顶点子集上精确求最大权独立集，返回 (mask, value)。

    平局规则：同价值取“升序工单号列表字典序最小”。free_bits 按工单升序排列，
    因此对应掩码的高位是更大的工单号。比较两个候选集合时，逐项比较升序工单号，
    首个不同位置小者胜；这等价于比较“按位从高到低”的字典序——高位（更大工单号）
    更早出现差异者决定胜负。
    """
    if not free_bits:
        return 0, 0

    # 把自由顶点映射到紧凑的局部位（局部顺序沿用全局升序）。
    local_pos = {g: k for k, g in enumerate(free_bits)}
    m = len(free_bits)
    lw = [weights[g] for g in free_bits]
    ladj = [0] * m
    for k, g in enumerate(free_bits):
        neighbours = adj[g]
        mask = 0
        for other_g, other_k in local_pos.items():
            if (neighbours >> other_g) & 1:
                mask |= 1 << other_k
        ladj[k] = mask

    best_mask, best_value = 0, 0

    def better(mask_a: int) -> bool:
        """值相等时判断 mask_a 的升序工单号列表是否字典序更小。

        局部下标 0 对应最小工单号。两个不同的独立集不可能一个是另一个的子集
        且价值相等（价值均为正，真子集价值严格更小）。因此二者在某个最小工单号处
        出现差异；升序列表逐个比较时，该位置上"包含这个更小工单号"的集合胜出。
        """
        if mask_a == best_mask:
            return False
        diff = mask_a ^ best_mask
        low = diff & -diff                      # 最低的不同位 = 最小的差异工单号
        k = low.bit_length() - 1
        return bool((mask_a >> k) & 1)

    def dfs(candidate_free: int, acc_mask: int, acc_value: int) -> None:
        nonlocal best_mask, best_value
        if acc_value > best_value or (acc_value == best_value and better(acc_mask)):
            best_mask, best_value = acc_mask, acc_value
        # 分支定界：剩余所有可取顶点价值上界都不足以超过当前最优则剪枝；
        # 等值时仍需搜索以判定字典序，故仅用严格“不足以超过”剪枝。
        remaining = candidate_free
        upper = acc_value
        while remaining:
            low = remaining & -remaining
            k = low.bit_length() - 1
            upper += lw[k]
            remaining ^= low
        if upper < best_value:
            return

        if candidate_free == 0:
            return
        # 取当前候选里局部下标最小者（对应最小工单号）分支，两个分支都探索。
        low = candidate_free & -candidate_free
        k = low.bit_length() - 1
        bit = 1 << k
        rest = candidate_free & ~bit
        # 分支 1：不选 k
        dfs(rest, acc_mask, acc_value)
        # 分支 2：选 k（排除其邻居）
        dfs(rest & ~ladj[k], acc_mask | bit, acc_value + lw[k])

    dfs((1 << m) - 1, 0, 0)
    # 映射回全局 mask
    global_mask = 0
    for k, g in enumerate(free_bits):
        if (best_mask >> k) & 1:
            global_mask |= 1 << g
    return global_mask, best_value


def _describe_req_conflicts(evidence: list[dict[str, Any]]) -> str:
    parts = []
    for item in evidence:
        pair_desc = f"{item['work_order_a']}×{item['work_order_b']}"
        sources = []
        for rec in item["evidence"]:
            fixture = rec.get("fixture")
            station = f"{rec.get('station_a')}/{rec.get('station_b')}"
            overlap = rec.get("overlap")
            basis = rec.get("basis")
            sources.append(f"{fixture}:{station} {overlap} ({basis})")
        parts.append(f"[{pair_desc} -> " + "; ".join(sources) + "]")
    return " ".join(parts)
