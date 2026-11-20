"""异常类型。"""
from __future__ import annotations


class PqScanError(Exception):
    """所有工具层错误的基类（不用于损坏 Parquet 恢复，仅做输入校验）。"""


class PlanError(PqScanError):
    """查询表达式或 schema 不支持。"""


class OccupancyError(PqScanError):
    """工单占位事件/互斥规则输入不合法，无法重建区间或冲突清单。"""


class DeliveryError(PqScanError):
    """交付选择输入不合法（缺价值、超出精确求解上限等）。"""


class DeliveryRejected(DeliveryError):
    """必须项之间互相冲突，整单交付请求被拒绝（附带冲突证据）。"""

    def __init__(self, message: str, evidence: list[dict]):
        super().__init__(message)
        self.evidence = evidence
