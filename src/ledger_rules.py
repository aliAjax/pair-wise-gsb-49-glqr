"""记账链纯规则：分层摊回、恢复次数占位与恢复保费。

约定（与旧原型口径一致）：
- 层容量 capacity = (限额 - 起赔点) * 分出比例；
- 每层允许 reinstatement_count 次恢复，第 0 槽为原始保障，总可恢复容量 = capacity * (1 + N)；
- 每份赔案占用一个恢复槽：核定(reserve)时预占，结算时确认，撤回事件时释放；
- 恢复保费 = 本次摊回 * 恢复费率（每次恢复同一费率）。
"""
from typing import Any, Dict, Optional, Set

from .domain import ValidationError, number, integer, text, optional_text

MONEY_EPS = 0.01
RULE_VERSION = "layer-recovery-v1"


def money(value: float) -> float:
    return round(float(value), 2)


def layer_width(attachment: float, limit_amount: float) -> float:
    return money(limit_amount - attachment)


def layer_capacity(attachment: float, limit_amount: float, cession_pct: float) -> float:
    return money(layer_width(attachment, limit_amount) * cession_pct)


def total_recovery_capacity(layer: Dict[str, Any]) -> float:
    capacity = layer_capacity(layer["attachment"], layer["limit_amount"], layer["cession_pct"])
    return money(capacity * (1 + int(layer["reinstatement_count"])))


def recoverable(loss_amount: float, layer: Dict[str, Any]) -> float:
    width = layer_width(layer["attachment"], layer["limit_amount"])
    retained = max(0.0, float(loss_amount) - float(layer["attachment"]))
    return money(min(retained, width) * float(layer["cession_pct"]))


def reinstatement_premium(amount: float, layer: Dict[str, Any]) -> float:
    return money(amount * float(layer["reinstatement_rate"]))


def choose_slot(active_slots: Set[int], reinstatement_count: int) -> Optional[int]:
    """在 0..N 中返回最小的空闲恢复槽；全部占用返回 None（由调用方报冲突）。"""
    for slot in range(0, int(reinstatement_count) + 1):
        if slot not in active_slots:
            return slot
    return None


def capacity_fits(used_amount: float, amount: float, layer: Dict[str, Any]) -> bool:
    return used_amount + amount <= total_recovery_capacity(layer) + MONEY_EPS


class LedgerRules:
    RULE_VERSION = RULE_VERSION

    def validate_event(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(payload or {})
        data.pop("request_id", None)
        data["event_code"] = text(data, "event_code")
        data["name"] = optional_text(data, "name")
        return data

    def validate_layer(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(payload or {})
        data.pop("request_id", None)
        data["layer_code"] = text(data, "layer_code")
        data["contract_ref"] = optional_text(data, "contract_ref")
        attachment = number(data, "attachment", 0)
        limit_amount = number(data, "limit_amount", 0)
        cession_pct = number(data, "cession_pct", 0, 1)
        reinstatement_count = integer(data, "reinstatement_count", 0, 20)
        reinstatement_rate = number(data, "reinstatement_rate", 0)
        if limit_amount <= attachment:
            raise ValidationError("赔款限额必须高于起赔点")
        data["attachment"] = attachment
        data["limit_amount"] = limit_amount
        data["cession_pct"] = cession_pct
        data["reinstatement_count"] = reinstatement_count
        data["reinstatement_rate"] = reinstatement_rate
        return data

    def validate_assess(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(payload or {})
        data.pop("request_id", None)
        data["claim_number"] = text(data, "claim_number")
        event_ref = data.get("event_code") or data.get("event_id")
        if not isinstance(event_ref, (int, str)) or not str(event_ref).strip():
            raise ValidationError("event_code或event_id至少提供一个")
        layer_ref = data.get("layer_code") or data.get("layer_id")
        if not isinstance(layer_ref, (int, str)) or not str(layer_ref).strip():
            raise ValidationError("layer_code或layer_id至少提供一个")
        data["loss_amount"] = number(data, "loss_amount", 0)
        return data

    def validate_settle(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(payload or {})
        data.pop("request_id", None)
        data["payment_ref"] = text(data, "payment_ref")
        claim_ref = data.get("claim_number") or data.get("claim_id")
        if not isinstance(claim_ref, (int, str)) or not str(claim_ref).strip():
            raise ValidationError("claim_number或claim_id至少提供一个")
        return data

    def validate_withdraw(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(payload or {})
        data.pop("request_id", None)
        data["reason"] = optional_text(data, "reason")
        return data
