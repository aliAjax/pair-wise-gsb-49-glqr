"""再保险合约与巨灾暴露管理领域规则与状态转换。

记账链采用两阶段模型：
- 核定(calculate)：只预占层容量与恢复次数，写 reserve；
- 结算(settle)：预占转为实耗，写 confirm 并累计恢复保费；
- 拒赔(reject)或事件撤回：预占释放，写 release。

本模块只做纯计算与状态判断，所有数据库内的容量复核在事务中完成。
"""
from typing import Any, Dict, Iterable, Tuple

from .domain import (
    CapacityExhausted,
    Conflict,
    EventInactive,
    STATE_BOUND,
    STATE_CALCULATED,
    STATE_CLAIM_SUBMITTED,
    STATE_INVALIDATED,
    STATE_QUOTED,
    STATE_REJECTED,
    STATE_SETTLED,
    ValidationError,
    integer,
    number,
    text,
)

INITIAL_STATE = STATE_QUOTED
CREATE_ROLES = {'underwriter'}
ACTION_ROLES = {
    'bind': {'underwriter'},
    'submit_claim': {'claims_officer'},
    'calculate': {'claims_officer'},
    'settle': {'finance'},
    'reject': {'finance', 'claims_officer'},
    # 事件注册/撤回由承保人负责，对账由管理员触发
    'register_event': {'underwriter'},
    'withdraw_event': {'underwriter'},
    'reconcile': {'admin'},
}
TRANSITIONS = {
    'bind': {STATE_QUOTED: STATE_BOUND},
    'submit_claim': {STATE_BOUND: STATE_CLAIM_SUBMITTED},
    'calculate': {STATE_CLAIM_SUBMITTED: STATE_CALCULATED},
    'settle': {STATE_CALCULATED: STATE_SETTLED},
    'reject': {STATE_CLAIM_SUBMITTED: STATE_REJECTED, STATE_CALCULATED: STATE_REJECTED},
}

# 金额/次数比较的容差
MONEY_EPS = 0.01


def round2(value: float) -> float:
    return round(float(value) + 0.0, 2)


class DomainRules:
    INITIAL_STATE = INITIAL_STATE

    def known_role(self, role: str) -> bool:
        all_roles = set(CREATE_ROLES)
        for roles in ACTION_ROLES.values():
            all_roles.update(roles)
        return role == "admin" or role in all_roles

    def role_can_create(self, role: str) -> bool:
        return role == "admin" or role in CREATE_ROLES

    def role_can_action(self, role: str, action: str) -> bool:
        return role == "admin" or role in ACTION_ROLES.get(action, set())

    # ---------- 巨灾事件 ----------

    def validate_event(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload or {})
        text(p, "event_id")
        text(p, "event_name")
        p["event_name"] = text(p, "event_name")
        p["occurred_on"] = text(p, "occurred_on")
        return p

    # ---------- 合约分层 ----------

    def validate_layer(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload or {})
        text(p, "layer_code")
        attachment = number(p, "attachment", 0)
        limit = number(p, "limit", 0)
        number(p, "cession_pct", 0, 1)
        number(p, "reinstatement_pct", 0, 1)
        integer(p, "reinstatement_total", 0, 100)
        if limit <= attachment:
            raise ValidationError("赔款限额必须高于起赔点")
        p["layer_width"] = round2(limit - attachment)
        # 层的可摊回容量（全损摊回上限）
        p["layer_capacity"] = round2(p["layer_width"] * float(p["cession_pct"]))
        return p

    def reinstatement_capacity(self, layer: Dict[str, Any]) -> float:
        """含恢复次数后的总容量 = 基础层容量 * (1 + 恢复次数)。"""
        return round2(float(layer["layer_capacity"]) * (1 + int(layer["reinstatement_total"])))

    @staticmethod
    def reinstatements_needed(layer: Dict[str, Any], folds: Dict[str, Any], recovery: float) -> int:
        """本笔预占需要消耗的恢复次数：首赔吃基础层（0次），超过基础容量后按层计次。"""
        base = float(layer["layer_capacity"])
        used = folds["confirmed_amount"] + folds["outstanding_amount"]
        if used + recovery <= base + MONEY_EPS:
            return 0
        # 跨越基础容量：从第2层起每层消耗1次恢复
        import math
        top_layer = math.ceil((used + recovery - MONEY_EPS) / base)
        return max(0, int(top_layer) - 1)

    # ---------- 赔案 ----------

    def validate_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload)
        text(p, "event_id")
        attachment = number(p, "attachment", 0)
        limit = number(p, "limit", 0)
        number(p, "cession_pct", 0, 1)
        number(p, "loss_amount", 0)
        number(p, "reinstatement_pct", 0, 1)
        number(p, "aggregate_prior", 0)
        if limit <= attachment:
            raise ValidationError("赔款限额必须高于起赔点")
        return p

    def layer_params_from_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """从旧式赔案载荷提取隐式合约分层参数。"""
        attachment = float(payload["attachment"])
        limit = float(payload["limit"])
        cession = float(payload["cession_pct"])
        return {
            "attachment": attachment,
            "limit": limit,
            "cession_pct": cession,
            "layer_width": round2(limit - attachment),
            "layer_capacity": round2((limit - attachment) * cession),
        }

    def recovery_for_loss(self, layer: Dict[str, Any], loss: float) -> float:
        """分层摊回：超过起赔点、不超过层宽，乘以分出比例。"""
        width = float(layer["layer_width"])
        retained = max(0.0, float(loss) - float(layer["attachment"]))
        return round2(min(retained, width) * float(layer["cession_pct"]))

    def reinstatement_premium_for(self, layer: Dict[str, Any], recovery: float) -> float:
        return round2(float(recovery) * float(layer["reinstatement_pct"]))

    def prepare_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = self.validate_create(payload)
        p.update(self.layer_params_from_payload(p))
        recovery = self.recovery_for_loss(p, float(p["loss_amount"]))
        p["recoverable_amount"] = recovery
        p["reinstatement_premium"] = self.reinstatement_premium_for(p, recovery)
        p["net_retention"] = round2(float(p["loss_amount"]) - recovery)
        # 新赔案创建时尚未核定，不预占任何容量
        p["reserved_amount"] = 0.0
        p["reserved_reinstatements"] = 0
        return p

    def check_create_conflicts(self, payload: Dict[str, Any], existing: Iterable[Dict[str, Any]]) -> None:
        """旧版即时容量检查（保留兼容）；新记账链的容量控制在核定事务中完成。"""
        event_id = payload.get("event_id")
        used = float(payload.get("aggregate_prior", 0))
        for item in existing:
            if item["state"] in {"rejected", "invalidated"} or item["payload"].get("event_id") != event_id:
                continue
            used += float(item["payload"].get("recoverable_amount", 0))
        capacity = float(payload["layer_width"]) * float(payload["cession_pct"])
        projected = min(max(0.0, float(payload["loss_amount"]) - float(payload["attachment"])), float(payload["layer_width"])) * float(payload["cession_pct"])
        if used + projected > capacity + MONEY_EPS:
            raise CapacityExhausted("同一事件累计摊回超过再保容量")

    def require_transition(self, record: Dict[str, Any], action: str) -> str:
        if record["state"] == STATE_INVALIDATED:
            raise EventInactive("赔案因巨灾事件撤回已失效，不能继续操作")
        allowed = TRANSITIONS.get(action, {}).get(record["state"])
        if allowed is None:
            raise Conflict("当前状态不允许执行%s" % action)
        return allowed

    def apply_action(self, record: Dict[str, Any], action: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any], str]:
        new_state = self.require_transition(record, action)
        data = dict(data or {})
        p = dict(record["payload"])
        changes: Dict[str, Any] = {}
        summary = ""
        if action == "bind":
            changes["bound_by"] = text(data, "underwriter_id")
            summary = "再保合约已绑定"
        elif action == "submit_claim":
            changes["claim_number"] = text(data, "claim_number")
            changes["claim_event_id"] = text(data, "event_id")
            summary = "赔案已提交"
        elif action == "calculate":
            loss = number(data, "approved_loss", 0)
            recovery = self.recovery_for_loss(p, loss)
            changes["approved_loss"] = loss
            changes["recoverable_amount"] = recovery
            changes["reinstatement_premium"] = self.reinstatement_premium_for(p, recovery)
            summary = "摊回金额已核定并预占层容量"
        elif action == "settle":
            if float(p.get("reserved_amount", p.get("recoverable_amount", 0))) <= 0:
                raise ValidationError("无可结算摊回")
            changes["payment_reference"] = text(data, "payment_reference")
            summary = "预占转为实耗，恢复保费已累计"
        elif action == "reject":
            changes["reject_reason"] = text(data, "reject_reason")
            summary = "赔案已拒绝，预占释放"
        p.update(changes)
        return new_state, p, summary or ("已执行%s" % action)

    # ---------- 容量/恢复次数复核（事务内调用） ----------

    @staticmethod
    def folds(entries: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        """把台账条目折叠成 (预占, 实耗, 释放) 的金额与次数。"""
        reserved_amount = confirmed_amount = released_amount = 0.0
        reserved_count = confirmed_count = released_count = 0
        for entry in entries:
            kind = entry["entry_type"]
            amount = float(entry["amount"])
            count = int(entry.get("reinstatement_count") or 0)
            if kind == "reserve":
                reserved_amount += amount
                reserved_count += count
            elif kind == "confirm":
                confirmed_amount += amount
                confirmed_count += count
            elif kind == "release":
                released_amount += amount
                released_count += count
        outstanding = reserved_amount - confirmed_amount - released_amount
        outstanding_count = reserved_count - confirmed_count - released_count
        return {
            "reserved_amount": round2(reserved_amount),
            "confirmed_amount": round2(confirmed_amount),
            "released_amount": round2(released_amount),
            "outstanding_amount": round2(outstanding),
            "reserved_count": reserved_count,
            "confirmed_count": confirmed_count,
            "released_count": released_count,
            "outstanding_count": outstanding_count,
        }

    def check_capacity(self, layer: Dict[str, Any], folds: Dict[str, Any], want_amount: float, want_count: int) -> None:
        """核定预占前：金额不超总容量，未用恢复次数足够。"""
        total_capacity = self.reinstatement_capacity(layer)
        if folds["confirmed_amount"] + folds["outstanding_amount"] + want_amount > total_capacity + MONEY_EPS:
            raise CapacityExhausted("层容量不足：预占后累计摊回%s超过总容量%s" % (
                round2(folds["confirmed_amount"] + folds["outstanding_amount"] + want_amount), total_capacity))
        available = int(layer["reinstatement_total"]) - folds["confirmed_count"] - folds["outstanding_count"]
        if want_count > available:
            raise CapacityExhausted("恢复次数不足：需要%s次，仅剩%s次" % (want_count, available))
