"""业务用例编排：巨灾事件、合约分层、赔案、恢复台账接成同一条记账链。

关键约定：
- 核定先预占(reserve)层容量和恢复次数，结算才确认消耗(confirm)并累计保费；
- 同一编号重放（create 同 reference、动作同 entry_no）返回原结果；
- 事件撤回后未结算记录失效重算，已结算保留原依据；
- 详情与审计一律标出来源：live / replay / reconcile / migration。
"""
from typing import Any, Dict, List, Optional

from .audit import AuditRecorder
from .domain import (
    Actor,
    Conflict,
    PermissionDenied,
    SOURCE_LIVE,
    SOURCE_REPLAY,
    text,
)
from .repository import Repository
from .rules import DomainRules


class Service:
    def __init__(self, repository: Repository, rules: DomainRules, audit: AuditRecorder = None) -> None:
        self.repository = repository
        self.rules = rules
        self.audit = audit or AuditRecorder(repository)
        # 注入事务内容量复核（供 reserve_capacity 在锁内调用）
        self.repository.set_capacity_checker(self.rules.check_capacity)
        self.repository.set_reinstatement_planner(self.rules.reinstatements_needed)

    @staticmethod
    def _actor(actor: Actor) -> Actor:
        if actor is None or not actor.user_id.strip() or not actor.role.strip():
            raise PermissionDenied("缺少调用身份")
        return actor

    def _ensure_known_role(self, actor: Actor) -> None:
        if not self.rules.known_role(actor.role):
            raise PermissionDenied("角色无权访问该服务")

    # ------------------------------------------------------------------ 事件/分层

    def register_event(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_action(actor.role, "register_event"):
            raise PermissionDenied("角色无权注册巨灾事件")
        event = self.rules.validate_event(payload or {})
        return self.repository.upsert_event(event, actor.user_id)

    def register_layer(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_create(actor.role):
            raise PermissionDenied("角色无权注册合约分层")
        layer = self.rules.validate_layer(payload or {})
        return self.repository.upsert_layer(layer, actor.user_id)

    def list_events(self, actor: Actor) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_events()

    def list_layers(self, actor: Actor) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_layers()

    def layer_balance(self, actor: Actor, layer_code: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        balance = self.repository.layer_balance(text({"layer_code": layer_code}, "layer_code"))
        if not balance:
            from .domain import NotFound
            raise NotFound("合约分层不存在")
        ledger = self.repository.list_ledger(layer_code=layer_code, limit=1)
        balance["outstanding_amount"] = round(
            float(balance["reserved_amount"]) - float(balance["confirmed_amount"])
            - float(balance["released_amount"]), 2)
        balance["outstanding_count"] = int(balance["reserved_count"]) - int(balance["confirmed_count"]) \
            - int(balance["released_count"])
        balance["last_entry"] = ledger[0] if ledger else None
        return balance

    def withdraw_event(self, actor: Actor, event_id: str, reason: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_action(actor.role, "withdraw_event"):
            raise PermissionDenied("角色无权撤回巨灾事件")
        event_id = text({"event_id": event_id}, "event_id")
        reason = text({"reason": reason or ""}, "reason")
        return self.repository.withdraw_event(event_id, actor.user_id, reason, source=SOURCE_LIVE)

    # ------------------------------------------------------------------ 赔案

    def create(self, actor: Actor, reference: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_create(actor.role):
            raise PermissionDenied("角色无权创建记录")
        reference = text({"reference": reference}, "reference")
        prepared = self.rules.prepare_create(payload or {})

        # 同一编号重放：完全一致则返回原结果；载荷不同则冲突，不做覆盖
        existing = self.repository.get_by_reference(reference)
        if existing is not None:
            if self._same_create(existing["payload"], prepared):
                existing["replayed"] = True
                existing["source"] = SOURCE_REPLAY
                return existing
            raise Conflict("reference已存在且内容不一致，拒绝重放覆盖")

        event_id = prepared["event_id"]
        layer_code = text(prepared, "layer_code") if prepared.get("layer_code") else "L-%s" % reference
        # 自动把巨灾事件和合约分层接入记账链（已存在则幂等跳过）
        self.repository.upsert_event({
            "event_id": event_id,
            "event_name": prepared.get("event_name", "事件%s" % event_id),
            "occurred_on": prepared.get("occurred_on", ""),
            "detail": {"auto_from_claim": reference},
        }, actor.user_id)
        self.repository.upsert_layer({
            "layer_code": layer_code,
            "attachment": float(prepared["attachment"]),
            "limit": float(prepared["limit"]),
            "cession_pct": float(prepared["cession_pct"]),
            "layer_width": float(prepared["layer_width"]),
            "layer_capacity": float(prepared["layer_capacity"]),
            "reinstatement_pct": float(prepared["reinstatement_pct"]),
            "reinstatement_total": int(prepared.get("reinstatement_total", 1)),
            "detail": {"auto_from_claim": reference},
        }, actor.user_id)
        record = self.repository.create(reference, self.rules.INITIAL_STATE, prepared, actor.user_id,
                                        event_id=event_id, layer_code=layer_code)
        record["replayed"] = False
        record["source"] = SOURCE_LIVE
        return record

    @staticmethod
    def _same_create(old: Dict[str, Any], new: Dict[str, Any]) -> bool:
        if str(old.get("event_id", "")) != str(new.get("event_id", "")):
            return False
        if str(old.get("layer_code", "")) != str(new.get("layer_code", "")):
            return False
        for key in ("attachment", "limit", "cession_pct", "loss_amount", "reinstatement_pct"):
            if float(old.get(key, 0) or 0) != float(new.get(key, 0) or 0):
                return False
        return True

    def list_records(self, actor: Actor, state: Optional[str] = None, limit: int = 100,
                     event_id: Optional[str] = None) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_records(state=state, limit=limit, event_id=event_id)

    def get_record(self, actor: Actor, record_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        record = self.repository.get(record_id)
        # 详情把同一记账链上的事件、分层、台账一并带出，每条均标出来源
        record["event"] = self.repository.get_event(record.get("event_id") or "")
        record["layer"] = self.repository.get_layer(record.get("layer_code") or "")
        ledger = self.repository.list_ledger(claim_id=record_id, limit=100)
        ledger.sort(key=lambda item: item["seq"])
        record["ledger"] = ledger
        record["ledger_sources"] = sorted({item["source"] for item in ledger})
        timeline = self.audit.timeline(record_id)
        record["audit_sources"] = sorted({item.get("source", SOURCE_LIVE) for item in timeline})
        return record

    def act(self, actor: Actor, record_id: int, expected_version: int, action: str,
            data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        action = text({"action": action}, "action")
        if not self.rules.role_can_action(actor.role, action):
            raise PermissionDenied("角色无权执行该操作")
        data = data or {}

        if action == "calculate":
            return self._calculate(actor, record_id, expected_version, data)
        if action == "settle":
            result = self.repository.confirm_consumption(
                claim_id=record_id, expected_version=int(expected_version),
                payment_reference=text(data, "payment_reference"), actor_id=actor.user_id)
            result["record"]["replayed"] = result["replayed"]
            result["record"]["source"] = SOURCE_REPLAY if result["replayed"] else SOURCE_LIVE
            return result["record"]
        if action == "reject":
            result = self.repository.reject_claim(
                claim_id=record_id, expected_version=int(expected_version),
                reject_reason=text(data, "reject_reason"), actor_id=actor.user_id)
            result["record"]["replayed"] = result["replayed"]
            result["record"]["source"] = SOURCE_REPLAY if result["replayed"] else SOURCE_LIVE
            return result["record"]

        # bind / submit_claim：纯状态动作
        record = self.repository.get(record_id)
        new_state, new_payload, summary = self.rules.apply_action(record, action, data)
        updated = self.repository.mutate(
            record_id=record_id, expected_version=int(expected_version), state=new_state,
            payload=new_payload, actor_id=actor.user_id, action=action,
            details={"summary": summary, "input": data, "from": record["state"], "to": new_state})
        updated["replayed"] = False
        updated["source"] = SOURCE_LIVE
        return updated

    def _calculate(self, actor: Actor, record_id: int, expected_version: int, data: Dict[str, Any]) -> Dict[str, Any]:
        record = self.repository.get(record_id)
        # 同编号重放：已有预占台账则原样返回，不重复占用
        reserve = next(
            (item for item in self.repository.list_ledger(claim_id=record_id, limit=10)
             if item["entry_no"] == "RSV-%s" % record_id),
            None,
        )
        if reserve is not None:
            record["replayed"] = True
            record["source"] = SOURCE_REPLAY
            record["replayed_entry"] = {"seq": reserve["seq"], "entry_no": reserve["entry_no"],
                                        "amount": reserve["amount"], "source": reserve["source"]}
            return record
        # 先做状态机与输入校验
        self.rules.require_transition(record, "calculate")
        from .domain import ValidationError
        approved_loss = data.get("approved_loss")
        if isinstance(approved_loss, bool) or not isinstance(approved_loss, (int, float)):
            raise ValidationError("approved_loss必须是非负数字")
        approved_loss = float(approved_loss)
        if approved_loss < 0:
            raise ValidationError("approved_loss不能小于0")
        layer = self.repository.get_layer(record.get("layer_code") or "") or record["payload"]
        recovery = self.rules.recovery_for_loss(layer, approved_loss)
        premium = self.rules.reinstatement_premium_for(layer, recovery)
        result = self.repository.reserve_capacity(
            claim_id=record_id, expected_version=int(expected_version), approved_loss=approved_loss,
            recovery=recovery, reinstatement_premium=premium, actor_id=actor.user_id)
        if result["replayed"]:
            result["record"]["replayed"] = True
            result["record"]["source"] = SOURCE_REPLAY
        else:
            result["record"]["replayed"] = False
            result["record"]["source"] = SOURCE_LIVE
            result["record"]["reserved_entry"] = {"seq": result["entry"]["seq"], "entry_no": result["entry"]["entry_no"]}
        return result["record"]

    def timeline(self, actor: Actor, record_id: int = None, limit: int = 200) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.audit.timeline(record_id, limit=limit)

    def ledger(self, actor: Actor, layer_code: str = None, event_id: str = None,
               claim_id: int = None, limit: int = 200) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_ledger(layer_code=layer_code, event_id=event_id,
                                           claim_id=claim_id, limit=limit)

    def reconcile(self, actor: Actor) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_action(actor.role, "reconcile"):
            raise PermissionDenied("角色无权执行对账")
        return self.repository.reconcile(actor_id=actor.user_id)

    def stats(self, actor: Actor) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        result = {"records": self.repository.stats()}
        result["premium"] = self.repository.premium_summary()
        return result

    # ------------------------------------------------------------------ 启动恢复

    def startup_recovery(self) -> Dict[str, Any]:
        """写入中断后重启：旧数据先补事件序列，再统一对账续作。"""
        backfill = None
        if self.repository.needs_legacy_backfill():
            backfill = self.repository.backfill_legacy()
        report = self.repository.reconcile(actor_id="system")
        return {"backfill": backfill, "reconcile": report}
