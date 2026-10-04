"""记账链用例编排。

记账口径：核定(assess)先预占层容量与恢复槽、预估恢复保费；
结算(settle)才确认消耗、累计保费。同一 request_id 重放返回首次结果；
最后一次恢复槽由数据库唯一索引仲裁，只收先到者。
"""
import json
from typing import Any, Dict, List, Optional

from . import ledger_rules as lr
from .domain import Actor, Conflict, DomainError, NotFound, PermissionDenied, ValidationError
from .ledger_rules import LedgerRules
from .ledger_store import LedgerStore, _now

SYSTEM_ACTOR = "system-recovery"
LEDGER_ROLES = {
    "create_event": {"cat_analyst"},
    "create_layer": {"underwriter"},
    "assess": {"claims_officer"},
    "settle": {"finance"},
    "withdraw": {"cat_analyst"},
    "reconcile": {"admin"},
}


def _error_class(code: str) -> type:
    return {
        "conflict": Conflict,
        "not_found": NotFound,
        "validation_error": ValidationError,
        "permission_denied": PermissionDenied,
    }.get(code, DomainError)


class LedgerService:
    def __init__(self, store: LedgerStore, rules: LedgerRules = None) -> None:
        self.store = store
        self.rules = rules or LedgerRules()

    # ---------- 身份 ----------
    @staticmethod
    def _actor(actor: Optional[Actor]) -> Actor:
        if actor is None or not actor.user_id.strip() or not actor.role.strip():
            raise PermissionDenied("缺少调用身份")
        return actor

    def _authorize(self, actor: Actor, command: str) -> None:
        if actor.role != "admin" and actor.role not in LEDGER_ROLES.get(command, set()):
            raise PermissionDenied("角色无权执行%s" % command)

    # ---------- 命令台账驱动 ----------
    def _execute(self, actor: Actor, command: str, request_id: str, payload: Dict[str, Any],
                 source: str = "live") -> Dict[str, Any]:
        handler = getattr(self, "_do_" + command)
        existing = self.store.get_command(request_id)
        if existing is not None:
            return self._replay(existing, payload)
        # 先登记 pending 命令（单独小事务），崩溃后可据此续作。
        self.store.stage_command(request_id, command, actor.user_id, payload, source)
        try:
            with self.store.transaction() as connection:
                command_row = connection.execute(
                    "SELECT id FROM idemp_commands WHERE request_id=?", (request_id,)
                ).fetchone()
                return handler(connection, actor.user_id, payload, request_id, source, False,
                               int(command_row["id"]))
        except Exception as exc:
            self._mark_failed(request_id, exc)
            raise

    def _resume(self, command: Dict[str, Any]) -> Dict[str, Any]:
        handler = getattr(self, "_do_" + command["command"])
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM idemp_commands WHERE id=? AND status='pending'", (int(command["id"]),)
            ).fetchone()
            if row is None:
                current = self.store.get_command(command["request_id"])
                if current and current["status"] == "completed" and current["response"] is not None:
                    response = dict(current["response"])
                    response["source"] = "replay"
                    return response
                raise Conflict("命令状态无法续作")
            try:
                return handler(connection, command["actor_id"], command["payload"],
                               command["request_id"], command.get("source", "recovery"), True,
                               int(command["id"]))
            except Exception as exc:
                self._mark_failed(command["request_id"], exc)
                raise

    def _replay(self, existing: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        if payload != existing["payload"]:
            raise Conflict("同一request_id的请求内容与首次提交不一致")
        if existing["status"] == "completed" and existing["response"] is not None:
            response = json.loads(json.dumps(existing["response"]))
            response["source"] = "replay"
            response["replayed"] = True
            return response
        if existing["status"] == "failed":
            error_cls = _error_class(existing.get("error_code") or "domain_error")
            raise error_cls(existing.get("error_message") or "命令此前执行失败")
        raise Conflict("命令仍在处理中，请稍后重试")

    def _mark_failed(self, request_id: str, exc: Exception) -> None:
        code = getattr(exc, "code", "domain_error")
        message = str(exc) or exc.__class__.__name__
        with self.store.transaction() as connection:
            connection.execute(
                "UPDATE idemp_commands SET status='failed', error_code=?, error_message=?, completed_at=?"
                " WHERE request_id=?",
                (code, message, _now(), request_id),
            )

    def _finish(self, connection, request_id: Optional[str], response: Dict[str, Any]) -> Dict[str, Any]:
        if request_id:
            self.store.complete_command(connection, request_id, response)
        return response

    # ---------- 事件 ----------
    def create_event(self, actor: Actor, payload: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._authorize(actor, "create_event")
        data = self.rules.validate_event(payload)
        return self._execute(actor, "create_event", request_id, data)

    def _do_create_event(self, connection, actor_id: str, data: Dict[str, Any], request_id: str,
                         source: str, resumed: bool, command_id: Optional[int] = None) -> Dict[str, Any]:
        duplicate = self.store.find_command_by(connection, "event_code", data["event_code"], "create_event",
                                               exclude_id=command_id)
        if duplicate is not None:
            return self._finish_duplicate(connection, duplicate, data, request_id)
        seq_no = self.store.next_event_seq(connection)
        event = self.store.insert_event(connection, data["event_code"], seq_no, data.get("name", ""),
                                        data, actor_id, source=source)
        response = dict(event)
        response["source"] = source
        self.store.audit(connection, "event", event["id"], "create_event", actor_id,
                         {"event_code": event["event_code"], "seq_no": seq_no, "source": source},
                         source=source, event_id=event["id"])
        return self._finish(connection, request_id, response)

    def get_event(self, actor: Optional[Actor], identifier: Any) -> Dict[str, Any]:
        actor = self._actor(actor)
        with self.store.read_connection() as connection:
            event = self.store.find_event(connection, identifier)
            event["source"] = event.get("source", "live")
            claims = []
            for claim in self.store.list_claims(event_id=event["id"]):
                layer = self.store.get_layer(connection, claim["layer_id"])
                entries = self.store.ledger_entries_for_claim(connection, claim["id"])
                claims.append(self._claim_view(connection, claim, event, layer, entries))
            event["claims"] = claims
        return event

    def list_events(self, actor: Optional[Actor]) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        return self.store.list_events()

    def withdraw_event(self, actor: Actor, identifier: Any, payload: Dict[str, Any],
                       request_id: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._authorize(actor, "withdraw")
        data = self.rules.validate_withdraw(payload)
        data["event"] = identifier
        return self._execute(actor, "withdraw_event", request_id, data)

    def _do_withdraw_event(self, connection, actor_id: str, data: Dict[str, Any], request_id: str,
                           source: str, resumed: bool, command_id: Optional[int] = None) -> Dict[str, Any]:
        event = self.store.find_event(connection, data["event"])
        if event["status"] == "withdrawn":
            response = self._withdrawn_response(event, source)
            return self._finish_duplicate(connection, None, data, request_id, fallback_response=response)
        active = self.store.active_entries_for_event(connection, event["id"])
        released_amount = lr.money(sum(float(item["amount"]) for item in active if item["status"] == "reserved"))
        settled = [item for item in active if item["status"] == "confirmed"]
        reserved_claims = self.store.void_event_claims(connection, event["id"])
        self.store.withdraw_event(connection, event["id"], data.get("reason", ""))
        event = self.store.get_event(connection, event["id"])
        response = dict(event)
        response["source"] = source
        response["released_claims"] = reserved_claims
        response["released_amount"] = released_amount
        response["kept_settled"] = [
            {"claim_id": item["claim_id"], "ledger_id": item["id"],
             "amount": lr.money(item["amount"]),
             "reinstatement_premium": lr.money(item["reinstatement_premium"]),
             "basis": item["basis"]}
            for item in settled
        ]
        self.store.audit(connection, "event", event["id"], "withdraw", actor_id,
                         {"reason": data.get("reason", ""), "void_claims": reserved_claims,
                          "released_amount": released_amount,
                          "kept_settled": response["kept_settled"], "source": source},
                         source=source, event_id=event["id"])
        return self._finish(connection, request_id, response)

    @staticmethod
    def _withdrawn_response(event: Dict[str, Any], source: str) -> Dict[str, Any]:
        response = dict(event)
        response["source"] = source
        response["replayed"] = True
        return response

    # ---------- 分层 ----------
    def create_layer(self, actor: Actor, payload: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._authorize(actor, "create_layer")
        data = self.rules.validate_layer(payload)
        return self._execute(actor, "create_layer", request_id, data)

    def _do_create_layer(self, connection, actor_id: str, data: Dict[str, Any], request_id: str,
                         source: str, resumed: bool, command_id: Optional[int] = None) -> Dict[str, Any]:
        duplicate = self.store.find_command_by(connection, "layer_code", data["layer_code"], "create_layer",
                                               exclude_id=command_id)
        if duplicate is not None:
            return self._finish_duplicate(connection, duplicate, data, request_id)
        layer = self.store.insert_layer(connection, data, actor_id, source=source)
        response = self._layer_view(connection, layer)
        response["source"] = source
        self.store.audit(connection, "layer", layer["id"], "create_layer", actor_id,
                         {"layer_code": layer["layer_code"], "source": source}, source=source)
        return self._finish(connection, request_id, response)

    def get_layer(self, actor: Optional[Actor], identifier: Any) -> Dict[str, Any]:
        actor = self._actor(actor)
        with self.store.read_connection() as connection:
            layer = self.store.find_layer(connection, identifier)
            return self._layer_view(connection, layer)

    def list_layers(self, actor: Optional[Actor]) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        with self.store.read_connection() as connection:
            layers = self.store.list_layers()
            return [self._layer_view(connection, layer) for layer in layers]

    def _layer_view(self, connection, layer: Dict[str, Any]) -> Dict[str, Any]:
        view = dict(layer)
        view["capacity"] = lr.layer_capacity(layer["attachment"], layer["limit_amount"], layer["cession_pct"])
        view["layer_width"] = lr.layer_width(layer["attachment"], layer["limit_amount"])
        view["total_recovery_capacity"] = lr.total_recovery_capacity(layer)
        view["position"] = self.store.layer_position(connection, layer["id"])
        view.setdefault("source", "live")
        return view

    # ---------- 核定：预占 ----------
    def assess(self, actor: Actor, payload: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._authorize(actor, "assess")
        data = self.rules.validate_assess(payload)
        return self._execute(actor, "assess", request_id, data)

    def _do_assess(self, connection, actor_id: str, data: Dict[str, Any], request_id: str,
                   source: str, resumed: bool, command_id: Optional[int] = None) -> Dict[str, Any]:
        duplicate = self.store.find_command_by(connection, "claim_number", data["claim_number"], "assess",
                                               exclude_id=command_id)
        if duplicate is not None:
            return self._finish_duplicate(connection, duplicate, data, request_id)
        event = self.store.find_event(connection, data.get("event_id") or data.get("event_code"))
        if event["status"] != "active":
            raise Conflict("事件已撤回，不能再核定赔案")
        layer = self.store.find_layer(connection, data.get("layer_id") or data.get("layer_code"))
        amount = lr.recoverable(data["loss_amount"], layer)
        if amount <= 0:
            raise Conflict("核定摊回为零，无需占用恢复次数")
        position = self.store.layer_position(connection, layer["id"])
        if not lr.capacity_fits(position["used_capacity"], amount, layer):
            raise Conflict("同一分层累计摊回超过总恢复容量")
        slot = lr.choose_slot(set(position["active_slots"]), int(layer["reinstatement_count"]))
        if slot is None:
            raise Conflict("恢复次数已用尽")
        premium = lr.reinstatement_premium(amount, layer)
        basis = {
            "rule_version": self.rules.RULE_VERSION,
            "event_code": event["event_code"],
            "event_seq_no": event["seq_no"],
            "layer_code": layer["layer_code"],
            "attachment": layer["attachment"],
            "limit_amount": layer["limit_amount"],
            "cession_pct": layer["cession_pct"],
            "reinstatement_rate": layer["reinstatement_rate"],
            "loss_amount": lr.money(data["loss_amount"]),
            "computed_at": _now(),
        }
        claim_id = self.store.insert_claim(
            connection,
            {"claim_number": data["claim_number"], "event_id": event["id"], "layer_id": layer["id"],
             "loss_amount": data["loss_amount"], "reserved_amount": amount,
             "reinstatement_no": slot, "basis": basis},
            actor_id, source=source,
        )
        claim = self.store.get_claim(connection, claim_id)
        entry_no = self.store.next_ledger_entry_no(connection, layer["id"])
        entry = {
            "claim_id": claim_id, "event_id": event["id"], "layer_id": layer["id"],
            "reinstatement_no": slot, "entry_no": entry_no, "amount": amount,
            "reinstatement_premium": premium, "status": "reserved",
            "basis": basis, "source": source,
        }
        try:
            entry["id"] = self.store.insert_ledger_entry(connection, entry)
        except Conflict:
            # 并发争抢同一恢复槽：唯一索引仲裁，后来者失败。
            raise Conflict("恢复次数已被并发请求占用，请按剩余容量重试")
        response = self._claim_view(connection, claim, event, layer, [entry])
        response["source"] = source
        self.store.audit(connection, "claim", claim_id, "assess", actor_id,
                         {"claim_number": data["claim_number"], "event_code": event["event_code"],
                          "layer_code": layer["layer_code"], "reinstatement_no": slot,
                          "reserved_amount": amount, "estimated_premium": premium,
                          "entry_no": entry_no, "source": source},
                         source=source, event_id=event["id"])
        self.store.audit(connection, "layer", layer["id"], "reserve", actor_id,
                         {"claim_id": claim_id, "reinstatement_no": slot, "amount": amount,
                          "reinstatement_premium": premium, "source": source}, source=source)
        return self._finish(connection, request_id, response)

    # ---------- 结算：确认 ----------
    def settle(self, actor: Actor, payload: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._authorize(actor, "settle")
        data = self.rules.validate_settle(payload)
        return self._execute(actor, "settle", request_id, data)

    def _do_settle(self, connection, actor_id: str, data: Dict[str, Any], request_id: str,
                   source: str, resumed: bool, command_id: Optional[int] = None) -> Dict[str, Any]:
        duplicate = self.store.find_command_by(connection, "payment_ref", data["payment_ref"], "settle",
                                               exclude_id=command_id)
        if duplicate is not None:
            return self._finish_duplicate(connection, duplicate, data, request_id)
        claim = self.store.find_claim(connection, data.get("claim_id") or data.get("claim_number"))
        if claim["status"] == "settled":
            if claim["payment_ref"] != data["payment_ref"]:
                raise Conflict("赔案已用其他付款编号结算")
            return self._finish_duplicate(connection, None, data, request_id,
                                          fallback_response=self._settled_view(connection, claim, source, True))
        if claim["status"] != "assessed":
            raise Conflict("仅已核定赔案可以结算，当前状态:%s" % claim["status"])
        event = self.store.get_event(connection, claim["event_id"])
        if event["status"] != "active":
            raise Conflict("事件已撤回，未结算预占已失效，不能结算")
        layer = self.store.get_layer(connection, claim["layer_id"])
        amount = claim["reserved_amount"]
        premium = lr.reinstatement_premium(amount, layer)
        try:
            self.store.settle_claim(connection, claim["id"], data["payment_ref"], amount, premium)
        except Exception as exc:
            raise Conflict("付款编号已被其他赔案使用") from exc
        claim = self.store.get_claim(connection, claim["id"])
        entries = self.store.ledger_entries_for_claim(connection, claim["id"])
        response = self._claim_view(connection, claim, event, layer, entries)
        response["source"] = source
        self.store.audit(connection, "claim", claim["id"], "settle", actor_id,
                         {"payment_ref": data["payment_ref"], "confirmed_amount": amount,
                          "reinstatement_premium": premium, "source": source},
                         source=source, event_id=event["id"])
        self.store.audit(connection, "layer", layer["id"], "confirm", actor_id,
                         {"claim_id": claim["id"], "amount": amount,
                          "reinstatement_premium": premium, "source": source}, source=source)
        return self._finish(connection, request_id, response)

    def _settled_view(self, connection, claim: Dict[str, Any], source: str, replayed: bool) -> Dict[str, Any]:
        event = self.store.get_event(connection, claim["event_id"])
        layer = self.store.get_layer(connection, claim["layer_id"])
        entries = self.store.ledger_entries_for_claim(connection, claim["id"])
        view = self._claim_view(connection, claim, event, layer, entries)
        view["source"] = source
        view["replayed"] = replayed
        return view

    def _claim_view(self, connection, claim: Dict[str, Any], event: Dict[str, Any], layer: Dict[str, Any],
                    entries: List[Dict[str, Any]]) -> Dict[str, Any]:
        view = dict(claim)
        view["event_code"] = event["event_code"]
        view["event_seq_no"] = event["seq_no"]
        view["event_status"] = event["status"]
        view["layer_code"] = layer["layer_code"]
        view["ledger"] = [
            {"id": item["id"], "entry_no": item["entry_no"], "reinstatement_no": item["reinstatement_no"],
             "amount": lr.money(item["amount"]),
             "reinstatement_premium": lr.money(item["reinstatement_premium"]),
             "status": item["status"], "source": item.get("source", "live")}
            for item in entries
        ]
        view.setdefault("source", "live")
        return view

    def get_claim(self, actor: Optional[Actor], identifier: Any) -> Dict[str, Any]:
        actor = self._actor(actor)
        with self.store.read_connection() as connection:
            claim = self.store.find_claim(connection, identifier)
            event = self.store.get_event(connection, claim["event_id"])
            layer = self.store.get_layer(connection, claim["layer_id"])
            entries = self.store.ledger_entries_for_claim(connection, claim["id"])
            return self._claim_view(connection, claim, event, layer, entries)

    def list_claims(self, actor: Optional[Actor], event_identifier: Any = None,
                    layer_identifier: Any = None) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        with self.store.read_connection() as connection:
            event_id = None
            layer_id = None
            if event_identifier is not None:
                event_id = self.store.find_event(connection, event_identifier)["id"]
            if layer_identifier is not None:
                layer_id = self.store.find_layer(connection, layer_identifier)["id"]
            claims = self.store.list_claims(event_id=event_id, layer_id=layer_id)
            views = []
            for claim in claims:
                event = self.store.get_event(connection, claim["event_id"])
                layer = self.store.get_layer(connection, claim["layer_id"])
                entries = self.store.ledger_entries_for_claim(connection, claim["id"])
                views.append(self._claim_view(connection, claim, event, layer, entries))
            return views

    def timeline(self, actor: Optional[Actor], entity_type: str, entity_id: int) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        if entity_type == "event":
            return self.store.event_timeline(entity_id)
        return self.store.chain_timeline(entity_type, entity_id)

    # ---------- 重放辅助 ----------
    def _finish_duplicate(self, connection, duplicate: Optional[Dict[str, Any]], payload: Dict[str, Any],
                          request_id: str, fallback_response: Dict[str, Any] = None) -> Dict[str, Any]:
        if duplicate is not None:
            if duplicate["status"] == "completed" and duplicate["response"] is not None:
                if duplicate["payload"] != payload:
                    raise Conflict("业务编号已被其他请求占用")
                self.store.complete_command(connection, request_id, duplicate["response"])
                response = json.loads(json.dumps(duplicate["response"]))
                response["source"] = "replay"
                response["replayed"] = True
                return response
            if duplicate["status"] == "pending":
                raise Conflict("相同业务编号的请求正在处理中")
            raise Conflict("业务编号已被失败请求占用，请更换编号")
        if fallback_response is None:
            raise Conflict("重复请求")
        self.store.complete_command(connection, request_id, fallback_response)
        response = dict(fallback_response)
        response["source"] = "replay"
        response["replayed"] = True
        return response

    # ---------- 启动续作与对账 ----------
    def startup_recovery(self) -> Dict[str, Any]:
        report = {"reconcile": None, "resumed": [], "resume_failures": [], "backfill": None}
        # 先修复中断留下的台账错位/漏登，再续作挂起命令，避免新预占撞上残留状态。
        report["reconcile"] = self.reconcile(Actor(SYSTEM_ACTOR, "admin"), run_kind="startup")
        for command in self.store.all_pending_commands():
            try:
                result = self._resume(command)
                report["resumed"].append({"request_id": command["request_id"],
                                          "command": command["command"],
                                          "source": result.get("source", "replay")})
            except Exception as exc:  # 单条失败不阻断其余续作
                report["resume_failures"].append(
                    {"request_id": command["request_id"], "command": command["command"],
                     "error": getattr(exc, "code", "error"), "message": str(exc)})
        report["backfill"] = self.backfill_legacy(Actor(SYSTEM_ACTOR, "admin"), run_kind="startup")
        self.store.save_recovery_run("startup", report)
        return report

    def reconcile(self, actor: Actor, run_kind: str = "manual") -> Dict[str, Any]:
        actor = self._actor(actor)
        if actor.role != "admin" and actor.user_id != SYSTEM_ACTOR:
            raise PermissionDenied("仅管理员可触发对账")
        report = {"repaired_ledger": [], "repaired_claims": [], "sequence_gaps": [], "anomalies": []}
        with self.store.transaction() as connection:
            # 1) 台账状态与赔案状态不一致：先以赔案状态为准修正并同步确认金额，
            #    否则错位行会被下面的孤儿查询误认领。
            for item in self.store.mismatched_ledger(connection):
                target = "confirmed" if item["claim_status"] == "settled" else "reserved"
                row = connection.execute(
                    "SELECT amount FROM reinstatement_ledger WHERE id=?",
                    (item["ledger_id"],)).fetchone()
                connection.execute("UPDATE reinstatement_ledger SET status=?, updated_at=? WHERE id=?",
                                   (target, _now(), item["ledger_id"]))
                if target == "confirmed":
                    connection.execute(
                        "UPDATE claims SET confirmed_amount=?, settled_at=COALESCE(settled_at, ?),"
                        " payment_ref=COALESCE(payment_ref, 'RECONCILE-' || ?) WHERE id=?",
                        (float(row["amount"]), _now(), item["claim_id"], item["claim_id"]))
                report["repaired_claims"].append({"ledger_id": item["ledger_id"], "status": target})
            # 2) 已核定/已结算赔案缺有效台账：按核定依据补登。
            for claim in self.store.orphan_claims(connection):
                layer = self.store.get_layer(connection, claim["layer_id"])
                amount = claim["reserved_amount"] or claim["confirmed_amount"]
                status = "confirmed" if claim["status"] == "settled" else "reserved"
                entry_no = self.store.next_ledger_entry_no(connection, layer["id"])
                entry = {
                    "claim_id": claim["id"], "event_id": claim["event_id"], "layer_id": layer["id"],
                    "reinstatement_no": int(claim["reinstatement_no"] or 0), "entry_no": entry_no,
                    "amount": amount, "reinstatement_premium": lr.reinstatement_premium(amount, layer),
                    "status": status, "basis": claim.get("basis", {}), "source": "recovery",
                }
                try:
                    self.store.insert_ledger_entry(connection, entry)
                    report["repaired_ledger"].append({"claim_id": claim["id"], "entry_no": entry_no})
                except Conflict:
                    report["anomalies"].append({"claim_id": claim["id"], "reason": "恢复槽被占，需人工核查"})
            # 3) 事件序列空洞。
            report["sequence_gaps"] = self.store.sequence_gaps(connection)
            self.store.audit(connection, "system", None, "reconcile", actor.user_id,
                             {"kind": run_kind, "report": report, "source": "recovery"},
                             source="recovery")
        self.store.save_recovery_run("reconcile", report)
        return report

    def backfill_legacy(self, actor: Actor, run_kind: str = "manual") -> Dict[str, Any]:
        """旧 records 数据补事件序列、接层与赔案，不改动旧表。"""
        actor = self._actor(actor)
        if actor.role != "admin" and actor.user_id != SYSTEM_ACTOR:
            raise PermissionDenied("仅管理员可触发旧数据回填")
        if not self.store.has_table("records"):
            return {"linked": 0, "skipped": 0, "events": {}, "source": "legacy"}
        report = {"linked": 0, "skipped": 0, "events": {}, "layers": [], "claims": [], "source": "legacy"}
        with self.store.transaction() as connection:
            for row in self.store.list_legacy_records(connection):
                if self.store.legacy_link(connection, int(row["id"])) is not None:
                    report["skipped"] += 1
                    continue
                self._backfill_one(connection, row, report)
            self.store.set_meta(connection, "legacy_backfill", "done")
            self.store.audit(connection, "system", None, "backfill_legacy", actor.user_id,
                             {"kind": run_kind, "report": report, "source": "legacy"}, source="legacy")
        self.store.save_recovery_run("backfill", report)
        return report

    def _backfill_one(self, connection, row: Any, report: Dict[str, Any]) -> None:
        payload = json.loads(row["payload"])
        now = _now()
        event_code = str(payload.get("event_id") or ("EVENT-RECORD-%s" % row["id"]))
        existing = self.store._event_by_identifier(connection, event_code)
        if existing is None:
            seq_no = self.store.next_event_seq(connection)
            event = self.store.insert_event(connection, event_code, seq_no, "",
                                            {"event_code": event_code}, "legacy-backfill", source="legacy")
            self.store.audit(connection, "event", event["id"], "import_event", "legacy-backfill",
                             {"record_id": row["id"], "source": "legacy"}, source="legacy",
                             event_id=event["id"])
            report["events"][event_code] = event["id"]
        else:
            event = self.store._event_row(existing)
        layer_code = "LAYER-RECORD-%s" % row["id"]
        layer = self.store.insert_layer(
            connection,
            {"layer_code": layer_code, "contract_ref": row["reference"],
             "attachment": float(payload.get("attachment", 0)),
             "limit_amount": float(payload.get("limit", 0)),
             "cession_pct": float(payload.get("cession_pct", 0)),
             "reinstatement_count": 1,
             "reinstatement_rate": float(payload.get("reinstatement_pct", 0))},
            "legacy-backfill", source="legacy", legacy_record_id=int(row["id"]),
        )
        self.store.audit(connection, "layer", layer["id"], "import_layer", "legacy-backfill",
                         {"record_id": row["id"], "source": "legacy"}, source="legacy")
        report["layers"].append(layer_code)
        claim_id = None
        state = row["state"]
        if state in ("claim_submitted", "calculated", "settled"):
            claim_number = payload.get("claim_number") or ("CLM-RECORD-%s" % row["id"])
            amount = float(payload.get("recoverable_amount", 0))
            claim_status = "settled" if state == "settled" else "assessed"
            basis = {"imported_from_record": row["id"], "state_at_import": state,
                     "loss_amount": payload.get("loss_amount"),
                     "approved_loss": payload.get("approved_loss"),
                     "source": "legacy"}
            claim_id = self.store.insert_claim(
                connection,
                {"claim_number": claim_number, "event_id": event["id"], "layer_id": layer["id"],
                 "loss_amount": float(payload.get("approved_loss") or payload.get("loss_amount") or 0),
                 "reserved_amount": amount if claim_status == "assessed" else 0,
                 "confirmed_amount": amount if claim_status == "settled" else 0,
                 "reinstatement_no": 0, "payment_ref": payload.get("payment_reference"),
                 "basis": basis},
                "legacy-backfill", source="legacy", legacy_record_id=int(row["id"]),
                status=claim_status, now=now,
            )
            entry = {
                "claim_id": claim_id, "event_id": event["id"], "layer_id": layer["id"],
                "reinstatement_no": 0,
                "entry_no": self.store.next_ledger_entry_no(connection, layer["id"]),
                "amount": amount,
                "reinstatement_premium": float(payload.get("reinstatement_premium", 0)),
                "status": "confirmed" if claim_status == "settled" else "reserved",
                "basis": basis, "source": "legacy", "created_at": now, "updated_at": now,
            }
            self.store.insert_ledger_entry(connection, entry)
            self.store.audit(connection, "claim", claim_id, "import_claim", "legacy-backfill",
                             {"record_id": row["id"], "state": state, "source": "legacy"},
                             source="legacy", event_id=event["id"])
            report["claims"].append(claim_number)
        self.store.save_legacy_link(connection, int(row["id"]), event["id"], layer["id"], claim_id, now)
        report["linked"] += 1

    def enrich_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        with self.store.read_connection() as connection:
            link = self.store.legacy_link(connection, int(record["id"]))
            if link is None:
                record["chain"] = None
                return record
            chain = {"source": "legacy", "event_id": link["event_id"], "layer_id": link["layer_id"],
                     "claim_id": link["claim_id"]}
            event = self.store.get_event(connection, link["event_id"])
            chain["event_code"] = event["event_code"]
            chain["event_seq_no"] = event["seq_no"]
            chain["event_status"] = event["status"]
            layer = self.store.get_layer(connection, link["layer_id"])
            chain["layer_position"] = self.store.layer_position(connection, layer["id"])
            record["chain"] = chain
            record["source"] = "legacy"
            return record

    def record_link(self, record_id: int) -> Optional[Dict[str, Any]]:
        return self.store.record_link(record_id)
