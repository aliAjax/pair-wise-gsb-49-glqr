"""HTTP 路由与统一错误输出。"""
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from .domain import Actor, DomainError, PermissionDenied, ValidationError


RECORD_RE = re.compile(r"^/api/records/(\d+)$")
ACTION_RE = re.compile(r"^/api/records/(\d+)/actions/([a-z_]+)$")
AUDIT_RE = re.compile(r"^/api/records/(\d+)/audit$")
EVENT_RE = re.compile(r"^/api/events/([A-Za-z0-9_\-\.]+)$")
EVENT_WITHDRAW_RE = re.compile(r"^/api/events/([A-Za-z0-9_\-\.]+)/withdraw$")
LAYER_RE = re.compile(r"^/api/layers/([A-Za-z0-9_\-\.]+)$")
CLAIM_RE = re.compile(r"^/api/claims/([A-Za-z0-9_\-\.]+)$")
CHAIN_AUDIT_RE = re.compile(r"^/api/(events|layers|claims)/(\d+)/audit$")


def make_handler(service: Any, static_dir: Path):
    class Handler(BaseHTTPRequestHandler):
        server_version = "reinsurance-exposure/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _actor(self) -> Actor:
            user_id = self.headers.get("X-User-Id", "").strip()
            role = self.headers.get("X-Role", "").strip()
            if not user_id or not role:
                raise PermissionDenied("缺少X-User-Id或X-Role")
            return Actor(user_id=user_id, role=role, organization=self.headers.get("X-Org", ""))

        def _body(self) -> Dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise ValidationError("Content-Length无效") from exc
            if length > 1024 * 1024:
                raise ValidationError("请求体过大")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError("请求体必须是JSON") from exc
            if not isinstance(data, dict):
                raise ValidationError("JSON顶层必须是对象")
            return data

        def _request_id(self, body: Dict[str, Any]) -> str:
            request_id = self.headers.get("Idempotency-Key", "").strip() or body.pop("request_id", "")
            if not request_id:
                raise ValidationError("写操作必须提供Idempotency-Key请求头或request_id字段")
            if not isinstance(request_id, str) or not request_id.strip():
                raise ValidationError("幂等键必须为非空文本")
            return request_id.strip()

        def _send(self, status: int, payload: Any, content_type: str = "application/json; charset=utf-8",
                  replay: bool = False) -> None:
            if content_type.startswith("application/json"):
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            else:
                body = payload
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            if replay:
                self.send_header("Idempotent-Replay", "true")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle_error(self, exc: Exception) -> None:
            if isinstance(exc, DomainError):
                self._send(exc.status, {"error": exc.code, "message": str(exc)})
            else:
                self._send(500, {"error": "internal_error", "message": "服务内部错误"})

        def do_GET(self) -> None:
            try:
                parsed = urlparse(self.path)
                if parsed.path == "/health":
                    self._send(200, {"status": "ok", "service": "reinsurance-exposure", "database": service.repository.health()})
                    return
                if parsed.path == "/":
                    page = (static_dir / "index.html").read_bytes()
                    self._send(200, page, "text/html; charset=utf-8")
                    return
                if parsed.path == "/api/records":
                    query = parse_qs(parsed.query)
                    records = service.list_records(self._actor(), state=query.get("state", [None])[0], limit=int(query.get("limit", ["100"])[0]))
                    self._send(200, {"items": records})
                    return
                match = RECORD_RE.match(parsed.path)
                if match:
                    record = service.get_record(self._actor(), int(match.group(1)))
                    self._send(200, record, replay=record.get("source") == "legacy")
                    return
                match = AUDIT_RE.match(parsed.path)
                if match:
                    items = service.timeline(self._actor(), int(match.group(1)))
                    for item in items:
                        item.setdefault("source", "live")
                    link = service.ledger.record_link(int(match.group(1)))
                    if link is not None:
                        for item in items:
                            item["source"] = "legacy"
                    self._send(200, {"items": items})
                    return
                if parsed.path == "/api/stats":
                    self._send(200, service.stats(self._actor()))
                    return
                if parsed.path == "/api/events":
                    query = parse_qs(parsed.query)
                    if query.get("layer"):
                        items = service.ledger.list_claims(self._actor(), layer_identifier=query["layer"][0])
                    else:
                        items = service.ledger.list_events(self._actor())
                    self._send(200, {"items": items})
                    return
                match = EVENT_RE.match(parsed.path)
                if match:
                    event = service.ledger.get_event(self._actor(), match.group(1))
                    self._send(200, event, replay=event.get("source") == "replay")
                    return
                if parsed.path == "/api/layers":
                    self._send(200, {"items": service.ledger.list_layers(self._actor())})
                    return
                match = LAYER_RE.match(parsed.path)
                if match:
                    self._send(200, service.ledger.get_layer(self._actor(), match.group(1)))
                    return
                if parsed.path == "/api/claims":
                    query = parse_qs(parsed.query)
                    items = service.ledger.list_claims(
                        self._actor(),
                        event_identifier=query.get("event", [None])[0],
                        layer_identifier=query.get("layer", [None])[0],
                    )
                    self._send(200, {"items": items})
                    return
                match = CLAIM_RE.match(parsed.path)
                if match:
                    self._send(200, service.ledger.get_claim(self._actor(), match.group(1)))
                    return
                match = CHAIN_AUDIT_RE.match(parsed.path)
                if match:
                    kind = match.group(1)
                    items = service.ledger.timeline(self._actor(), kind[:-1], int(match.group(2)))
                    self._send(200, {"items": items})
                    return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

        def do_POST(self) -> None:
            try:
                parsed = urlparse(self.path)
                body = self._body()
                if parsed.path == "/api/records":
                    record = service.create(self._actor(), body.get("reference", ""), body.get("data", {}))
                    self._send(201, record)
                    return
                if parsed.path == "/api/events":
                    request_id = self._request_id(body)
                    result = service.ledger.create_event(self._actor(), body.get("data", body), request_id)
                    self._send(201 if result.get("source") != "replay" else 200, result,
                               replay=result.get("source") == "replay")
                    return
                if parsed.path == "/api/layers":
                    request_id = self._request_id(body)
                    result = service.ledger.create_layer(self._actor(), body.get("data", body), request_id)
                    self._send(201 if result.get("source") != "replay" else 200, result,
                               replay=result.get("source") == "replay")
                    return
                if parsed.path == "/api/claims/assess":
                    request_id = self._request_id(body)
                    result = service.ledger.assess(self._actor(), body.get("data", body), request_id)
                    self._send(201 if result.get("source") != "replay" else 200, result,
                               replay=result.get("source") == "replay")
                    return
                if parsed.path == "/api/claims/settle":
                    request_id = self._request_id(body)
                    result = service.ledger.settle(self._actor(), body.get("data", body), request_id)
                    self._send(200, result, replay=result.get("source") == "replay")
                    return
                match = EVENT_WITHDRAW_RE.match(parsed.path)
                if match:
                    request_id = self._request_id(body)
                    result = service.ledger.withdraw_event(self._actor(), match.group(1), body.get("data", body),
                                                           request_id)
                    self._send(200, result, replay=result.get("source") == "replay")
                    return
                if parsed.path == "/api/admin/reconcile":
                    report = service.ledger.reconcile(self._actor())
                    self._send(200, report)
                    return
                if parsed.path == "/api/admin/backfill":
                    report = service.ledger.backfill_legacy(self._actor())
                    self._send(200, report)
                    return
                match = ACTION_RE.match(parsed.path)
                if match:
                    version = body.get("expected_version")
                    if not isinstance(version, int):
                        raise ValidationError("expected_version必须是整数")
                    record = service.act(self._actor(), int(match.group(1)), version, match.group(2), body.get("data", {}))
                    self._send(200, record)
                    return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

    return Handler


def create_server(host: str, port: int, service: Any, static_dir: Path) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(service, static_dir))
