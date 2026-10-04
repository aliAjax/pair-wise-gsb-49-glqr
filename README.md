# 再保险合约与巨灾暴露管理

纯Python标准库实现的再保险合约与巨灾暴露管理原型，使用SQLite持久化，HTTP接口由`http.server`提供。

## 记账链（巨灾事件 → 合约分层 → 赔案 → 恢复台账）

同一巨灾事件下多份合约的容量占用与恢复保费按一条链记账：

- **核定预占**：`POST /api/claims/assess` 在单事务内预占层容量与恢复槽（槽0为原始保障，槽1..N为第1..N次恢复），并预估恢复保费；事件撤回后预占失效。
- **结算确认**：`POST /api/claims/settle` 才真正确认消耗、累计恢复保费；已结算赔案冻结核定依据（basis快照），事件撤回也不改变。
- **幂等重放**：所有写操作必须携带 `Idempotency-Key`（或请求体内 `request_id`）。同一编号重放返回首次结果并带 `Idempotent-Replay: true` 响应头，载荷不一致返回409。
- **并发仲裁**：恢复槽由部分唯一索引 `(layer_id, reinstatement_no) WHERE status IN ('reserved','confirmed')` 仲裁，并发争抢最后一次恢复时只有先提交者成功，其余返回409。
- **事件撤回**：`POST /api/events/{code}/withdraw` 使未结算预占失效（台账转 `released`、赔案转 `void`、释放槽位与容量并重算分层占用），已结算记录保留原依据。
- **中断续作**：命令先以 `pending` 登记台账再执行业务；重启时先对账（补登漏台账、修正状态错位、检查事件序列），再续作挂起命令。可用 `--no-recover` 跳过。
- **旧数据回填**：旧 `records` 不做改动，启动时非破坏式补事件序列、挂接分层/赔案台账；详情与审计均标 `source=legacy`，在线数据为 `live`，恢复补登为 `recovery`，重放结果为 `replay`。

口径：层容量 `(限额-起赔点)×分出比例`；总可恢复容量 `层容量×(1+恢复次数)`；摊回 `min(max(0,损失-起赔点),层宽)×分出比例`；恢复保费 `摊回×恢复费率`。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动对账续作和服务启动。
- `src/domain.py`：领域数据类型、错误和基础校验。
- `src/rules.py`：旧记录流程的状态转换、分层摊回、赔偿限额和恢复保费。
- `src/repository.py`：SQLite建表、事务和查询（WAL模式）。
- `src/service.py`：旧记录用例编排、权限检查、乐观并发和审计，详情挂接记账链。
- `src/ledger_rules.py`：记账链纯规则（容量、恢复槽、保费、校验）。
- `src/ledger_store.py`：事件/分层/赔案/恢复台账/命令幂等/链审计的表结构与事务。
- `src/ledger_service.py`：核定预占、结算确认、撤回失效、启动续作、对账与旧数据回填。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：旧记录审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则计算、失败场景、记账链、并发争抢和中断恢复测试。

## 启动

```bash
python3 app.py --db ./data.db --port 8325
```

默认端口为`8325`，默认数据库位于项目目录。服务启动时自动建表并执行对账续作与旧数据回填（结果打印到标准输出）。

## 记账链接口

写操作均需 `Idempotency-Key` 请求头，请求体支持直接展开或放在 `data` 字段内。

- `POST /api/events`：登记巨灾事件（`event_code`，自动分配事件序列）。角色：cat_analyst。
- `POST /api/layers`：登记合约分层（`attachment/limit_amount/cession_pct/reinstatement_count/reinstatement_rate`）。角色：underwriter。
- `POST /api/claims/assess`：核定赔案并预占恢复槽。角色：claims_officer。
- `POST /api/claims/settle`：结算确认消耗并累计保费。角色：finance。
- `POST /api/events/{code}/withdraw`：撤回事件（未结算失效、已结算保留）。角色：cat_analyst。
- `GET /api/events`、`GET /api/events/{code}`：事件列表/详情（含赔案与分层占用）。
- `GET /api/layers`、`GET /api/layers/{code}`：分层列表/详情（含容量、活跃槽位、已确认/预占保费）。
- `GET /api/claims?event=&layer=`、`GET /api/claims/{number}`：赔案查询（含台账条目与核定依据）。
- `GET /api/events/{id}/audit`、`/api/layers/{id}/audit`、`/api/claims/{id}/audit`：记账链审计时间线（含source）。
- `POST /api/admin/reconcile`、`POST /api/admin/backfill`：手工触发对账/回填。角色：admin。

## 主要接口（旧记录流程，保持不变）

- `GET /health`：健康检查。
- `GET /`：演示页面。
- `GET /api/records`：记录列表，可带`state`和`limit`参数。
- `GET /api/records/{id}`：记录详情；已回填的旧记录额外返回 `chain` 与 `source=legacy`。
- `GET /api/records/{id}/audit`：审计时间线（回填后标source）。
- `GET /api/stats`：状态统计。
- `POST /api/records`：创建记录，请求体为`{"reference":"...","data":{...}}`。
- `POST /api/records/{id}/actions/{action}`：执行业务动作，请求体为`{"expected_version":1,"data":{...}}`。

除`/health`和`/`外，请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。记账链角色：cat_analyst、underwriter、claims_officer、finance、admin（admin可执行全部操作）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖旧记录完整流程、规则计算、重复引用、权限拒绝、版本冲突，以及记账链的预占/确认、幂等重放、恢复次数争抢（多线程只收先到者）、事件撤回、启动续作、对账修复和旧数据回填。
