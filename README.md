# 再保险合约与巨灾暴露管理

纯Python标准库实现的再保险合约与巨灾暴露管理原型，使用SQLite持久化，HTTP接口由`http.server`提供。
巨灾事件、合约分层、赔案和恢复台账接成同一条记账链。

## 记账链模型

- **实体链**：巨灾事件 `cat_events` → 合约分层 `layers`（起赔点/限额/分出比例/恢复次数）→ 赔案 `records` → 追加式台账 `reinstatement_ledger`。
- **两阶段记账**：
  - 核定 `calculate` 只写 `reserve`：预占层容量与恢复次数，保费不确认；
  - 结算 `settle` 写 `confirm`：预占转实耗、累计恢复保费；
  - 拒赔 `reject` 或事件撤回写 `release`：释放预占。
- **容量口径**：首赔吃基础层容量（恢复次数记0），超出基础容量后每层消耗1次恢复；
  总容量 = 基础层容量 × (1 + 恢复次数)。
- **幂等**：同一 reference 创建重放返回原记录；核定/结算/释放按固定台账编号
  （`RSV-/CNF-/REL-{claim_id}`）重放返回原结果，不重复占用。
- **并发**：所有容量复核在 `BEGIN IMMEDIATE` 事务内按实时台账折叠完成，争抢最后一次恢复时只有先到者成功。
- **事件撤回**：未结算赔案置 `invalidated` 并释放预占（可在新赔案中重新核定），已结算的 `confirm` 原依据保留。
- **写入中断恢复**：服务启动先为旧数据补事件序列（`source=migration`），再以台账为唯一事实对账重建余额、修补半成品（`source=reconcile`）。
- **来源标记**：台账、审计、详情均标 `live / replay / reconcile / migration`。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动对账和服务启动。
- `src/domain.py`：领域数据类型、状态常量、错误和基础校验。
- `src/rules.py`：状态转换、分层摊回、容量与恢复次数核算（纯函数）。
- `src/repository.py`：SQLite建表/旧库迁移、追加式台账、事务内预占/确认/释放、对账续作。
- `src/service.py`：用例编排、自动接链、幂等重放、权限和详情组装。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：审计时间线（含事件级全局审计）。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、记账链、并发争抢、撤回与重启对账测试。

## 启动

```bash
python3 app.py --db ./data.db --port 8325
```

默认端口`8325`，默认数据库位于项目目录。服务启动时自动建表、迁移旧数据并执行对账。

## 主要接口

### 事件与合约分层

- `POST /api/events`：注册巨灾事件（`underwriter`），体 `{"data":{"event_id","event_name","occurred_on"}}`。
- `POST /api/events/withdraw`：撤回事件，体 `{"event_id":"...","reason":"..."}`。
- `GET /api/events`：事件列表。
- `POST /api/layers`：注册合约分层，体 `{"data":{"layer_code","attachment","limit","cession_pct","reinstatement_pct","reinstatement_total"}}`。
- `GET /api/layers`：分层列表。
- `GET /api/layers/{code}/balance`：分层容量与恢复次数余额（含未结清占用）。

### 赔案与记账

- `POST /api/records`：创建赔案；未显式注册事件/分层时按载荷自动接链。同 reference 重放返回原记录。
- `GET /api/records?state=&event_id=&limit=`：赔案列表。
- `GET /api/records/{id}`：赔案详情，附事件、分层、完整台账及来源标记。
- `POST /api/records/{id}/actions/{bind|submit_claim|calculate|settle|reject}`：动作，体 `{"expected_version":N,"data":{...}}`；同编号重放返回原结果并标 `replayed=true`。
- `GET /api/records/{id}/audit`：赔案审计时间线。
- `GET /api/ledger?layer=&event=&claim=&limit=`：恢复台账（全局可查，按 seq 排序）。
- `GET /api/audit?limit=`：全局审计（含事件撤回、迁移、对账记录）。
- `GET /api/stats`：赔案状态统计与已确认恢复保费汇总。
- `POST /api/reconcile`：手动触发对账续作（`admin`）。

其余：`GET /health` 健康检查、`GET /` 演示页面。除二者外请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。

角色：`underwriter`（建分层/事件/赔案、绑定、撤回）、`claims_officer`（提交与核定赔案、可拒赔）、`finance`（结算、可拒赔）、`admin`（全部只读查询与对账）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖完整流程、规则计算、重复引用/权限/版本冲突、两阶段预占与确认、同号重放、
并发争抢最后一次恢复、拒赔释放、事件撤回失效/保留、旧库迁移补序列和写入中断重启对账。
