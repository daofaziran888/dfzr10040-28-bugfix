# 熔炉工坊 · 实时数据融合与宽表构建平台

面向多源异构数据的实时接入融合、宽表映射构建、维度关联编排、数据质量门禁与分区快照管理的实时数据融合平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源定义 | `source_def` | 数据源 | 源编号、源名称、源类型 |
| Schema映射 | `schema_map` | 映射方案 | 方案编号、源表名称、目标宽表 |
| 维度表 | `dim_table` | 维度定义 | 维度编号、维度名称、主键字段 |
| 融合作业 | `fusion_job` | 融合任务 | 作业编号、目标宽表、参与数据源 |
| 冲突仲裁 | `merge_conflict` | 冲突规则 | 规则编号、目标宽表、冲突字段 |
| 质量门禁 | `quality_gate` | 门禁规则 | 规则编号、目标宽表、检查类型 |
| 分区快照 | `snapshot_mgr` | 快照分区 | 分区编号、目标宽表、分区键值 |
| 数据回填 | `backfill_task` | 回填任务 | 任务编号、目标宽表、回填区间 |
| 数据血缘 | `lineage_edge` | 血缘边 | 边编号、上游节点、下游节点 |
| 重放队列 | `replay_queue` | 重放记录 | 记录编号、融合作业、失败原因 |
| 水位追踪 | `watermark_track` | 水位记录 | 记录编号、数据源、当前水位 |
| 分区策略 | `partition_rule` | 分区定义 | 策略编号、目标宽表、分区类型 |
| 宽表定义 | `wide_table_def` | 宽表元数据 | 表编号、宽表名称、字段列表 |
| 增量归并 | `delta_merge` | 增量批次 | 批次编号、融合作业、归并键 |
| 冷热分层 | `cold_store` | 分层策略 | 策略编号、目标宽表、热数据天数 |
| 快照导出 | `export_snapshot` | 导出计划 | 计划编号、目标宽表、导出格式 |
| 指标聚合 | `metric_aggr` | 聚合规则 | 规则编号、目标宽表、聚合函数 |
| 交叉校验 | `cross_validate` | 校验方案 | 方案编号、主体宽表、参照宽表 |
| 数据漂移 | `data_drift` | 漂移记录 | 记录编号、宽表名称、漂移类型 |
| 融合监控 | `fusion_monitor` | 监控指标 | 指标编号、融合作业、监控维度 |
| 变更审计 | `audit_trail` | 审计记录 | 记录编号、变更目标、操作人 |
| 目录检索 | `catalog_search` | 数据目录 | 目录编号、宽表名称、标签列表 |

## 指标聚合的取数依据

指标聚合不再只看一条边就读取整张表，而是先冻结「聚合依据回执」：

- 入口 `POST /api/metric_aggr/compute`：血缘边的下游节点必须是目标表，
  否则立即拒绝越权取数，且不创建规则、回执或物化；
- 分组维度、度量字段必须被**本次传入的那条边**覆盖，不能借用同表其他边的权限；
  未覆盖的字段（如 `tax`）直接判定无读取权限；
- 通过校验后生成 `RCPT-*` 回执，写明目标表、来源节点、逐字段读取权限和
  实际采用的 `边编号@版本`；物化只按回执授权的字段建立输入范围；
- 分组键缺失（`None`、空串、`NULL`）的行不进入任何分组，不会出现空键合法分组。

血缘变更（`POST /api/lineage_edge/{边编号}/revoke|corrections|confirm`）与读取物化
（`GET /api/metric_aggr/materializations/{编号}`）并发时，回执内容保持冻结：
只要本次物化采用的那一版血缘边已撤销、已更正或字段范围变化，读取就返回
`ok=false` 和 `stale_edges`（采用版本 / 当前版本 / 原因），要求确认新边后重算，
不会把旧分组当作当前有效结果。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
