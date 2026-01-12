# 内网部署与网络开通清单（AI-Trader）

## 1. 部署形态（建议）

### 形态 A：纯内网运行（推荐用于合规/隔离）

- 内网运行 Agent + MCP 工具服务 + Web UI
- 外部能力（LLM、行情、新闻）通过企业统一出口代理或内网中转服务访问
- 优点：出口可控、便于审计与限流、密钥不散落

### 形态 B：内网运行 + 允许直接访问外部服务（适合 PoC）

- 直接从内网服务器访问第三方 API（需要放行多个域名/端口）
- 风险：出口面宽、审计与变更复杂

## 2. 需要开通的网络（按功能）

### 2.1 内网入站（提供服务）

- Web UI（静态文件）：默认 `tcp/8888`（见 [start_ui.sh](file:///Users/taoyi3513/code/AI-Trader/scripts/start_ui.sh)）
- MCP 工具服务（HTTP）：默认
  - Math：`tcp/8000`
  - Search：`tcp/8001`
  - Trade：`tcp/8002`
  - LocalPrices：`tcp/8003`
  - CryptoTrade：`tcp/8005`
  - 端口来源：[start_mcp_services.py](file:///Users/taoyi3513/code/AI-Trader/agent_tools/start_mcp_services.py#L25-L43)

建议：以上端口只对同机/同网段开放，或仅开放给运行 Agent 的主机（最小权限）。

### 2.2 内网出站（调用外部服务）

项目中涉及的外部请求主要来自：

- LLM（通过 OpenAI 兼容接口）：由 `OPENAI_API_BASE` 指向的地址决定（通常为企业网关/代理）
- Alpha Vantage（美股/加密行情、新闻）：`https://www.alphavantage.co/query`
  - 代码参考：
    - [get_daily_price.py](file:///Users/taoyi3513/code/AI-Trader/data/get_daily_price.py)
    - [get_interdaily_price.py](file:///Users/taoyi3513/code/AI-Trader/data/get_interdaily_price.py)
    - [get_daily_price_crypto.py](file:///Users/taoyi3513/code/AI-Trader/data/crypto/get_daily_price_crypto.py)
    - [tool_alphavantage_news.py](file:///Users/taoyi3513/code/AI-Trader/agent_tools/tool_alphavantage_news.py)
- Jina 搜索（可选）：`https://s.jina.ai`、`https://r.jina.ai`
  - 代码参考：[tool_jina_search.py](file:///Users/taoyi3513/code/AI-Trader/agent_tools/tool_jina_search.py)
- Tushare（A 股日线，可选）：默认走其 SDK/域名；代码中也出现了 `http://tushare.xyz:5000` 的设置
  - 代码参考：[get_daily_price_tushare.py](file:///Users/taoyi3513/code/AI-Trader/data/A_stock/get_daily_price_tushare.py)
- efinance（A 股小时级，可选）：通常会访问其数据源（域名由库实现决定，建议通过抓包/审计确定）

建议放行策略（最稳妥）：

- 仅允许访问企业统一代理（如 `proxy.corp:port`）与企业自建中转（如 `llm-gateway.corp`）
- 第三方域名统一由代理侧做 allowlist 与审计

如果必须直接放行第三方域名（PoC）：

- 允许 `tcp/443` 访问：
  - `www.alphavantage.co`
  - `s.jina.ai`、`r.jina.ai`（如启用 Jina 搜索）
  - Tushare 的实际域名（或 `tushare.xyz:5000`，不建议长期依赖）
- 允许 `tcp/80` 的情况仅在明确依赖时（如 `tushare.xyz:5000` 走 HTTP）

## 3. 其他必须考虑的问题（内网常见坑）

### 3.1 密钥与配置管理

- `.env` 里包含敏感 token（例如 Tushare），应放入内网密钥管理系统（Vault/KMS/配置中心），运行时注入
- 禁止把 `.env` 放入镜像或制品仓库

### 3.2 DNS / 证书 / 代理

- 如果内网需要 HTTPS 解密或自签证书，Python Requests 可能需要配置 `REQUESTS_CA_BUNDLE`
- 如走 HTTP 代理，需在运行环境配置 `HTTP_PROXY/HTTPS_PROXY/NO_PROXY`

### 3.3 可观测性与审计

- MCP 工具服务建议接入统一日志（stdout 收集）与指标（请求量、耗时、错误率）
- 外呼（LLM、行情、搜索）的审计建议在代理侧落库，做到可追溯

### 3.4 性能与并发

- 多模型并行会导致外呼并发暴增（LLM/行情/搜索），需要限流与熔断
- `start_mcp_services.py` 默认是多进程启动服务，内网部署时建议由进程管理器（systemd/supervisor/k8s）托管

### 3.5 数据与合规

- 行情数据与新闻数据可能有使用条款限制；内网长期部署前要确认许可证与可再分发性
- 交易日志 `data/**/log/*.jsonl` 可能含有模型输出，需制定保留周期与脱敏策略

## 4. 推荐落地路径

- 第 1 阶段：PoC（少量模型，允许直连外部）
- 第 2 阶段：接入企业 LLM 网关 + 外部数据代理（统一出站）
- 第 3 阶段：K8s/容器化、统一观测、密钥托管、最小权限网络策略落地

