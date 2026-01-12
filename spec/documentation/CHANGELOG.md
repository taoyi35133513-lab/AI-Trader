# 项目变更日志 (CHANGELOG)

## 文档说明
- **文档目的**：记录 AI-Trader 代码库的重要变更历史，以便追踪项目演进过程。
- **更新频率**：每次重要变更（Feature, Fix, Refactor 等）后立即更新。
- **格式规范**：采用标准 Markdown 语法，按时间倒序排列。

---

## 2025-12-21 v0.2.0
- [优化] Shell 脚本标准化与虚拟环境强制激活 @TraeAI
  - **变更描述**：
    1. 修改了 `scripts/` 目录下所有 12 个 shell 脚本。
    2. 在脚本头部增加了虚拟环境 (`.venv`) 的检查与激活逻辑。
    3. 将所有 `python` 命令替换为 `python3`，确保解释器版本一致性。
    4. 简化了 `regenerate_cache.sh` 中的 Python 版本探测逻辑。
  - **影响模块/文件**：
    - `scripts/main.sh`
    - `scripts/main_step[1-3].sh`
    - `scripts/main_a_stock_step[1-3].sh`
    - `scripts/main_crypto_step[1-3].sh`
    - `scripts/start_ui.sh`
    - `scripts/regenerate_cache.sh`

## 2025-12-21 v0.1.0
- [文档] 创建项目架构分析文档 @TraeAI
  - **变更描述**：
    1. 在 `spec/` 目录下创建了 `architecture_analysis.md`。
    2. 详细分析了项目目录结构、技术架构、构建部署流程及代码组织原则。
    3. 添加了 Mermaid 架构图。
  - **影响模块/文件**：
    - `spec/architecture_analysis.md`

