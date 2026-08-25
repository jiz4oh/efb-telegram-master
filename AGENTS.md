# Agent 工作说明

## 包管理器
- 使用 `python3`。
- 依赖与构建配置在 `pyproject.toml`。
- 测试框架为 `pytest`。

## 文件级命令
- 运行单元测试：`python3 -m pytest tests/unit`
- 语法检查：`python3 -m py_compile efb_telegram_master/*.py`

## 关键约定
- 任何消息渲染改动需同时覆盖发送与编辑路径。
- 涉及 `parse_mode='HTML'` 时，确保 HTML 安全转义，避免触发 Telegram entity 解析失败。
- 保持最小 diff，不做无关重构。