# Anthropic Messages 适配设计

## Context

[模型适配器契约](../../../docs/specs/model-adapter.md) 将 Anthropic Messages 列作验证样例。使用其 [Messages API](https://platform.claude.com/docs/en/api/messages/create?version=2023-06-01) 和 [SSE 事件](https://platform.claude.com/docs/en/build-with-claude/streaming)。

## Goals / Non-Goals

接通普通文本和客户端工具调用。供应商内置工具、思考块及上下文压缩随各自契约接入。

## Decisions

- 宿主通过 ModelAdapter 选择已明确的 API 版本，按 ModelID 发起带 token 上限的请求。本地继续执行 Run 的字节上限。
- 完整消息由 SSE 的开始、内容块、结束原因及 message_stop 构成。工具 JSON 在块结束时校验，规范调用 ID 包含当前步，HTTP 历史中的调用和结果使用同一供应商 ID。
- HTTP 测试可控制结束标记、字节、错误和连接关闭。真实冒烟单独使用配置的 Claude 模型及凭据。

## Risks / Trade-offs

维持阶段 A 的单次请求。未识别或不能表示的内容块按协议错误处理。
