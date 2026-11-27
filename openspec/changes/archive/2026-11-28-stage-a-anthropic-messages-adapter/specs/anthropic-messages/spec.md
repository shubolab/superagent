# Anthropic Messages 适配器

## ADDED Requirements

### Requirement: 规范请求与工具回传

适配器 MUST 将模型绑定、指令、历史、工具声明及配对结果编码为 Anthropic Messages 请求，通过供应商客户端持有的凭据发起单次请求。

#### Scenario: 两轮读取
- **WHEN** 第一轮提出本地搜索，第二轮使用规范回执请求模型
- **THEN** 第二次 HTTP 请求带同一工具声明、配对的 tool_use 和 tool_result，返回最终文本及用量

#### Scenario: 认证和模型不存在
- **WHEN** 供应商 HTTP 或流报告认证失败或模型不存在
- **THEN** Wait 提供对应停止原因，调用次数为一，错误不带凭据和供应商原始正文

### Requirement: 流式完成和资源上限

适配器 MUST 将文本增量与完整 ModelCompleted 分开。解析时限制帧、累计规范输出及工具参数，并随请求取消关闭响应。

#### Scenario: 工具参数被拆分
- **WHEN** SSE 分片交付工具 JSON，随后交付有效的结束原因与 message_stop
- **THEN** 结束信号前工具零派发；接受完整回复后仅派发一次

#### Scenario: 中断或超限
- **WHEN** SSE 提前 EOF、参数无效、达到 max_tokens 或超过 SDK 字节限额
- **THEN** 没有完整回复和工具执行，Run 得到相应的截断、协议或预算终态
