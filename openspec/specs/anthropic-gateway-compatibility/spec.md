# anthropic-gateway-compatibility Specification

## Purpose

规定 Anthropic 兼容网关的显式 Bearer 认证和私有思考块处理，保证跳过思考内容仍受流式输入上限约束。

## Requirements

### Requirement: 独立配置认证

适配器 MUST 将凭据按宿主选择用于已配置端点，支持 Bearer 及默认的 x-api-key。

#### Scenario: 兼容网关的认证和路径
- **WHEN** 宿主指定带路径的本地配置端点和 Bearer 认证
- **THEN** 实际 Messages 请求带 Authorization，不重复发送 x-api-key。请求走该端点及标准 Messages 路径

### Requirement: 兼容网关的有界思考块

适配器 MUST 仅在宿主明确启用后消费可丢弃的 thinking 及签名块。不得进入 SDK 文本和后续请求，保留原有有界和完整回复规则。

#### Scenario: 思考后给出工具调用
- **WHEN** 网关按顺序给出思考、工具参数、工具结束原因及 message_stop
- **THEN** SDK 只公开规范工具调用与文本。下一次请求的工具 ID 和回执仍正确配对

#### Scenario: 思考输入异常
- **WHEN** 未启用兼容选项、思考块不完整，或思考数据超过独立字节上限
- **THEN** 模型流按对应协议或预算错误结束，工具零派发
