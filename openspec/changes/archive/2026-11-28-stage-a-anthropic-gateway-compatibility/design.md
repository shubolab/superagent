# 兼容网关设计

## Context

CC Switch 的当前 Claude 绑定为百智兼容网关。实测 Bearer 请求交付完整 SSE，包含 thinking 和签名。该网关已接受不回传思考块的第二轮工具回执。

## Decisions

- 宿主用 `AnthropicMessagesOptions` 选择认证方式和有界思考块处理。只发送一种凭据头，保持原端点和禁跳转。
- 思考数据不进入规范输出。限制帧、块数量和独立累计字节，维持块索引及合法结束校验。
- 测试入口用进程环境接收端点和认证方式。CC Switch 的配置文件仍由宿主管理，仓库不含其凭据。

## Risks / Trade-offs

只有实测允许省略思考块的端点才启用此兼容选项。
