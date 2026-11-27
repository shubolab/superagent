# Anthropic 兼容网关验收

## Why

当前 CC Switch Claude 配置使用 Bearer 认证，通过兼容网关运行工具。适配器需要按宿主配置处理认证及网关私有思考块。

## What Changes

- 显式配置 Bearer 认证、自定义端点及已验证可忽略的思考块。
- 思考数据分块丢弃、单独限额。文本与工具继续依赖完整结束标记。
- 让现有真实冒烟及压缩对照复用同一绑定。

## Capabilities

### New Capabilities

- `anthropic-gateway-compatibility` 定义兼容网关的认证和思考边界。

## Impact

可选适配器参数和真实验收入口。默认供应商绑定沿用 API key 和现有事件策略。
