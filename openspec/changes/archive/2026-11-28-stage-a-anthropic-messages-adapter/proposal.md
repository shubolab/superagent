# 阶段 A · Anthropic Messages 适配器

## Why

模型工厂已支持宿主登记适配器。按模型流设计中列出的 Anthropic Messages 样例，验证一个具体供应商协议的流和工具配对。

## What Changes

- 增加可选的 Anthropic Messages 工厂构造器。
- 归并有界 SSE 文本、工具参数和用量，在完整结束信号后归一化回复。
- 将已提交的工具声明和结果编入下一次供应商请求。以本地 HTTP 服务器验收；真实冒烟使用宿主提供的模型绑定和凭据。

## Capabilities

### New Capabilities

- `anthropic-messages` 定义 Anthropic Messages 与 Go 模型契约的转换。

## Impact

模型工厂继续按宿主配置选适配器。新增一个 HTTP 适配器和测试。
