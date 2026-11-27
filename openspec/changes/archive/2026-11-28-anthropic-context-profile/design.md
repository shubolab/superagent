# Anthropic 计量配置设计

## Context

[模型计量契约](../../../docs/specs/context-compaction-contract.md#计量与发送) 要求使用实际供应商编码并在本地计量。Anthropic 普通绑定已通过可控服务器测试。

## Goals / Non-Goals

接通模型资料、共享编码与发送指纹。真实模型的校准和长任务结果由宿主提供模型绑定后验证。

## Decisions

- 计量配置按供应商 ModelID 注入。计量类型、版本与窗口固定在装配中。
- 本地计量函数读取共享编码生成的请求字节。摘要置于独立的历史消息，输出预留成为实际 max_tokens。
- 普通请求仍使用适配器默认 token 上限；计量请求可收紧该上限。

## Risks / Trade-offs

SDK 不提供猜测的 Anthropic 窗口及 tokenizer。估算版本须符合既有校准和失效规则。
