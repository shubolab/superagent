# Anthropic 本地上下文计量

## ADDED Requirements

### Requirement: 按模型选择本地计量配置

Anthropic 适配器 MUST 仅为显式声明窗口、计量类型、计量版本及本地计量函数的 ModelID 提供 ModelInputMeter。

#### Scenario: 已配置与未配置模型
- **WHEN** 同一供应商客户端绑定两个 ModelID，其中一个拥有计量配置
- **THEN** 工厂仅为已配置的模型暴露计量能力。普通模型保持原有行为

### Requirement: 发送与计量共享 Anthropic 编码

适配器 MUST 使用相同的供应商请求字节计量和发送，并在发起 HTTP POST 前验证输入指纹。历史摘要须独立编码，宿主指令和工具回执顺序保持。

#### Scenario: 计量与真实 HTTP 请求对照
- **WHEN** 输入包含原文、工具及上下文摘要，并由宿主提供的本地计量函数计量
- **THEN** 计量收到的字节与本地 HTTP 服务收到的字节相同；工具配对保持关联，输出上限与计量中的请求一致

#### Scenario: 计量后输入改变
- **WHEN** 计量后改变工具声明、模型版本或输出参数
- **THEN** 适配器发起 HTTP POST 的次数为零，报告 context_input_changed
