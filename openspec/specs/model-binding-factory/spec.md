# model-binding-factory Specification

## Purpose

定义模型绑定装配及生命周期，包括按供应商连接复用客户端、单独解析凭据，以及将配置和初始化错误归一化回报给宿主。

## Requirements

### Requirement: 装配模型和供应商客户端

模型工厂 MUST 先校验所有配置和引用，为每个在用 ProviderRef 解析凭据并创建一个客户端，返回完整的 ModelRef 登记表。

#### Scenario: 共用与独立客户端
- **WHEN** 两个 ModelRef 绑定同一个 ProviderRef，另一个绑定不同的 ProviderRef
- **THEN** 前两个模型共用一个客户端并保留各自 ModelID，另一个客户端独立持有其连接配置及凭据

#### Scenario: 装配中断
- **WHEN** 配置无效、凭据缺失、客户端构造失败或装配被取消
- **THEN** 返回可识别错误及空绑定，已创建客户端在独立有界上下文下释放
- **AND** 错误说明不携带凭据正文或未清理的底层诊断

### Requirement: 绑定生命周期与运行错误

宿主 MUST 可以单独关闭其装配。模型适配器以规范错误向运行器报告已识别的请求失败。

#### Scenario: 并发请求和新装配
- **WHEN** 同一个客户端服务于多个并行 Run，宿主再以新凭据创建一份绑定
- **THEN** 取消一个 Run 不影响另一个，新绑定独立，旧绑定可在旧 Run 收尾后幂等关闭

#### Scenario: 请求期认证失败或模型不可用
- **WHEN** 本地凭据解析成功，但 Generate 或流返回可识别的认证或模型不存在错误
- **THEN** SDK Wait 返回 Failed 及对应的模型停止原因，一轮请求只尝试一次
