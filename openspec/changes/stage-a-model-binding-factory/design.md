# 模型绑定工厂设计

## Context

[模型适配器工厂](../../../docs/specs/model-factory.md) 已规定宿主配置、创建时机、凭据归属和错误语义。

## Goals / Non-Goals

交付供应商无关的宿主装配、客户端所有权及模型错误。具体 API 适配协议和真实供应商冒烟由独立切片验收。

## Decisions

- 宿主在 `ModelFactoryConfig.Adapters` 登记供应商构造器及默认端点。工厂在单次装配内为每个在用 ProviderRef 调用一次构造器，并给 ModelRef 创建独立的 ModelID 路由。
- 校验完成后才解析凭据、创建客户端。本地解析的凭据只交由对应构造器，设置错误以原因码及可披露的引用回报。
- 资源归 ModelBindings。失败逆序清理已建客户端。宿主关闭时得到一次清理的共享结果。

## Risks / Trade-offs

宿主负责在关闭旧装配前停止使用它。具体构造器与客户端遵循有界上下文和并发接口契约。
