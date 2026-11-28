# SuperAgent

SuperAgent 是一个从零设计的 Go Agent 底座。独立 TUI 和 Go SDK 共用一套执行契约，独立对话可以同时推进、流式回复和分别取消。组件通过 Mesh 调用本地或远程实现，需要隔离的工作可以交给沙箱执行。

项目处于设计评审阶段。

- 图解 · [架构与阶段边界](docs/humanized/architecture.md) · [Go SDK](docs/humanized/go-sdk.md) · [阶段 A 最小闭环](docs/humanized/stage-a-acceptance.md)
- 规范 · [架构与 Mesh](docs/specs/architecture.md) · [阶段 A · bypass](docs/specs/stage-a-bypass.md) · [Go SDK 契约与接口](docs/specs/go-sdk-contract.md)

代码从 [SDK 客户端](client.go) 和 [Mesh 装配](mesh.go) 进入，[运行器](internal/run) 和 [Mesh 路由与配置](internal/mesh) 在 `internal/`。公开 API 验收在 [tests/sdk](tests/sdk)，全量回归使用 `go test ./...`。
