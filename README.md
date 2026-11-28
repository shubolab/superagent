# SuperAgent

SuperAgent 是一个 Go Agent 底座。独立 TUI 和 Go SDK 共用进程内运行器，独立对话可以同时推进、流式回复和分别取消。当前提供本机只读工作区、有界搜索、持久恢复，以及经 mTLS 连接的远程工具。运行器、工具和执行环境支持宿主替换绑定。

项目处于设计评审阶段。

- 图解 · [架构与阶段边界](docs/humanized/architecture.md) · [Go SDK](docs/humanized/go-sdk.md) · [阶段 A 最小闭环](docs/humanized/stage-a-acceptance.md)
- 规范 · [架构与 Mesh](docs/specs/architecture.md) · [阶段 A · bypass](docs/specs/stage-a-bypass.md) · [Go SDK 契约与接口](docs/specs/go-sdk-contract.md)

根目录保留 [SDK 客户端](client.go)、[Mesh 装配](mesh.go) 和 [会话接口](session.go)。外部宿主可直接引用 [contract](contract) 的共享类型，自行装配 [model](model) 工厂、[mesh](mesh) 传输、[runner](runner) 运行器和 [schedule](schedule) 调度器，通过 [sdk](sdk) 接入。公开 API 验收见 [tests/sdk](tests/sdk)，全量回归使用 `mise exec -- go test ./...`。

`OpenSession` 默认将每个会话的历史写入一个 `.session.jsonl` 文件，每行是一轮已提交的 Run。宿主可用 `OpenSessionWithStore` 注入自己的 [session.Store](session/store.go)。

配置 `ANTHROPIC_API_KEY` 或 `ANTHROPIC_AUTH_TOKEN` 后，运行 `go run ./cmd/superagent -model <模型 ID>` 打开双会话终端。默认一个运行名额，可用 `-parallel 2` 同时运行两条会话。

兼容网关通过 `ANTHROPIC_BASE_URL` 配置。对已验证可省略思考块的网关显式加 `-discard-thinking`。恢复在途 Run 时使用原模型、端点和适配模式。升级前以通用标识保存、缺少原模型身份的在途 Run 会保留为待核对。
