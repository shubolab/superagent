# Go SDK 宿主装配与 Mesh 绑定

阶段 A 设计稿。Go 宿主和 TUI 使用同一个 `Client` 契约。宿主先登记进程内组件，再构造 Mesh，最后将 `Client` 绑定到一个运行器。 [Go SDK 调用契约](go-sdk-contract.md)

## 创建 Client

```go
type RunnerRef string
type ModelRef string
type ToolRef string
type EnvironmentRef string

func NewMesh(cfg MeshConfig) (*Mesh, error)
func NewClient(mesh *Mesh, runner RunnerRef) (*Client, error)
```

1. 宿主填写 `MeshConfig` 登记进程内实现。Mesh 先验证组件绑定，再按宿主配置装配运行器并登记其执行入口。文件配置同样解析成 `MeshConfig`。 [本地配置格式](mesh-transport.md#本地配置格式)
2. `NewClient` 核对 Mesh 中的运行器绑定并构造 SDK 入口。
3. `Start`、`Observe`、`Wait`、`Cancel` 经同一运行器实例处理。`Client.Run(id)` 沿用该绑定构造句柄。

宿主拥有 Mesh 的生命周期，多个 `Client` 可以共用它。宿主关闭时先停止受理，再按其策略等待或取消本地 Run，并释放自己创建的组件。

## 登记组件

```go
type MeshConfig struct {
    LocalRunners      map[RunnerRef]RunnerConfig
    LocalModels       map[ModelRef]ModelProvider
    LocalTools        map[ToolRef]LocalToolBinding
    LocalEnvironments map[EnvironmentRef]EnvironmentProvider
}

type LocalToolBinding struct {
    Spec ToolSpec
    Impl Tool
}
```

| 登记项 | 内容 |
| --- | --- |
| 运行器 | 登记 `RunnerConfig`，指定可用绑定、授权配置和默认环境。装配过程接上已登记的依赖。运行事件也由运行器装配，经 Mesh 使用 |
| 模型 | 宿主先构造模型绑定，再按 `ModelRef` 登记 `ModelProvider`。宿主管理这批绑定的生命周期，凭据由适配器侧客户端持有。 [模型工厂契约](model-factory.md) |
| 工具 | 按 `ToolRef` 登记实现与 `ToolSpec`。声明包含展示给模型的名称、描述、参数约束，以及工具所需的环境能力。运行器从已授权的声明生成模型工具列表。 [工具调用契约](tool-invoke.md) |
| 执行环境 | 按 `EnvironmentRef` 登记环境提供方。提供方声明实际可执行的资源限制，分配时确认生效额度。 [执行环境契约](execution-environment.md) |

同一 Mesh 内，`组件类型 + 引用` 对应唯一绑定。构造完成后登记表固定，新配置用于新的装配。工具和模型实现共享给多个 Run 时须支持并发，或由提供方限流。

## 后续混合部署

本地以发起组件调用的进程为准。运行器用它所在进程的 Mesh 解析模型、工具和环境。以后将 `ToolRef("search")` 指向独立工具进程时，宿主的 `StartRequest` 可沿用原引用，由运行器侧 Mesh 选择绑定，由传输接口承接调用。 [Mesh 传输契约](mesh-transport.md)

## Run 受理时的绑定与授权

- **登记让组件可解析。** 运行器根据宿主授权及经验证的调用方身份检查本次 `Start` 请求的模型和工具。一个 Run 中展示给模型的工具名称须唯一。引用缺失或授权不符时拒绝受理。 [宿主权限配置](run-authorization.md#宿主授予-run)
- **受理固定本次绑定。** 运行器为本次 Run 固定模型、工具、环境与工具声明。`EnvironmentRequirement` 表达所需能力，环境绑定由运行器按宿主配置选择。实际资源可在首次使用前分配。
- **派发复核本次权限。** 工具使用工作区时，运行器在派发前申请按 `CallID` 约束的环境授权，工具提供方和环境分别复核业务与资源权限。环境分配时确认实际能力。已受理 Run 沿用原绑定，分配或调用失败按执行契约报告。

## 验收场景

| 场景 | 应得到的结果 |
| --- | --- |
| 装配本地运行器和依赖 | 运行器以登记表解析模型、工具和环境。测试传输替身可从同一边界注入断流和回执丢失 |
| 配置重复登记同类引用，或同一 Run 请求两个同名工具 | 分别在构造 Mesh 或受理 Run 时明确报冲突 |
| 工具未授权或环境能力不足 | 相关调用未派发，并返回可识别的错误 |
| Run 在途时换用新的装配 | 新 Run 使用新装配，原 Run 的等待、取消和工具调用仍对应首次受理的绑定 |
