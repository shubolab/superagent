# 阶段 A · 模型适配器工厂

宿主先装配模型，再把各 `ModelRef` 对应的 `ModelProvider` 交给 Mesh。工厂创建供应商客户端和绑定，模型适配器用它执行 `Generate`。 [宿主装配](go-sdk-assembly.md) · [模型流契约](model-adapter.md)

## 配置与装配

```go
type ProviderRef string

type ModelFactoryConfig struct {
    Providers map[ProviderRef]ProviderConfig
    Models    map[ModelRef]ModelBindingConfig
    Adapters  map[string]ModelAdapter
}

type ProviderConfig struct {
    Adapter       string
    Endpoint      string
    CredentialRef string
}

type ModelBindingConfig struct {
    Provider ProviderRef
    ModelID  string
}

type CredentialSource interface {
    Resolve(ctx context.Context, ref string) (string, error)
}

func NewModelBindings(ctx context.Context, cfg ModelFactoryConfig, secrets CredentialSource) (*ModelBindings, error)
func (b *ModelBindings) LocalModels() map[ModelRef]ModelProvider
func (b *ModelBindings) Close(ctx context.Context) error
```

| 配置 | 含义 |
| --- | --- |
| `ProviderRef` | 宿主为一份供应商连接配置取的名字。配置选择已支持的 `Adapter`、端点及凭据引用。端点省略时使用适配器明确声明的默认值。适配器声明是否要求凭据 |
| `ModelRef` | SDK 使用的逻辑模型名。每个引用绑定一个 `ProviderRef` 和该供应商识别的 `ModelID` |
| `MeshConfig.LocalModels` | 宿主取 `bindings.LocalModels()` 登记已经装配好的模型。方法返回登记表副本，客户端仍由原装配持有 |

例如，宿主配置供应商连接 `primary`，使用已支持的适配器和凭据引用 `models/primary`。它把 `chat` 和 `chat-fast` 绑定到 `primary` 下的两个供应商模型。两者共用客户端，`StartRequest.Model` 只填逻辑名，例如 `chat`。运行器在 `Start` 校验可见性和授权，固定本次 Run 的模型绑定。适配器按绑定上的 `ModelID` 发出请求。 [Run 模型授权](run-authorization.md#宿主授予-run)

每种 `ModelAdapter` 提供 `Open` 构造一个支持并发的 `ModelAdapterClient`。客户端以独立调用上下文和绑定的 `ModelID` 接收 `Generate`。

Go 宿主可以直接登记自有 `ModelProvider`，并管理它的生命周期。文件装配同样可将已建成的适配器登记为本地实现名，由 Mesh YAML 的 `models.<ref>.local` 引用。 [Mesh 本地配置](mesh-transport.md#本地配置格式)

## 创建、复用和释放

1. `NewModelBindings` 先校验配置及引用，再为有模型绑定的每个 `ProviderRef` 解析所需凭据、创建一个供应商客户端。供应商 API 请求从 `Generate` 开始。
2. 同一份装配中，使用同一 `ProviderRef` 的模型及其 Run 共用客户端。不同 `ProviderRef` 各自持有客户端。共享客户端须支持并发，适配器可按供应商配置限流。每次 `Generate` 使用独立的调用上下文和流，单个 Run 的取消只结束自己的请求。
3. 全部绑定就绪后才返回可登记的对象。装配中途失败或取消时，用独立的有界期限清理本次已建资源。已成功装配但后续 Mesh 构造失败时，由宿主关闭这批绑定。
4. 宿主停止受理后等待或取消依赖这批绑定的 Run，再调用幂等的 `Close` 释放工厂创建的资源。配置变更和凭据轮换通过重新装配生效，新 Run 使用新绑定，旧 Run 按原绑定收尾。

## 凭据归属

宿主注入 `CredentialSource`，可从环境或秘密存储解析凭据引用。每个在用的 `ProviderRef` 装配时解析一次，要求认证的适配器收到空凭据时报错。解析后的凭据由适配器侧客户端在本地持有，仅用于该供应商连接的认证。Mesh 配置只登记适配器引用，`StartRequest` 和 `ModelRequest` 的模型选择只携带 `ModelRef`。回报使用可披露的标识、原因码和经适配器清理的说明。

## 失败怎么回报

| 失败位置 | 回报 |
| --- | --- |
| 供应商类型不受支持、端点无效、模型标识为空或引用未定义 | `NewModelBindings` 返回 `model_config_invalid`，指出失败配置项及适用的 `ProviderRef` 或 `ModelRef`。本次装配整体失败 |
| 凭据缺失、读取失败或供应商客户端无法构造 | `NewModelBindings` 返回 `model_credential_unavailable` 或 `model_client_init_failed`。装配取消或到期按上下文错误报告 |
| `Start` 选择了未登记、不可见或未授权的模型 | SDK 返回绑定或授权错误，模型请求次数为零 |
| `Generate` 或 `Recv` 遇到供应商拒绝认证、模型不存在或连接失败 | 适配器分别报告 `model_auth_failed`、`model_not_found` 或 `model_request_failed`。运行器按模型流契约收尾，宿主通过 `Wait` 读取终态和 `Result.Stop` |

装配成功表示本地绑定可用。凭据被供应商接受、端点可达和供应商模型存在，由实际请求结果确认。适配器有明确的供应商错误时使用对应的原因码，无法细分时用 `model_request_failed`。阶段 A 每轮只发起一次供应商请求，沿用本次 Run 固定的绑定。 [模型断流与异常结束](model-adapter.md#断流与异常结束)

## 验收场景

| 场景 | 可观察结果 |
| --- | --- |
| 两个模型引用共用一个 `ProviderRef`，并发启动两个 Run | 构造一次客户端。请求使用各自绑定的 `ModelID`，取消其中一路时另一路继续 |
| 另一 `ProviderRef` 配置不同端点或凭据 | 客户端独立，各自只用对应配置 |
| 多个供应商装配中途失败 | 调用返回可识别的错误，已创建资源均被释放，Mesh 无本次装配的部分绑定 |
| 本地能读到凭据，但供应商拒绝认证 | 工厂构造成功。首次模型请求以可识别的认证错误结束，诊断中只包含可披露的信息 |
| 在途时轮换凭据并重新装配 | 新 Run 走新客户端。旧 Run 收尾后释放旧装配 |
