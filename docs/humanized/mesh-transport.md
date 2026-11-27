# Mesh 传输图解

阶段 A 先交付进程内调用。Mesh 用同一个传输接口承接普通请求和流，供以后接入远程传输。

```mermaid
flowchart TB
    call["SDK 或运行器"] --> mesh["Mesh · 解析本地绑定"]
    mesh --> transport["MeshTransport"]
    transport --> local["阶段 A · 进程内分派"]
    local --> unary["Call · 一次响应<br/>启动 / 等待 / 取消 / 工具 / 环境"]
    local --> stream["OpenStream · 有序事件<br/>观察 / 模型"]

    classDef bound fill:#166071,color:#fff,stroke:#166071;
    class mesh,transport bound;
```

| 情况 | 对 Run 的影响 |
| --- | --- |
| 关掉观察或等待超时 | 结束对应调用，Run 继续 |
| 显式取消 | 运行器停止新派发，并向在途工作传递取消。终态由 `Wait` 确认 |
| 模型流提前结束 | 本轮按失败或 Run 的停止原因收尾 |
| 工具可能已收到请求，回包却丢失 | 依据现有证据记录结果不明 |

本地配置用逻辑引用找到已登记实现，例如 `search` 绑定 `bounded-search`。绑定在构造 Mesh 时校验并固定。 [传输契约与 YAML 样例](../specs/mesh-transport.md) · [宿主装配](go-sdk-assembly.md)
