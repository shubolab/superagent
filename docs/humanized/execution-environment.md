# 执行环境图解

需要工作区的 Run 绑定一个受限环境句柄。工具按次获得授权，可以在同一个工作区各做获准的操作。

## 从能力声明到执行

```mermaid
---
config:
  flowchart:
    nodeSpacing: 26
    rankSpacing: 32
---
flowchart TB
    ask["运行器 · 核对要求与预算"] --> check{"环境能兑现所需上限？"}
    check -->|缺少必需能力| reject["拒绝派发并说明原因"]
    check -->|确认额度| bind["为 Run 分配环境句柄"]
    bind --> grant["按 CallID 授权<br/>路径 · 操作 · 额度 · 时限"]
    grant --> execute["工具经 Mesh 使用环境<br/>执行前复核授权"]

    classDef bound fill:#166071,color:#fff,stroke:#166071;
    class bind,grant bound;
```

| 环境 | 声明与执行 |
| --- | --- |
| 本机 | 以宿主实测能力为准。进程内工具用有界读写，通用命令要求可验证的整组进程限制 |
| 沙箱 | 分别声明隔离范围、内存、时限和磁盘额度，配置完成后启动工作负载 |
| 远程节点 | 执行节点确认生效上限并负责执行。断开连接后，节点仍按已接受的时限收尾 |

## 取消后如何收尾

```mermaid
---
config:
  flowchart:
    nodeSpacing: 26
    rankSpacing: 32
---
flowchart TB
    cancel["运行器接受取消"] --> halt["停止派发<br/>请求环境终止工作"]
    halt --> verify{"环境确认工作停止？"}
    verify -->|已停止| keep["保留所需产物<br/>回收运行资源与临时工作区"]
    verify -->|失联或待处理| pending["标记待确认<br/>按原句柄查询"]
    pending --> verify
    keep -->|确认回收| released["环境报告已释放"]
    keep -->|仍待处理| pending

    classDef bound fill:#166071,color:#fff,stroke:#166071;
    class released,keep bound;
```

- 句柄和调用授权随 Run 收尾失效。后续 Run 使用同一工作区时重新授权。
- 临时工作区按策略回收。已有工作区、已发布产物各按其保留期管理，部分产物标明未完成。
- `Wait` 可以先返回 Run 终态，资源是否释放由环境单独确认。

[执行环境详细契约](../specs/execution-environment.md) · [工具调用与回执](tool-call-lifecycle.md)
