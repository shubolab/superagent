# 工具调用生命周期

设计评审稿。场景是远程工具把文件保存到可保留的工作区，执行成功的回执在返回途中丢失。

## 从授权到回执

```mermaid
---
config:
  sequence:
    actorMargin: 24
    width: 100
    height: 44
    messageMargin: 30
    mirrorActors: false
---
sequenceDiagram
    participant R as 运行器
    participant M as Mesh
    participant T as 工具提供方
    participant E as 执行环境
    R->>R: 核验授权、参数和预算
    Note right of R: B 先保存派发意图
    R->>M: 派发 RunID / CallID
    M->>T: 寻址并转发调用
    T->>T: 复核身份和资源权限
    Note right of T: B 先登记，回包前存回执
    T->>M: 按环境句柄保存文件
    M->>E: 转发保存请求
    E-->>M: 确认文件已保存
    M-->>T: 转发写入结果
    T-->>M: 返回工具回执
    M--xR: 返回途中断连
```

运行器只看到断连。工具提供方及执行环境掌握各自能确认的执行事实。

## 回执丢失后怎么收尾

```mermaid
flowchart TB
    lost["派发后断连 · 结果未知"]
    lost -->|阶段 A| stop["结束 Run<br/>ToolOutcomeUnknown"]
    lost -->|阶段 B| recover["保存待核对状态<br/>按原 CallID 查证"]
    recover --> success["证实已完成<br/>按原结果接续"]
    recover --> skipped["证实未执行<br/>按已知失败处理"]
    recover --> pending["仍无法确认<br/>继续核对或结束为结果不明"]
```

- **阶段 A** 在内存保留 Run 结果，结束后请求释放资源。工作区按原保留策略处理。
- **阶段 B** 从检查点恢复，按原 `CallID` 查证。建议先验收同机重启，多运行器接管随后验证。
- **大输出** 在执行侧分块写入产物。运行器接收有界的小结果或产物引用，检查点保存续跑所需的数据。
- **文件搜索** 同时限制扫描量、单行缓冲、工具内存和返回结果。扫到上限时，把已确认的命中和未扫完的状态一起返回。 [大文件搜索图解](run-budget.md#grep-扫大文件)

`Tool.Invoke` 的回执分为已知结果、明确未执行和结果不明。取消请求和资源释放独立确认。 [Go 请求与回执类型](../specs/tool-invoke.md)

[详细生命周期与验收场景](../specs/tool-call-lifecycle.md) · [架构图解](architecture.md)
