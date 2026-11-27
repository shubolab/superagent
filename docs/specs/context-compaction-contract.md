# 上下文压缩接口与验收

后续可选能力。运行器拥有原始消息、当前模型视图和压缩裁决。模型适配器负责计量及发送，原文读取方负责授权与版本校验。 [触发参数与算法](context-compaction.md)

## 计量与发送

```go
type ModelInputMeter interface {
    ContextLimits() ModelContextLimits
    MeasureInput(ctx context.Context, req ModelRequest) (InputTokenMeasurement, error)
}

type ModelContextLimits struct {
    WindowTokens   uint64
    MaxInputTokens uint64
}

type InputTokenMeasurement struct {
    Tokens         uint64
    Kind           string
    ProfileVersion string
    InputDigest    string
}

type ModelContextOptions struct {
    Summary             []ContextSummaryEntry
    MaxOutputTokens     uint64
    ExpectedInputDigest string
}
```

启用时，模型绑定提供可选的 `ModelInputMeter`，`ModelRequest` 携带可选的 `Context *ModelContextOptions`。计量和派发均经 Mesh，合规请求继续使用 `ModelProvider.Generate`。`WindowTokens` 须为正数，`MaxInputTokens=0` 表示沿用总窗口约束。`MaxOutputTokens` 按当前模型映射为实际输出上限，预留涵盖供应商计入窗口的全部输出。

| 数据 | 口径 |
| --- | --- |
| 发送前 `Tokens` | 对实际供应商输入表示计量，包括指令、摘要、消息、工具声明和协议包装。使用同一编码路径生成摘要指纹 `InputDigest`，覆盖模型、计量版本和实际输出选项。凭据及 `ExpectedInputDigest` 属于计量输入之外的控制数据 |
| `Kind` 与 `ProfileVersion` | `exact` 表示适配器掌握该版本的编码与分词。否则用 `estimated`，按同一模型和编码版本的实测输入验证低估量，配置安全余量 `S`。计量在本地执行；版本失效或缺少可靠计量时返回明确错误 |
| 实际发送及结算 | 运行器将指纹写入 `ExpectedInputDigest`。适配器发送前核对，变化时返回 `context_input_changed`，供应商调用次数为零。供应商报告的输入 token 单独记录，缓存命中部分在总输入中只算一次，缺失时记为未知。摘要计入规范请求字节，Run 额度沿用字节和请求次数结算 |

估算模式用冻结的同模型请求样本校准安全余量。线上观察到低估超过 `S` 时，该计量版本须重新验证后方可用于准入。

[模型请求契约](model-adapter.md#请求) · [Run 预算计量](run-budget.md#预算与扣减)

## 摘要来源与回查

```go
type ContextSourceRef string

type ContextSummaryEntry struct {
    Section string
    Text    string
    Sources []ContextSourceRef
}

type ContextSourceReader interface {
    ReadSource(ctx context.Context, ref ContextSourceRef, offset, maxBytes uint64) (ContextSourcePage, error)
}

type ContextSourcePage struct {
    Content    []byte
    NextOffset uint64
    EOF        bool
}
```

- 运行器生成不透明引用，将它绑定到本 Run、原始快照中的消息块和版本，或获准的不可变 `ArtifactRef`。同一原文的句柄跨压缩版本稳定；历史里的重复 `CallID` 由各自的原始块区分。
- 读取方从受信上下文校验主体、Run、权限及版本。按原始字节偏移读取，页大小取 `maxBytes`、宿主单页上限和适用的剩余额度中的最小值。`maxBytes` 须为正数，偏移须落在原文内或末尾。到达末尾时正常返回且置 `EOF=true`。环境产物按原授权及保留期读取，原文所在 Run 记录释放后引用失效。过期、越权或版本不符统一返回 `context_source_unavailable`。
- 摘要条目分为目标与约束、决策、已确认效果和待办，每项至少关联一个已登记的原文句柄。模型主动回查时，经宿主已登记并授权的只读工具调用，按普通工具额度扣减。用引用替代正文前须确认模型有可用的授权回查路径。

[执行环境与产物](execution-environment.md#工作区和产物保留) · [Run 权限门禁](run-authorization.md)

## 连续压缩与切换

1. 保护本次 Run 收到的全部用户消息原文、宿主指令和固定项。每轮从原始记录组装，保留用户消息次序。涉及受保护消息的工具批次整组保留。保护项超硬限额时返回 `BudgetExceeded`。
2. 每个已提交消息版本最多尝试一次。摘要调用沿用 `Generate`，关闭工具，接受完整的 `ModelCompleted(stop)`，解析为按来源关联的条目。下一次压缩从新增的原始块生成新条目，运行器原样合并仍保留的旧条目。若要合并旧条目，须用它们指向的原文重建。预算不足时按回退处理。
3. 运行器持有候选的基准消息版本和覆盖块清单，核对来源、保护原文与完整工具配对。原块须有摘要条目或可回查的替代记录才可移出输入视图，缺少覆盖的块保留原文。发布前重新核算实际输入和余额，并与停止裁决在同一 Run 内排序。原始历史与 `Result.Messages` 始终以已提交记录为准。

## 回退与终态

下列顺序只用于 Run 仍可推进时。每份回退视图都须覆盖当前消息版本，包含最新已提交块，并重新核验压缩替代引用、计量及剩余额度。摘要请求已消耗的次数、字节和时间照常结算。

| 条件 | 行为 |
| --- | --- |
| 摘要超限、无效、来源失效、失败或收益不足 | 尝试已校验的确定性缩减视图，其次使用上一次已发布的视图并补齐新增块，最后检查当前完整原文。发送首个通过复核的视图 |
| 可计量候选均超 `B` 或 `L` | 返回 `BudgetExceeded`，记录命中的窗口或字节限额，正常模型请求次数为零 |
| 仅因缺少可靠计量而无法确认候选可发 | 终态为 `Failed`，原因 `context_measurement_unavailable` |
| 取消、Run 超时或消息版本已经变化 | 丢弃当前候选。取消和超时沿用原终态规则；版本变化时按最新已提交消息重新组装 |
| 供应商在正式请求中报告窗口超限 | 终态为 `Failed`，原因 `model_context_overflow`。记录同一请求的估算与实测，不重发此轮 |

## 验收用例

验收用固定计量器、可控模型流和版本化原文替身，检查模型发送次数、实际发送内容、工具调用次数、终态、降级选择及预算账目。

| 用例 | 输入或故障 | 预期 |
| --- | --- | --- |
| 触发边界 | 固定 `F=104400`、`B=116000`，分别输入 `104399`、`104400`、`116000`、`116001` token，控制字节数为 `L` 或 `L+1` | 字节数等于 `L` 时首值直发，其余触发检查。等于硬限额仍可回退发送；任一硬限额多一单位时，原视图零派发 |
| 计量与发送一致 | 指令、中文文本、工具 schema 和摘要一起计量，计量后修改 schema；另一次报告总输入 1100，其中缓存命中 300 | 使用实际编码结果。修改后指纹不符，供应商请求零次。实测总输入仍为 1100，Run 字节账按规范请求结算 |
| 估算低于实测 | 安全余量 `S=4000`，同计量版本的请求实测比估算多 4001 | 记录偏差，该计量版本待重新验证。后续仅缺可靠计量时零派发 |
| 原文可回查 | 同一 8 字节源在两次压缩后按最多 3 字节分页读取。尝试跨 Run 读取、读过期产物或替换同路径的版本 | 句柄稳定，三个页面依次返回原始 3、3、2 字节，末页 EOF。越权或失效统一不可查，当前版本内容不进入返回 |
| 连续三次压缩 | 历史中包含否定约束和后续更正，摘要刻意省略它们。加入两段含相同历史 `CallID` 的工具批次 | 三次实际输入中用户原文及顺序一致，来源匹配各自原文块，批次保持配对。使用相同模型和工具回执时，压缩前后的 `Result.Messages` 一致 |
| 摘要准入与重试 | 只剩一次模型请求，原视图在硬限额内。另对一个已失败的基准版本重复触发 | 保留该次调用给普通请求，摘要发起零次。失败版本最多产生一次摘要请求 |
| 摘要失败后回退 | 摘要流截断、非法结构或引用失效。上版视图之后已有新工具结果。分别让缩减视图、补齐后的上版、完整原文成为第一个可用候选 | 每次只发送对应候选，包含最新完整批次。摘要发起一次、工具重放零次，摘要消耗仍入账 |
| 无法回退 | 保护项已经超限，或候选都超限；单独注入计量不可用 | 前者 `BudgetExceeded`，计量不可用时 `Failed/context_measurement_unavailable`。两者正常模型派发均为零，已提交消息仍可读取 |
| 取消和迟到 | 摘要快完成时先提交取消，或注入基准版本已变化的候选 | 旧候选未发布。取消保留原终态；版本变化按最新消息组装。整轮提交历史按原回执返回 |
