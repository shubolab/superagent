# run-lifecycle Specification

## Purpose

让 Go 宿主能够经本地 Mesh 启动一次 Run、消费实时模型文本、请求取消并独立读取最终结果。为纯文本闭环规定受理、事件、停止和模型调用的可观察行为。

## Requirements

### Requirement: 本地 Run 受理

系统 MUST 通过已绑定运行器受理授权主体的纯文本 Run，在受理时固定消息、模型绑定和有效预算，并维持同一运行器实例内的受理去重。

#### Scenario: 本地绑定与输入快照
- **WHEN** 宿主登记本地模型并以有效的非空 `RunID` 启动 Run
- **THEN** `Start` 回报已受理，唯一一次模型请求使用固定的模型、指令、历史和本次输入
- **AND** `Start` 返回后取消其调用上下文或修改原历史，Run 继续使用自己的上下文及原快照

#### Scenario: 未能受理
- **WHEN** 模型引用未登记、主体无权使用模型、`RunID` 为空或请求预算非法
- **THEN** `Start` 返回可识别的调用错误，Run 未受理，模型调用次数为零

#### Scenario: 相同启动请求重送与冲突
- **WHEN** 同一主体在同一运行器和保留期内，用相同 `RunID` 并发或顺序重送相同请求
- **THEN** 所有成功响应对应原 Run，模型最多调用一次
- **AND** 同一标识配不同请求返回启动冲突

### Requirement: 模型完成与预算

运行器 MUST 在每次派发和接收处执行该 Run 的模型额度，并在有效的纯文本 `ModelCompleted(stop)` 到达后按停止顺序裁决成功。

#### Scenario: 正常完成与消息增量
- **WHEN** 可控模型依次交付“你”“好”和完整答复“你好”
- **THEN** Run 以 `Succeeded` 结束，`Answer` 为“你好”，`Messages` 依次为本次输入和最终答复
- **AND** 历史快照及模型草稿留在各自的输入和观察路径

#### Scenario: 提前 EOF 或异常完成
- **WHEN** 正常 Run 在完整回复前遇到 EOF、模型错误，或收到表示工具调用的完成原因
- **THEN** Run 以 `Failed` 结束，`Messages` 只含本次输入，`Answer` 为空，已交付增量仍可作为预览

#### Scenario: 模型输入超出有效额度
- **WHEN** Run 已受理，而完整模型请求的规范字节数超过有效输入上限
- **THEN** 模型请求次数为零，`Wait` 取得 `BudgetExceeded` 及可区分的输入超限原因

#### Scenario: 模型输出等值与越界
- **WHEN** 回复的输出上限为六字节，模型交付“你”“好”及完整答复“你好”
- **THEN** 已交付的增量和重复的完整快照只计一次，六字节完整答复可成功
- **AND** 同样的模型输出在五字节上限下使 Run 返回 `BudgetExceeded`，模型流被关闭

### Requirement: 有界实时观察

每个 Run MUST 提供一次有界观察订阅。事件按 Run 连续编号，观察的停止和错误只影响该订阅。

#### Scenario: 顺序、终态与 EOF
- **WHEN** 观察者在有界缓冲和保留期内首次订阅正常完成的 Run
- **THEN** 依次读到 `run.started`、文本增量、`message.completed`、唯一的 `run.finished`，然后读到 `io.EOF`
- **AND** 文本增量可合并，合并后文本及连续 `Seq` 保持正确，终态与 `Wait` 一致

#### Scenario: 关闭或取消观察
- **WHEN** 宿主对阻塞的观察调用 `Close`，或取消观察上下文
- **THEN** `Recv` 解阻并分别报告流已关闭或上下文错误，Run 继续
- **AND** `Close` 可重复调用，后续订阅返回可识别的拒绝错误

#### Scenario: 慢观察者
- **WHEN** 暂停事件消费并使该订阅超过字节容量
- **THEN** 已缓冲的连续前缀先交付，随后报告溢出，`Wait` 仍能取得原 Run 的终态

### Requirement: 独立等待和稳定终态

系统 MUST 在保留期内向拥有者提供可并发、可重复读取的终态快照。等待上下文控制本次读取，提交的终态保持固定。

#### Scenario: 等待截止及多等待者
- **WHEN** 第一个 `Wait` 在模型阻塞时到期，随后模型完整结束，多个调用继续等待
- **THEN** 第一个调用返回上下文错误，后续调用得到同一逻辑结果及空 Go 错误
- **AND** 对一个返回快照的修改只影响该快照

#### Scenario: 用原标识读取或跨主体访问
- **WHEN** 拥有者以 `Client.Run(id)` 取得句柄，或另一主体使用同一标识尝试观察、等待或取消
- **THEN** 拥有者读取原 Run，另一主体得到记录不可查

### Requirement: 显式取消与到期

系统 MUST 将已记录的取消和 Run 截止传给在途模型，并按 Run 内的裁决顺序提交唯一终态。

#### Scenario: 取消先于模型完成
- **WHEN** 可控模型阻塞，运行器先接受 `Cancel`，模型随后尝试交付最终答复
- **THEN** 重复取消得到确认，模型收到停止信号且流能关闭，`Wait` 返回 `Cancelled` 和空 Go 错误
- **AND** 迟到答复留在已关闭的推进路径，终态中的 `Messages` 只含本次输入

#### Scenario: 模型成功先于取消
- **WHEN** 正常答复已提交成功终态，宿主随后调用 `Cancel`
- **THEN** 取消调用确认 Run 已结束，`Wait` 继续返回原成功快照

#### Scenario: Run 截止时间到达
- **WHEN** 模型流阻塞且 Run 截止时间先到达
- **THEN** 在途模型收到停止信号，`Wait` 返回 `TimedOut` 和空 Go 错误

#### Scenario: 两个 Run 的取消隔离
- **WHEN** 两个 Run 使用同一已登记模型并独立推进，宿主只取消其中一个
- **THEN** 被取消的 Run 收尾为 `Cancelled`，另一个 Run 仍可输出并完成，各自的观察和结果归属原 `RunID`
