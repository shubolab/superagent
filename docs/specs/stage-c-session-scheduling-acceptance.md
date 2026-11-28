# 阶段 C · 本地会话调度验收

2026-11-28，macOS arm64，Go 1.24.3。变更为 `stage-c-local-session-scheduling`。使用真实 SDK 本地 Mesh、会话目录和阶段 B 检查点，可控模型与慢工具。核心场景的活跃上限为 1。

| 用例 | 观测 |
| --- | --- |
| 单名额交接 | 两个客户端共享名额。甲卡在工具时，乙的预约、检查点和派发数均为 0。甲终止且历史仍为 v0 时，乙自动流式输出并提交到 v1。甲模型 2 次、工具 1 次，乙模型 1 次 |
| 等待取消和超时 | 主动取消、调用方截止、宿主排队到期分别返回不同错误。三个被移除请求均无派发。等待位置可复用，重新提交的请求获准后获得完整执行预算 |
| 排满、FIFO 和去重 | 分别测试等待上限 0、1、2。排满立即拒绝，原请求重复等待只派发 1 次。异参和同会话不同 ID 返回冲突及会话忙。晚来的丁在丙之后派发 |
| 取消竞争 | 取消先到，候选请求从等待队列移除。获准但尚未受理时取消，候选零派发，下一项获准。已受理时使用 `Run.Cancel`，慢工具确认前仍占用名额 |
| 异常与重复释放 | 明确拒绝、模型失败与取消终态都推进后继请求。重复释放、关闭观察和 `Wait` 超时保持正确计数。受理不明时保留名额，可信核对后释放。带 `CleanupPending` 的终态释放 Run 名额 |
| 恢复门禁 | 满载时可提交可信终态和已封存增量。活跃 Run 的恢复复用原名额，替换失去推进权的本地运行也沿用原名额。需继续派发的提交补跑排队，释放后仅派发原 `RunID` 一次。直接启动和直接恢复的绕行派发数为 0 |
| 进程重启 | 在甲持有原检查点、乙纯排队时终止原进程。新装配的等待队列为空。甲按原 `RunID` 从检查点继续，乙仅在显式重送后派发 |

[SDK 调度验收](../../sdk/session_scheduling_test.go) · [取消与释放验收](../../sdk/session_scheduling_release_test.go) · [恢复与进程验收](../../sdk/session_scheduling_recovery_test.go) · [可控时钟](../../schedule/scheduler_test.go) · [运行器故障注入](../../runner/scheduling_test.go)

## 复验

7 个用例、全量 Go 普通回归、全量 race、`go vet` 和 OpenSpec 严格校验均通过。Anthropic 实连冒烟和评估开关为 0。

```sh
go test -count=1 -run '^(TestSessionScheduling|TestScheduled|TestWaitDeadline|TestUncertainAdmission|TestGrantVersus)' ./sdk ./runner ./schedule
go test -count=1 ./...
GORACE=atexit_sleep_ms=0 go test -race -count=1 ./...
go vet ./...
openspec validate --all --strict
```
