# TUI · 本地重进验收

2026-11-28，macOS arm64，Go 1.24.3。伪终端驱动独立子进程。每次重进使用同一真实的本地会话、检查点、回执目录，以及可控模型和持久回执工具。断点使用真实进程退出。两种无检查点的授予分支在实际调度授予后，定向写入未尝试或已尝试预约。终态过期通过回拨原检查点的完成时间触发。

| 用例 | 结果 |
| --- | --- |
| 双会话历史 | 甲乙各从 v1 读回原消息，各发送一次后到 v2。后续模型请求各含自己的原两条消息和新输入，顺序及内容相符。损坏甲的会话索引后，乙仍可发送并保存到 v3 |
| 原 Run 接续 | 甲在 v0 重进，按 Enter 前模型新增派发和回执查询为 0。之后原 Run Step 1 → 2，持久回执查询 1 次，工具执行总数 1，历史到 v1。旧观察流只在原进程打开一次。乙独立到 v1 |
| 终态补存 | B 终态和 C 封存两分支都从 v0 到 v1，再重进保持 v1。封存分支的 B 终态已过期，注入一次保存失败，乙仍独立保存到 v1。甲重试后保留原回执，新增模型和工具派发均为 0 |
| 纯排队退出 | Ctrl+D 确认乙未发送，强关分支同样保留乙 v0 和空队列。乙原请求派发为 0，重新输入后用新 ID 派发 1 次，保存到 v1。甲保留原检查点和 v0 |
| 授予与退出竞争 | 乙三分支均为 v0，模型新增派发为 0。未尝试预约清理并恢复草稿。已受理未应答分支显示可继续，已尝试但无检查点分支显示待核对，两者均保留原 ID |
| 恢复门禁 | 旧进程占用、记录缺失或损坏、绑定变化、终态过期均保留甲原 ID、v1 和既有历史。已到原截止或已受理 Esc 的分支按相应终态到 v2。七个分支甲模型新增派发为 0，乙均独立到 v1 |
| 恢复等待再次退出 | 第二个进程撤回乙恢复排队，乙在 v0 且原检查点字节一致。第三个进程显示同一 ID，按 Enter 后才派发 Step 1 一次并到 v1。所有退出触发的 `Run.Cancel` 调用均为 0 |

一次复验的原 `RunID` 记录。每个 ID 由对应会话前缀、点和表中的后缀组成。甲前缀为 `3a648860c9cdcccf0efe19b510c1cd31f8763a3ac615029f68771b61958aaa85`，乙前缀为 `1c1fbea0a1cf284189a701a104ea562fcd04237cba663d4c2b1edc0c5b1c9449`。

| 场景 | 会话 | 原 ID 后缀 |
| --- | --- | --- |
| 双会话首轮 | 甲、乙 | `b83f9cc895aa427f02942628c32d786f`、`4ff637c427b360cc9691bc28af5afc7a` |
| 工具回执 | 甲 | `3d64378efb997ab1827f8fb1de8b0ce2` |
| 终态、封存 | 甲 | `8e99619704302ba78de7e161b9f173ab`、`e6921861d436c17ed66256f10a7fa381` |
| 原纯排队 Ctrl+D、强关 | 乙 | `2df64adb62ae9588f7b7d50cabe5a5c8`、`da7effcc7033f015673eb04bbc10a377` |
| 未尝试、已受理、已尝试 | 乙 | `429e974a0f639a7498ed57b5ccf4a907`、`a8c3052e2fc8c94f5ba7c8f86c7ef758`、`27bfba390408ee32850a8083f1951afe` |
| 旧进程占用、原截止、已停止 | 甲 | `09045c8d310d725c58c9464dc06f13d1`、`0dfa54972eafea3a77eba1da3a0c0b78`、`0a68e984cb61a79a1562b4525b221c7f` |
| 恢复排队再退出 | 乙 | `b89d06278f6de697ae954d0356dfbe9d` |

[独立进程验收](../../internal/tui/restart_cases_test.go) · [进程装配与故障注入](../../internal/tui/restart_pty_test.go) · [SDK 只读与授予复核](../../session_reopen_test.go)

## 复验

独立进程伪终端 7 项、SDK 定向检查、全量 Go 回归、全量 race、`go vet` 均通过。独立进程验收在 race 下另连续运行两轮。OpenSpec 2.2.0 严格校验 17 项通过。

```sh
go test -count=1 -v -run '^TestTUIRestart' ./internal/tui
go test -count=1 -run '^(TestInspectPendingReadOnly|TestCommitPendingResultIdempotent|TestResumeAdmittedRechecksAfterGrant)$' .
SUPERAGENT_ANTHROPIC_LIVE=0 SUPERAGENT_ANTHROPIC_EVAL=0 go test -count=1 ./...
GORACE=atexit_sleep_ms=0 SUPERAGENT_ANTHROPIC_LIVE=0 SUPERAGENT_ANTHROPIC_EVAL=0 go test -race -count=1 ./...
go vet ./...
openspec validate --all --strict
```
