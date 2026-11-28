# TUI · 本地有界搜索验收

2026-11-28，macOS arm64，Go 1.24.3。用 `go build -o <临时目录>/superagent ./cmd/superagent` 构建实际命令。PTY 驱动独立进程，模型经回环 Anthropic Messages SSE 夹具返回受控回复。使用真实文件、只读授权、回执日志、Run 检查点和会话索引。

```sh
<临时目录>/superagent -model loopback-search -state-dir <状态目录> -parallel 2 -workspace <工作区>
```

| 用例 | 验收结果 |
| --- | --- |
| 1. 命令与工作区 | 默认请求无工具。显式启用后仅声明 `search`。原纯文本历史和在途 Run 在启用后仍可读回。空、缺失、非目录及状态路径重叠在首个模型请求前拒绝，含经符号链接重叠的状态路径与回执子目录。绝对路径、父目录、越界链接和 FIFO 均明确未执行，扫描扣量为零 |
| 2. 两轮搜索 | 工具声明的 SSE 增量已到而完整回复未到时，回执数为零。放行后执行一次，扫描 13 字节，模型两轮。最终第 1 行预览仅出现一次，甲 v1、乙 v0。另在工具派发后中断 `Observe`，独立 `Wait` 仍只保存一份确认结果 |
| 3. 搜索内限额 | 完整零命中、扫描上限、单行上限、预览上限、命中上限和回执上限均按结果显示。界面最多 3 条预览。文件中的终端控制字符不会进入画面内容 |
| 4. Run 扫描预算 | 甲的第一份部分搜索用完 1 MiB 扫描余额，后续搜索派发为零。界面同时保留部分结果和独立的 Run 预算停止。乙独立答复，甲乙各保存到 v1 |
| 5. 回执已落盘、未结算 | 在真实搜索写完回执后强制退出，并替换源文件内容。新进程按 Enter 前，原 Run 模型、工具和回执查询新增为零。恢复按原 RunID 与 CallID 查询一次，模型 Step 2 收到原文件结果一次。总执行一次，历史 v0 → v1。用普通构建产物作为重进进程复验同一链路 |
| 6. 已结算或已有终态 | 第一分支在批次结算后、Step 2 模型意图保存前退出，从 Step 2 继续，查询和重复结算均为零。第二分支使用实际命令，在 Run 终态已持久、会话索引锁仍持有时退出，重进只补存历史。两分支再次重进仍为 v1 |
| 7. 原回执不可用 | 缺失、损坏、CallID 不符和指纹不符的四个分支都在原核对期限结束后以 `ToolOutcomeUnknown` 收尾。新增模型派发与工具重发都为零，未确认工具批次未入历史，已知用户消息入 v1。旧进程占用或同路径替换工作区时，甲保留原待恢复 ID，乙可继续 |

用例 3 的实测。源文件为 8 MiB 与 64 MiB 的两组使用稀疏尾部，整个扫描范围写入了真实短行。RSS 为命令进程峰值。

| 样本 | 源大小 B | 扫描 B | 编码回执 B | 峰值 RSS B |
| --- | ---: | ---: | ---: | ---: |
| 完整零命中 | 30 | 30 | 294 | 15417344 |
| 大文件零命中 | 8388608 | 1048576 | 329 | 15007744 |
| 更大文件零命中 | 67108864 | 1048576 | 329 | 14499840 |
| 超长单行 | 32768 | 20480 | 325 | 14991360 |
| 预览上限 | 1024 | 1024 | 611 | 15106048 |
| 命中数上限 | 704 | 704 | 2362 | 15106048 |
| 回执上限 | 16128 | 16128 | 16139 | 19742720 |

用例 5 的原环境计量为 13 B，随回执持久保存。回执未结算时重启，从 1 MiB 预留中按实际 13 B 结算一次。计量缺失、超限或与结果不符的三个分支按原预留结算。再次重启保持原扣量和会话回执，所有分支原搜索只执行一次。 [进程验收](../../cmd/superagent/search_recovery_test.go)

甲的下列 ID 共用前缀 `b96ec71b3645a929d08ee7c1d1997950d22b6b0c6db7650c39950efa3a3bc1cc.`。

| 断点或结果 | 原 RunID 后缀 | CallID |
| --- | --- | --- |
| 正常两轮 | `2b1cf6df346286f3bf1b2e57343d32c1` | `anthropic:1:a1` |
| Run 扫描预算停止 | `66dc8e2346e92f1b8c3c01ef02e987eb` | `anthropic:2:second` |
| 持久回执后退出，计数验证 | `06b64e8584f83dfba08dc7f7914ae360` | `anthropic:1:original` |
| 持久回执后退出，普通命令重进 | `b23eef2ae63fd1657205fe8d2c22da2d` | `anthropic:1:original` |
| 已结算、终态补存 | `4e69d27e6e647f0a3db77b49f02c67a6` / `2db058d7336d806b3dc3107e3e2b068e` | `anthropic:1:settled` / `anthropic:1:finished` |
| 缺失、损坏、CallID 不符、指纹不符 | `29bf757d8aaa9126a584c2eda6263710` / `7aaa61f4f38eaee57c20ba5589d4cf17` / `28f60d5eb253a255d40f68a77b315260` / `3b8cf72606bdf4024f1c0539ec0c1a80` | `anthropic:1:original` |

故障进程使用同一命令装配与真实搜索工具，在回执落盘后或下一模型意图保存前通过测试探针暂停。终态补存断点使用普通二进制和真实会话索引锁。原回执损坏与错配分别定向修改真实日志。 [命令和限额验收](../../cmd/superagent/search_cases_test.go) · [重进验收](../../cmd/superagent/search_recovery_test.go)

## 复验

7 项命令与独立进程验收、工具观察中断 PTY 用例、全量回归、全量 race、`go vet` 和 OpenSpec 严格校验均通过。换根和进程占用场景用界面就绪帧同步输入后，在 race 下另连续通过四次。

```sh
GORACE=atexit_sleep_ms=0 go test -count=1 -v ./cmd/superagent ./internal/tui -run '^(TestSearchAcceptance|TestPTYToolObservationInterrupt)'
SUPERAGENT_ANTHROPIC_LIVE=0 SUPERAGENT_ANTHROPIC_EVAL=0 go test -count=1 ./...
GORACE=atexit_sleep_ms=0 SUPERAGENT_ANTHROPIC_LIVE=0 SUPERAGENT_ANTHROPIC_EVAL=0 go test -race -count=1 ./...
go vet ./...
openspec validate --all --strict
```
