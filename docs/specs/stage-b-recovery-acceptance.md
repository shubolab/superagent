# 阶段 B 恢复验收记录

验收使用同一宿主、独立进程、真实回环 mTLS 和可控模型。断点矩阵随 OpenSpec 变更 `stage-b-recovery-acceptance` 保存。 [恢复主规范](../../openspec/specs/run-recovery/spec.md)

| 用例 | 结果 | 观测 |
| --- | --- | --- |
| B1 · 活进程接管 | 通过 | 工具提交文件与证据后退出，回执库仍待完成。新运行器以所有权代次 2 查接单代次 1。旧运行器恢复活动后，结算检查点保持原值，旧派发和发布被拒。`c1` 请求、执行、发布、结算各 1 次，Run 成功且清理待确认 |
| B2 · 再次崩溃 | 通过 | 分别终止已收到重建回复的运行器、只结算 `c1` 的运行器、已提交整批的运行器、已持久化终态但尚未返回 `Wait` 的运行器。仅结算前分支再查 `c1`。`c1` 与 `c2` 各执行 1 次，第二轮模型请求包含同序的两份结果，按两份回执的实际编码长度扣减输出额度 |
| B3 · 证据不足 | 通过 | 原版本缺失、证据错配、持续断连到期及主动取消均以 `ToolOutcomeUnknown` 收尾。`c1` 的 1,024 字节结果预留只扣 1 次。新增模型派发与原 `Invoke` 重发均为 0。证据恢复后由独立新进程读取，终态、历史与账本一致 |
| B4 · 续跑门禁 | 通过 | 从已结算检查点触发工作区替换或耗尽模型请求额度，得到 `recovery_workspace_unavailable` 或 `model_request_limit`。`c1` 结算保持固定，后续派发为 0 |
| B6 · 生命周期 | 通过 | 三个进程分别写终态、在 4 秒保留期内读取、过期后读取。保留期内结果一致，过期为 `ErrRunNotFound`，查询与执行均无新增。内置非保留产物及压缩绑定均在受理时返回 `ErrRecoveryUnavailable`，检查点和派发均为 0 |

## B5 · 产物绑定

| 绑定 | 结果 | 观测 |
| --- | --- | --- |
| 只声明保留和封存的测试绑定 | 拒绝路径通过 | 绑定未实现 `DurableArtifactStore`，运行器在受理阶段返回 `ErrRecoveryUnavailable`，检查点、模型调用、工具调用均为 0 |
| SDK 内置 `NewLocalArtifactStore` | 拒绝路径通过 | 内置绑定不具备跨重启保留能力。运行器在受理阶段返回 `ErrRecoveryUnavailable`，检查点、模型调用、工具调用均为 0 |
| `NewLocalRetainedArtifactStore` 实际本地绑定 | 通过 | 用独立进程和同一私有目录验收，原文件 167,400 字节，单页 48 KiB。按原引用重读的完整 SHA-256 一致，定向复验的检查点 3,026 字节、回执 751 字节。回执、检查点仅保存引用 |

- **B5.1–B5.2。** 在分块中和封存后但回执未落盘时终止写入进程，产物不可公开读取。回执落盘后、发布前，以及发布后、结算前分别终止进程。后继进程沿用原引用、版本和到期时间。原 `c1`、可见发布、结果结算各一次，模型只接收一份配对结果，正文用量与小回执分别入账。 [发布断点测试](../../sdk/retained_artifact_process_test.go)
- **B5.3–B5.4。** 跨块页、末页和 EOF 均通过，越权、篡改引用和等长损坏的页面返回不可用。待核对产物遗失或损坏时，以 `ToolOutcomeUnknown` 收尾。已结算产物遗失时保留原结算并停止依赖派发。终态后产物过期会回收正文，`Wait` 仍返回原终态和引用。上述路径原工具重发均为零。 [分页与异常测试](../../sdk/retained_artifact_boundaries_test.go)
- **B5.5。** 旧进程保持存活时，从第 1 代接管至第 2 代。旧封存和发布被拒，新代次只确认原回执选中的版本。两个进程共享字节和份数上限，崩溃后的回收保留在途预留及未过期产物。替换存储目录以 `ErrRecoveryBinding` 阻止续跑。 [接管测试](../../sdk/retained_artifact_boundaries_test.go) · [配额测试](../../internal/artifact/retained_capacity_test.go)

## 复验

全量普通回归、竞态回归、`go vet ./...` 和 OpenSpec 严格校验通过。产物部署结果按上表单独记录。

```sh
go test -count=1 -v -run '^TestStageB[1-6]' ./sdk
go test -count=1 -v -run '^TestRetained(Artifact|Quota)' ./sdk ./internal/artifact
go test -count=1 ./...
go test -race -count=1 ./...
```
