# 阶段 A · 本地 Mesh 配置

## Why

Go 宿主已能直接登记组件。文件部署需要把逻辑引用映射到相同的本地组件和宿主策略。

## What Changes

- 读取带版本的 YAML，校验字段、重复项、组件引用和运行器可见范围。
- 解析为同一个 Go MeshConfig；运行器可为工作区请求补全默认环境引用。

## Capabilities

### New Capabilities

- `local-mesh-config` 定义进程内组件和宿主策略的配置装配。

## Impact

新增 SDK 配置读取入口。现有 Go 装配接口继续使用。
