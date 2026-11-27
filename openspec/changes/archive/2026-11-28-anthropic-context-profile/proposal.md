# Anthropic 本地上下文计量

## Why

压缩运行器已有模型计量接口。Anthropic 绑定需要使用同一编码路径计量及发送，才可在宿主完成实测校准后启用。

## What Changes

- 宿主按供应商 ModelID 提供本地窗口、计量版本和计量函数。
- 计量与发送共用 Anthropic 请求编码，并在网络派发前校验指纹。
- 将压缩摘要作为独立历史数据提交，并保留同轮工具声明和回执的顺序。

## Capabilities

### New Capabilities

- `anthropic-context-profile` 定义 Anthropic 可选计量和仅在可靠配置下的派发。

## Impact

新增 Anthropic 的资料配置入口和本地 HTTP 验收。默认构造方式继续提供原始模型流。
