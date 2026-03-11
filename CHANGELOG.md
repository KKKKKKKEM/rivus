# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-11

### Added
- `Pipeline` — 多节点流水线，支持管道符 `|` 组合节点
- `@node` / `node()` 装饰器 — 将函数快速包装为节点
- `BaseNode` — 类式节点基类
- `Context` — 线程安全的 item + 共享元数据容器，支持跨进程序列化（pickle）
- 多并发模型 — `concurrency_type="thread"`（默认）和 `concurrency_type="process"`
- 生成器支持 — 节点 `yield` 多个值实现扇出
- `gather=True` — 插入屏障，将上游所有输出汇聚为列表后整体传入节点
- `ctx.stop()` / `ctx.request_stop()` — 节点内优雅停止流水线，状态记录为 `STOPPED`
- `PipelineReport` / `NodeReport` — 结构化运行报告，含 min/max/avg/p50/p95 耗时统计
- `LogConfig` / `RivusLogger` — 可配置的结构化日志体系
- 完整异常体系：`RivusError`, `NodeError`, `PipelineError`, `PipelineTimeoutError`, `ContextKeyError`, `PipelineStop`
