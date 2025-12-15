# Project Context

## Purpose

Authenticated Storage Benchmarks (ASB) 是一个用于评估区块链认证存储系统性能的模块化基准测试工具。主要目标是支持多种键值存储后端和认证存储算法，收集全面的性能指标数据，为 LVMT（multi-Layer Versioned Multipoint Trie）的调优和评估提供支持。

核心目标：
- 对比评估不同认证存储方案的性能
- 收集执行时间、读写放大、内存使用、CPU profiling 等指标
- 支持随机工作负载和真实以太坊交易 trace 测试

## Tech Stack

- **语言**: Rust 1.67.0（通过 rust-toolchain 固定）
- **构建系统**: Cargo workspace
- **CLI 框架**: structopt
- **键值存储后端**:
  - RocksDB（默认）
  - MDBX
  - 内存数据库（kvdb-memorydb）
- **性能分析**: pprof-rs
- **脚本**: Python 3（run.py 用于批量实验）
- **密码学**: 基于 arkworks 的多项式承诺

## Project Conventions

### Code Style

- 使用 Rust 标准格式化（rustfmt）
- 模块按职责分离到独立 crate
- Trait-based 抽象接口设计：
  - `kvdb` 用于后端接口
  - `authdb-trait` 用于认证存储接口
- Feature flags 用于可选功能：
  - `light-hash`: 用 blake2b 替代 keccak256
  - `thread-safe`: 线程安全实现
  - `lmpts`: 启用 LMPTs 支持

### Architecture Patterns

采用模块化分层架构：

```
benchmarks/        -> 主程序入口 (asb-main)
asb-options/       -> CLI 参数解析
asb-backend/       -> 键值数据库后端抽象层
asb-authdb/        -> 认证存储实现
asb-tasks/         -> 任务生成器（随机/真实 trace）
asb-profile/       -> 指标收集与报告
```

数据流：CLI Options → Backend 创建 → AuthDB 包装 → Task 生成 → 执行与指标收集

### Testing Strategy

- 通过 `cargo build --release` 进行编译验证
- 使用 `--no-stat` 获取准确的运行时间测量
- 支持 `--seed` 参数进行可重复测试
- 内存约束测试通过 cgroup 限制（8GB）

### Git Workflow

- 主分支开发
- 使用 `.gitignore` 排除：
  - 构建产物（`/target`）
  - 密码学参数（`/pp`）
  - 实验数据（`/trace`, `/results`, `/warmup`）
  - 日志文件

## Domain Context

本项目面向区块链认证存储领域：

- **认证存储（Authenticated Storage）**: 提供数据完整性证明的键值存储，常用于区块链状态管理
- **Merkle Patricia Trie (MPT)**: 以太坊使用的认证数据结构
- **LVMT**: 多层版本化多点 Trie，本项目的研究贡献
- **AMT**: Authenticated Multipoint Evaluation Tree，LVMT 的构建模块
- **读写放大**: 衡量存储系统额外 I/O 开销的指标
- **Epoch**: 一组操作后请求 Merkle root，默认 10,000 次操作

## Important Constraints

- **Rust 版本固定**: 必须使用 1.67.0
- **存储需求**: 完整评估需要约 300GB 可用空间
- **内存限制**: 论文实验在 8GB 内存约束下进行
- **LMPTs 依赖冲突**: 启用 LMPTs 需手动修改 `asb-backend/Cargo.toml`
- **首次运行**: LVMT/AMT 首次使用需生成密码学参数（可能需要数小时）
- **平台**: 主要在 Ubuntu 22.04 上测试

## External Dependencies

- **RocksDB**: C++ 键值存储库
- **libmdbx**: MDBX 数据库
- **conflux-rust**: 借用部分类型和工具
- **openethereum**: MPT 实现来源
- **密码学参数**: 可从 Google Drive 下载预生成参数
- **Ethereum Trace API**: 用于获取真实交易 trace
