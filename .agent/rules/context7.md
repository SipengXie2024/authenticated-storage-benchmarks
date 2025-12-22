## Context7 MCP 使用规则

Context7 提供最新的库文档和代码示例，解决 LLM 训练数据过时的问题。

### 必须使用的场景

以下情况**强制**使用 Context7 获取文档：

1. **实现涉及第三方库的功能时**
   - 用户提到使用某个框架/库（如 React、Tokio、diesel 等）
   - 需要调用不熟悉的 API
   - 库版本可能与训练数据不同

2. **遇到库相关错误时**
   - 编译错误提示 API 用法不正确
   - 运行时错误与库行为相关
   - 版本兼容性问题

3. **用户询问特定库的用法时**
   - "如何用 X 库实现..."
   - "X 库的 Y 功能怎么用"
   - "X 库的最佳实践是什么"

4. **需要最新文档时**
   - 2024年后发布的库/版本
   - 频繁更新的框架（如 Next.js、Rust crates）
   - API 可能已废弃或更改的情况

### 标准工作流

```
1. resolve-library-id → 获取正确的库 ID
2. get-library-docs → 获取文档内容
   ├─ mode='code' → API 引用、代码示例、函数签名
   └─ mode='info' → 概念指南、架构设计、最佳实践
```

**重要**：必须先调用 `resolve-library-id`，除非用户明确提供了 `/org/project` 格式的 ID。

### mode 参数选择指南

| 需求类型 | mode | 示例 |
|---------|------|------|
| 函数签名和参数 | `code` | "tokio::spawn 怎么用" |
| 代码示例 | `code` | "给我一个 diesel 查询的例子" |
| 概念理解 | `info` | "React hooks 的设计理念" |
| 架构问题 | `info` | "Next.js App Router 如何工作" |
| 最佳实践 | `info` | "Rust 错误处理的推荐方式" |

### topic 参数最佳实践

- 使用具体的功能名称：`routing`、`hooks`、`async`、`error-handling`
- 避免过于宽泛的主题：❌ "usage" ✅ "connection-pooling"
- 如果首次搜索不够，尝试 `page=2, 3...` 获取更多内容

### 与其他工具的协同

**Context7 + Thinking**：学习复杂库时
```
1. Thinking: 规划学习路径和关键概念
2. Context7 (mode='info'): 理解整体架构
3. Context7 (mode='code'): 获取具体实现示例
4. Thinking: 整合知识，制定实现方案
```

**Context7 + Search_code**：在项目中应用库
```
1. Search_code: 查找项目中的现有用法
2. Context7: 对比官方推荐做法
3. 决定是否需要更新现有代码
```

**Context7 + WebSearch**：获取最新信息
```
1. Context7: 获取稳定版文档
2. WebSearch: 查找最新版本变更、已知问题
```

### 不使用的场景

- 标准库功能（如 Python 内置函数、Rust std）
- 简单且稳定的 API（不太可能变化）
- 用户已提供完整的 API 文档或示例
- 纯粹的语法问题（与库无关）

### 示例场景

**场景1**: 用户说"帮我用 sqlx 实现数据库连接池"
```
→ resolve-library-id("sqlx")
→ get-library-docs(id, topic="connection-pool", mode="code")
```

**场景2**: 用户问"Axum 中间件的工作原理"
```
→ resolve-library-id("axum")
→ get-library-docs(id, topic="middleware", mode="info")
```

**场景3**: 编译错误显示 serde 宏用法不对
```
→ resolve-library-id("serde")
→ get-library-docs(id, topic="derive macros", mode="code")
```
