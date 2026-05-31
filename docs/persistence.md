# 持久化

核心包只依赖 `Store` 接口。

`Store` 负责：

- 流程实例读写。
- 任务执行读写。
- 历史事件追加。
- 幂等记录。
- 到期任务扫描。
- 并发版本控制。
- Outbox 记录。

默认实现：

- MySQL：`persistence/mysql`
- PostgreSQL：`persistence/postgres`
- 内存：`workflowtest`

MySQL 和 PostgreSQL 都提供默认建表 SQL。生产环境可以直接调用 `InitSchema`，也可以把 SQL 交给迁移工具管理。
