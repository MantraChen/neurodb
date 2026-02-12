## NeuroDB

NeuroDB 是一个实验性的 **学习型索引数据库引擎**，目录结构基于 Go 的 `cmd/` 与 `pkg/` 约定设计，包含：

- `cmd/server`：服务启动入口
- `pkg/sql_proxy`：SQL 解析与协议代理层（适配 MySQL/PostgreSQL 协议）
- `pkg/storage`：底层存储抽象与驱动适配
- `pkg/core`：核心索引与数据结构
  - `memory`：内存表（MemTable，处理实时写入）
  - `learned`：学习型索引（SSTable，只读/大批量数据）
  - `buffer`：缓冲池与页面缓存
- `pkg/model`：线性回归、神经网络、RMI 等模型
- `pkg/optimizer`：自适应代价模型（查询优化器）
- `pkg/monitor`：工作负载感知与统计信息
- `configs`：配置文件

本仓库当前仅包含目录结构与占位代码，后续可逐步实现各模块的具体逻辑。
