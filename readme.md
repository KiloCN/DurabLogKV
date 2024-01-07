# DurabLogKV

DurabLogKV 是一个基于 LSM-Tree（Log-Structured Merge-Tree）实现的高性能、持久化的键值存储引擎。它旨在提供高效的写操作处理，同时保持合理的读取性能，适用于处理大量数据的场景。

“DurabLogKV”命名的由来是结合了“Durability”（持久性）、“Log”（LSM-Tree的日志结构）和“KV”（Key-Value），反映了系统的持久性和基本结构。

## 特性

- **高效写操作**：利用 LSM-Tree 结构优化写入性能。
- **数据持久化**：保证数据的持久存储，防止数据丢失。
- **快速读取**：通过稀疏索引和布隆过滤器加速键值对的检索。
- **内存-磁盘结构**：结合 MemTable 和 SSTable 以优化读写性能。
- **层级存储机制**：支持多层级数据存储，优化空间使用和读取效率。
