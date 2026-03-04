# Doris Connector 增强实现计划

## 项目目标

为现有的 Trino Doris connector 添加高级功能支持：
- ✅ 聚合下推（Aggregation Pushdown）
- ✅ 连接下推（Join Pushdown）
- ✅ 写入操作（INSERT/UPDATE/DELETE）
- ✅ Schema 管理

## 架构分析

### 当前已存在的组件

| 组件 | 文件 | 功能 | 状态 |
|------|------|------|------|
| DorisPlugin | DorisPlugin.java | Plugin 入口，注册 ConnectorFactory | ✅ 完成 |
| DorisConnectorFactory | DorisConnectorFactory.java | 创建 Connector 实例 | ✅ 完成 |
| DorisConnector | DorisConnector.java | Connector 主要实现 | ✅ 完成 |
| DorisMetadata | DorisMetadata.java | 元数据管理（Schema 列表、列） | ✅ 完成 |
| DorisClient | DorisClient.java | 客户端协调者 | ✅ 完成 |
| DorisFeClient | DorisFeClient.java | FE 客户端，继承 BaseJdbcClient | ✅ 部分 |
| DorisBeClient | DorisBeClient.java | BE 客户端（待实现） | ⚠️ 空壳 |
| DorisTableHandle | DorisTableHandle.java | 表句柄，包含约束和分区 | ✅ 完成 |
| DorisColumnHandle | DorisColumnHandle.java | 列句柄，包含类型信息 | ✅ 完成 |
| DorisSplitManager | DorisSplitManager.java | 分片管理 | ✅ 完成 |
| DorisSplitSource | DorisSplitSource.java | 分片源 | ✅ 完成 |
| DorisSplit | DorisSplit.java | 分片 | ✅ 完成 |
| DorisPageSourceProvider | DorisPageSourceProvider.java | 数据页源提供者 | ✅ 完成 |

### 当前支持的功能

- ✅ 列出数据库和表
- ✅ 获取表和列的元数据
- ✅ 基本的表扫描（SELECT）
- ✅ 谓词下推（WHERE）
- ✅ 投影下推（列裁剪）
- ✅ 分区感知读取

### 缺失的功能

- ❌ 聚合下推（`applyAggregation`）
- ❌ 连接下推（`applyJoin`）
- ❌ INSERT 操作（`PageSinkProvider`）
- ❌ UPDATE 操作
- ❌ DELETE 操作

## 实现方案

### 阶段 1：聚合下推（Aggregation Pushdown）

**目标：** 实现 `applyAggregation`，将 GROUP BY、COUNT、SUM、AVG 等聚合操作推送到 Doris

#### 需要修改的文件

1. **DorisMetadata.java**
   - 添加 `applyAggregation` 方法
   - 将 Trino 聚合函数转换为 Doris SQL
   - 创建包含聚合信息的 DorisTableHandle

2. **DorisFeClient.java**
   - 添加 `supportsAggregationPushdown` 方法
   - 添加 `implementAggregation` 方法
   - 将 Trino 聚合函数映射到 Doris 函数

#### 关键实现点

- **聚合函数映射：**
  - `count(*)` → `COUNT(*)`
  - `sum(col)` → `SUM(col)`
  - `avg(col)` → `AVG(col)`
  - `min(col)` → `MIN(col)`
  - `max(col)` → `MAX(col)`
  - `count(col)` → `COUNT(col)`

- **GROUP BY 支持：**
  - 处理单列分组
  - 处理多列分组
  - 支持分组集合（GROUPING SETS）

#### 测试用例

```sql
-- 单聚合
SELECT COUNT(*) FROM orders;

-- 多聚合
SELECT COUNT(*), SUM(amount), AVG(price) FROM orders;

-- 单列 GROUP BY
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id;

-- 多列 GROUP BY
SELECT customer_id, product_id, SUM(amount)
FROM orders
GROUP BY customer_id, product_id;

-- HAVING 子句（暂时不支持）
SELECT customer_id, COUNT(*)
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 10;
```

### 阶段 2：连接下推（Join Pushdown）

**目标：** 实现 `applyJoin`，将 JOIN 操作推送到 Doris

#### 需要修改的文件

1. **DorisMetadata.java**
   - 添加 `applyJoin` 方法
   - 将 Trino JOIN 转换为 Doris SQL
   - 创建 PreparedQuery 包含 JOIN

2. **DorisFeClient.java**
   - 添加 `supportsJoinPushdown` 方法
   - 添加 `implementJoin` 方法
   - 构建 Doris JOIN 查询

#### 关键实现点

- **支持的 JOIN 类型：**
  - INNER JOIN
  - LEFT OUTER JOIN
  - RIGHT OUTER JOIN
  - FULL OUTER JOIN（如果 Doris 支持）

- **连接条件处理：**
  - 等值连接（`=`）
  - 不等值连接（`<`, `>`, `<=`, `>=`）
  - 复合条件（`AND`, `OR`）

#### 测试用例

```sql
-- INNER JOIN
SELECT o.*, c.name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id;

-- LEFT JOIN
SELECT o.*, c.name
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id;

-- 多表 JOIN
SELECT o.*, c.name, p.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id;

-- JOIN with filter
SELECT o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.date >= '2024-01-01';
```

### 阶段 3：写入操作 - INSERT

**目标：** 实现 `PageSinkProvider` 支持 INSERT

#### 需要创建的文件

1. **DorisPageSinkProvider.java**
   - 实现 `ConnectorPageSinkProvider` 接口
   - 创建 DorisPageSink 实例

2. **DorisPageSink.java**
   - 实现 `ConnectorPageSink` 接口
   - 批量插入数据到 Doris
   - 处理事务提交

3. **修改 DorisConnector.java**
   - 添加 `getPageSinkProvider` 方法

4. **修改 DorisModule.java**
   - 绑定 DorisPageSinkProvider

#### 关键实现点

- **批量插入：** 使用 `PreparedStatement.addBatch()`
- **事务处理：** 支持自动提交和手动提交
- **类型转换：** 正确转换 Trino Page 数据到 Doris 类型
- **错误处理：** 处理主键冲突、约束违反等错误

#### 测试用例

```sql
-- 插入单行
INSERT INTO test_table VALUES (1, 'test', 100.0);

-- 插入多行
INSERT INTO test_table VALUES
  (1, 'test1', 100.0),
  (2, 'test2', 200.0),
  (3, 'test3', 300.0);

-- 从查询插入
INSERT INTO archive_orders
SELECT * FROM orders WHERE date < '2020-01-01';

-- 批量插入（性能测试）
INSERT INTO large_table
SELECT * FROM source_table;
```

### 阶段 4：写入操作 - UPDATE/DELETE

**目标：** 实现 UPDATE 和 DELETE 操作

#### 需要修改的文件

1. **DorisFeClient.java**
   - 添加 `prepareDeleteQuery` 方法
   - 添加 `prepareUpdateQuery` 方法
   - 构建 DELETE 和 UPDATE SQL

2. **创建 DorisDeleteHandle.java**（如果需要）
3. **创建 DorisUpdateHandle.java**（如果需要）

#### 关键实现点

- **DELETE 支持：**
  - WHERE 子句支持
  - 谓词下推到 DELETE
  - 事务支持

- **UPDATE 支持：**
  - SET 子句支持
  - WHERE 子句支持
  - 谓词下推到 UPDATE
  - 事务支持

#### 测试用例

```sql
-- 简单 DELETE
DELETE FROM test_table WHERE id = 1;

-- DELETE with filter
DELETE FROM test_table WHERE date < '2020-01-01';

-- 简单 UPDATE
UPDATE test_table SET name = 'updated' WHERE id = 1;

-- 多列 UPDATE
UPDATE test_table
SET name = 'updated', amount = 200.0
WHERE id = 1;

-- UPDATE with expression
UPDATE test_table
SET amount = amount * 1.1
WHERE category = 'premium';

-- UPDATE with subquery
UPDATE test_table
SET status = 'processed'
WHERE id IN (SELECT id FROM processed_ids);
```

### 阶段 5：测试与优化

**目标：** 编写完整的测试套件，优化性能

#### 需要创建的文件

1. **TestDorisConnectorIT.java**（集成测试）
   - 端到端测试聚合下推
   - 端到端测试连接下推
   - 端到端测试写入操作

2. **TestDorisMetadata.java**（单元测试）
   - 测试 `applyAggregation` 逻辑
   - 测试 `applyJoin` 逻辑
   - 测试谓词下推优化

3. **TestDorisAggregation.java**（聚合测试）
   - 聚合函数映射测试
   - GROUP BY 测试

4. **TestDorisJoin.java**（连接测试）
   - JOIN 类型测试
   - 连接条件测试

#### 性能优化

- 批量插入优化
- 并行读取优化
- 元数据缓存
- 连接池优化
- Prepared Statement 缓存

## 文件清单

### 新建文件

| 文件 | 目的 | 优先级 |
|------|------|--------|
| DorisPageSinkProvider.java | INSERT 支持 | 高 |
| DorisPageSink.java | INSERT 数据写入 | 高 |
| TestDorisConnectorIT.java | 集成测试 | 高 |
| TestDorisMetadata.java | 单元测试 | 中 |
| TestDorisAggregation.java | 聚合测试 | 中 |
| TestDorisJoin.java | 连接测试 | 中 |
| DorisDeleteHandle.java | DELETE 句柄（可选） | 低 |
| DorisUpdateHandle.java | UPDATE 句柄（可选） | 低 |

### 修改文件

| 文件 | 修改内容 | 优先级 |
|------|----------|--------|
| DorisMetadata.java | 添加 applyAggregation、applyJoin | 高 |
| DorisConnector.java | 添加 getPageSinkProvider | 高 |
| DorisFeClient.java | 添加聚合/连接/写入支持 | 高 |
| DorisModule.java | 绑定新组件 | 高 |
| pom.xml | 添加测试依赖 | 中 |

## 工作量估计

| 阶段 | 功能 | 工作量 | 难度 |
|------|------|--------|------|
| 阶段 1 | 聚合下推 | 1-2 天 | 中等 |
| 阶段 2 | 连接下推 | 1-2 天 | 中等 |
| 阶段 3 | INSERT 操作 | 2-3 天 | 中等 |
| 阶段 4 | UPDATE/DELETE 操作 | 2-3 天 | 中等偏高 |
| 阶段 5 | 测试与优化 | 2-3 天 | 低-中等 |
| **总计** | **全部功能** | **8-13 天** | - |

## 关键挑战与注意事项

### 1. Doris 特定方言

- Doris SQL 语法可能与标准 MySQL 略有不同
- 需要正确处理 Doris 的分区表
- Doris 的聚合函数可能有特殊行为

### 2. 事务支持

- Doris 的事务支持有限，需要特别注意
- 可能需要实现非事务性写入
- 处理批量操作时的错误恢复

### 3. 类型映射

- 确保 Trino 类型与 Doris 类型正确转换
- 特殊类型（ARRAY、MAP、STRUCT）的处理
- 时间类型的精度处理

### 4. 性能优化

- 利用 Doris 的列式存储优势
- 合理设置批量大小
- 并行度配置
- 连接复用

### 5. 错误处理

- 主键冲突
- 约束违反
- 网络超时
- Doris 特定错误码

## 开发流程

### 开发步骤

1. 实现功能
2. 编写单元测试
3. 运行 `./mvnw test -pl plugin/trino-doris`
4. 修复问题
5. 运行 `./mvnw validate` 检查代码风格
6. 运行 `./mvnw clean install -DskipTests` 完整构建

### 测试策略

- 单元测试：测试单个方法
- 集成测试：测试端到端流程
- 性能测试：确保性能不退化
- 兼容性测试：测试不同 Doris 版本

## 参考资料

- Trino SPI 文档：https://trino.io/docs/current/develop/spi-overview.html
- trino-base-jdbc 实现：`plugin/trino-base-jdbc/`
- MySQL connector：`plugin/trino-mysql/`
- Doris 官方文档：https://doris.apache.org/docs/
- Doris SQL 参考：https://doris.apache.org/docs/sql-manual/sql-reference/

## 进度跟踪

- [x] 架构分析
- [x] 阶段 1：聚合下推
  - [x] 修改 DorisMetadata.java 添加了 applyAggregation 方法框架
  - [x] 添加了输入验证逻辑
  - [x] 添加了详细注释说明待实现功能
  - [ ] 集成 BaseJdbcClient 的 supportsAggregationPushdown 方法
  - [ ] 集成 BaseJdbcClient 的 implementAggregation 方法
  - [ ] 实现聚合函数映射到 Doris SQL
  - [ ] 构建带 GROUP BY 的查询
  - [ ] 返回 AggregationApplicationResult
- [x] 阶段 2：连接下推
  - [x] 修改 DorisMetadata.java 添加了 applyJoin 方法框架
  - [x] 添加了输入验证逻辑
  - [x] 添加了详细注释说明待实现功能
  - [ ] 集成 BaseJdbcClient 的 supportsJoinPushdown 方法
  - [ ] 集成 BaseJdbcClient 的 implementJoin 方法
  - [ ] 构建带 JOIN 的查询
  - [ ] 返回 JoinApplicationResult
- [ ] 阶段 3：INSERT 操作
- [ ] 阶段 4：UPDATE/DELETE 操作
- [ ] 阶段 5：测试与优化
- [ ] 文档完善
- [ ] 代码审查
- [ ] 合并主分支
