# Kafka Topic 管理工具（MVP）

基于 **Spring Boot 2.7.x + JDK 8 + Maven + kafka-clients 2.8.0** 的 Kafka Topic 管理后端服务。

## 1. 项目说明

当前版本提供 RESTful API 方式的 Kafka Topic 管理能力，支持：

- Kafka 集群配置（默认单集群，结构已支持扩展为多集群）
- 查询 Topic 列表
- 查询 Topic 详情
- 创建 Topic
- 删除一个或多个 Topic（逗号分隔）
- 集群健康检查
- 统一返回结构
- 全局异常处理
- 基本操作日志输出

## 2. 技术栈与版本

- JDK: 8（编译目标 1.8）
- Spring Boot: 2.7.18
- Kafka Client: 2.8.0
- Build Tool: Maven

## 3. 启动方式

### 3.1 本地运行

```bash
mvn spring-boot:run
```

### 3.2 打包运行

```bash
mvn clean package
java -jar target/kafkautils-1.0.0-SNAPSHOT.jar
```

默认端口：`8080`

## 4. 配置说明

配置文件：`src/main/resources/application.yml`

```yaml
server:
  port: 8080

spring:
  application:
    name: kafkautils

kafka:
  active-cluster: default
  clusters:
    default:
      bootstrap-servers: localhost:9092
      security-protocol: PLAINTEXT
      request-timeout-ms: 5000
```

可选扩展（按需）：

- `sasl-mechanism`
- `sasl-jaas-config`

## 5. 接口说明

统一响应结构：

```json
{
  "code": 200,
  "message": "操作成功",
  "data": {}
}
```

### 5.1 查询 Topic 列表

- `GET /kafka/topic/list`

返回字段包含：
- `topicName`
- `partitions`
- `replicationFactor`
- `configSummary`（如 cleanup.policy / retention.ms / max.message.bytes）

### 5.2 查询 Topic 详情

- `GET /kafka/topic/detail?topicName=xxx`

返回字段包含：
- Topic 基础信息
- 分区详情（leader/replicas/isr）
- 常用配置（cleanup.policy、retention.ms、max.message.bytes）

### 5.3 创建 Topic

- `POST /kafka/topic/create`

请求示例：

```json
{
  "topicName": "test-topic",
  "partitions": 3,
  "replicationFactor": 1,
  "retentionMs": 604800000,
  "cleanupPolicy": "delete"
}
```

参数校验：
- `topicName`：必填，长度与字符合法
- `partitions`：必须 > 0
- `replicationFactor`：必须 > 0

### 5.4 删除 Topic（支持批量）

- `POST /kafka/topic/delete`

请求示例：

```json
{
  "topicNames": "test-topic-1,test-topic-2",
  "forceDelete": false
}
```

说明：
- `topicNames` 支持逗号分隔多个 Topic
- 删除前会保护系统 Topic，不允许删除：
  - `__consumer_offsets`
  - `__transaction_state`
  - `__schema_history`

### 5.5 Kafka 集群健康检查

- `GET /kafka/cluster/health`

返回集群 ID、controller、节点数量与节点列表。

## 6. 注意事项

1. 当前版本通过 Kafka AdminClient 提供 Topic 管理能力。
2. 集群不可达时会返回统一异常结构。
3. 建议生产环境开启认证并使用安全协议。

## 7. 关于“Topic 创建时间”

Kafka AdminClient 在当前常规能力下通常无法直接查询 Topic 创建时间。

**本版本未提供 Topic 创建时间字段，且不会伪造该字段。**

如需支持该信息，通常需要依赖外部审计日志、平台侧元数据记录或额外扩展机制。
