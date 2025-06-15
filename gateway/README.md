# Netty Gateway

基于Netty实现的分布式长连接网关服务，用于管理和维护客户端与服务端之间的长连接通信。支持网关集群化部署，并为业务服务提供简单的消息推送能力。

## 功能特性

### 已实现功能
- 连接管理
  - 支持客户端与网关建立长连接
  - 提供客户端认证机制
  - 维护连接会话信息
  - 支持连接的优雅关闭
  - 实现心跳机制保活

- 消息处理
  - 自定义二进制消息协议
  - 支持消息校验和
  - 支持扩展字段
  - 支持不同类型消息：认证、心跳、业务消息等
  - 支持消息的双向传输

- 路由转发
  - 支持基于业务类型的消息路由
  - 支持路由的负载均衡

- 会话管理
  - 维护客户端会话状态
  - 支持会话属性的存储和读取
  - 支持会话的超时管理

### 计划功能
- 集群管理
- 分布式路由表
- 分布式会话存储
- 业务服务SDK
- 监控和告警
- 安全机制

## 快速开始

### 环境要求
- JDK 8+
- Maven 3.6+
- Nacos 2.5.1+

### 构建项目
```bash
mvn clean package
```

### 运行测试
```bash
# 运行单元测试
mvn test

# 运行完整检查（包括代码风格、覆盖率等）
mvn verify
```

### 启动服务
```bash
java -jar target/gateway-${version}.jar
```

## 项目结构
```
gateway/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── io/netty/gateway/
│   │   │       ├── codec/      # 消息编解码
│   │   │       ├── common/     # 公共工具
│   │   │       ├── config/     # 配置管理
│   │   │       ├── connection/ # 连接管理
│   │   │       ├── handler/    # 消息处理
│   │   │       ├── protocol/   # 协议定义
│   │   │       ├── route/      # 路由管理
│   │   │       └── session/    # 会话管理
│   │   └── resources/
│   └── test/
└── pom.xml
```

## 开发规范

### 代码质量
项目使用以下工具确保代码质量：
- Checkstyle：代码风格检查
- JaCoCo：代码覆盖率检查（要求80%以上）
- JUnit 5：单元测试框架

### 提交规范
提交代码前需要：
1. 通过所有单元测试
2. 通过代码风格检查
3. 确保测试覆盖率达标
4. 遵循Git commit message规范

## 技术栈
- Netty 4.2.3：网络通信框架
- Nacos 2.5.1：服务注册与发现
- JUnit 5：单元测试框架
- Mockito：测试模拟框架
- SLF4J & Log4j2：日志框架
- Jackson：JSON处理

## 文档
- [需求文档](PRD.md)
- [设计文档](docs/design.md)
- [API文档](docs/api.md)

## 贡献指南
1. Fork 项目
2. 创建特性分支
3. 提交变更
4. 推送到分支
5. 创建Pull Request

## 开源协议
[Apache License 2.0](LICENSE) 