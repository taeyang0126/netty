# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 在此代码库中工作时提供指导。

**重要约束：在此代码库中工作时，所有回答必须使用中文。**

## 构建命令

### 构建所有模块
```bash
mvn clean install
```

### 构建单个模块
```bash
cd <module-name>
mvn clean install
```

### 运行测试
```bash
# 运行所有测试
mvn test

# 运行特定模块的测试
cd <module-name>
mvn test

# 运行单个测试类
mvn test -Dtest=<TestClassName>

# 运行单个测试方法
mvn test -Dtest=<TestClassName>#<testMethodName>
```

### 构建配置文件
- `-Pfast` - 跳过检查（checkstyle、forbiddenapis、revapi 等）和测试，加快构建速度
- `-Pleak` - 启用偏执级别的资源泄漏检测
- `-PnoUnsafe` - 禁用 Unsafe 使用
- `-Paggregate` - 生成聚合的 Javadoc 和交叉引用

### Checkstyle 和验证
```bash
# 仅运行 checkstyle
mvn checkstyle:check

# 运行所有验证（checkstyle、XML 格式、禁止的 API）
mvn validate
```

## 项目架构

Netty 是一个模块化的异步事件驱动网络应用框架。架构按层级组织：

### 核心模块（分层依赖）

1. **common** - 基础工具类（引用计数、资源泄漏检测、并发工具等）
2. **buffer** - ByteBuf 抽象（零拷贝、池化/非池化、堆/直接缓冲区）
3. **resolver** & **resolver-dns** - DNS 解析和主机名解析
4. **transport** - 核心 Channel 抽象、EventLoop、Pipeline 和 Bootstrap（包含 bootstrap 包）
5. **codec-base** & **codec** - 编码器/解码器基类和协议实现
6. **handler** - Channel 处理器（SSL、日志、流量整形等）

### 协议支持模块

- **codec-http**、**codec-http2**、**codec-http3** - HTTP 协议支持
- **codec-redis**、**codec-mqtt**、**codec-stomp**、**codec-smtp**、**codec-socks**、**codec-haproxy**、**codec-dns**、**codec-memcache**、**codec-xml** - 协议编解码器
- **codec-protobuf**、**codec-marshalling** - 序列化支持

### 传输层实现

- **transport-native-epoll** - Linux 原生传输（epoll）
- **transport-native-kqueue** - macOS/BSD 原生传输（kqueue）
- **transport-native-io_uring** - Linux io_uring 传输
- **transport-sctp**、**transport-udt**、**transport-rxtx** - 其他传输方式

### 核心架构模式

#### EventLoop 模型
- `EventLoop` - 处理 I/O 事件的单线程事件循环
- `EventLoopGroup` - 用于负载均衡的 EventLoop 组
- `SingleThreadEventLoop` - 单线程事件循环的基类实现
- I/O 操作始终在 EventLoop 线程上执行

#### Channel 和 Pipeline
- `Channel` - 表示一个打开的连接（socket、文件等）
- `ChannelPipeline` - 处理入站/出站事件的 `ChannelHandler` 链
- `ChannelHandlerContext` - 管道中每个处理器的上下文
- `AbstractChannelHandlerContext` - 管理处理器调用和事件传播

#### ByteBuf 抽象
- `ByteBuf` - 零拷贝缓冲区抽象（不同于 NIO ByteBuffer）
- `ByteBufAllocator` - 创建缓冲区（可以是池化或非池化）
- `PooledByteBufAllocator` - 内存池化分配器，性能更优
- `ReferenceCounted` - 引用计数，用于缓冲区生命周期管理
- PoolArena/PoolChunk - 使用 jemalloc 启发的内存池实现

#### Bootstrap
- `Bootstrap` - 客户端引导类
- `ServerBootstrap` - 服务端引导类（有独立的子 Channel）
- `AbstractBootstrap` - 基础配置和 Channel 工厂逻辑

#### Handler 类型
- `ChannelInboundHandler` - 处理入站事件（读、channel 激活等）
- `ChannelOutboundHandler` - 处理出站操作（写、连接、绑定等）
- `ChannelDuplexHandler` - 同时处理入站和出站
- `SimpleChannelInboundHandler` - 便捷类，自动释放消息

### 资源管理
- 所有 `ByteBuf` 和 `ReferenceCounted` 对象必须被释放
- 使用 `ResourceLeakDetector` 检测泄漏（默认启用）
- `Recycler` - 对象池，减少 GC 压力

### 原生组件
- 原生传输（epoll、kqueue、io_uring）在特定平台上提供更好的性能
- 原生传输模块编译 JNI 代码 - 需要特定平台的构建工具
- 参见 wiki 中的 [native-transports](https://netty.io/wiki/native-transports.html)

## 模块依赖关系

模块只能依赖同一层或更低层的模块：
- common（无依赖）
- buffer → common
- resolver → common
- transport → common, buffer, resolver
- codec → transport, buffer, common
- handler → transport, buffer, resolver, codec

## 测试

测试位于 `src/test/java` 中，与源代码并列。测试资源在 `src/test/resources`。

测试框架：
- JUnit 5 (junit-jupiter)
- AssertJ 断言库
- Mockito 模拟框架

## 重要说明

- 构建需要 Java 8+
- 项目使用多版本 JAR 支持 Java 9+ 模块系统
- OSGi 清单自动生成
- 强制执行 Checkstyle - 见 `io/netty/checkstyle.xml`
- 禁止 API 检查防止使用不允许的 API
- 内部 API 标记有 `@UnstableApi` - 这些可能会在没有通知的情况下更改