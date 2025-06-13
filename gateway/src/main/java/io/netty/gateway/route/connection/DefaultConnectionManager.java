package io.netty.gateway.route.connection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.gateway.route.ServiceInstance;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 默认的连接管理器
 * </p>
 *
 * @author 伍磊
 */
public class DefaultConnectionManager implements ConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionManager.class);

    private final Map<ServiceInstance, Connection> connections;
    // 一个静态的、线程安全的 Map，用于缓存正在进行的连接尝试。
    private final Map<ServiceInstance, CompletableFuture<Connection>> pendingConnections;
    private final EventLoopGroup workerGroup;
    private final Bootstrap bootstrap;
    private volatile boolean closed;
    private final ExecutorService createConnectionExecutor;
    private final ExecutorService bizExecutor;

    public DefaultConnectionManager() {
        this.connections = new ConcurrentHashMap<>();
        this.pendingConnections = new ConcurrentHashMap<>();
        this.workerGroup = new MultiThreadIoEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2,
                NioIoHandler.newFactory());
        this.bootstrap = new Bootstrap();
        this.closed = false;
        // 创建连接线程池
        this.createConnectionExecutor = new ThreadPoolExecutor(
                1,
                2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10),
                new DefaultThreadFactory("upstream-connection-create"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        this.bizExecutor = new ThreadPoolExecutor(
                4,
                4,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new DefaultThreadFactory("upstream-biz-handler"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // 初始化Bootstrap
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                // 连接超时时间
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, getConnectTimeoutMillis())
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpClientCodec());
                        pipeline.addLast(new HttpObjectAggregator(65536));
                    }
                });
    }

    @Override
    public CompletableFuture<Connection> getConnection(ServiceInstance instance) {
        if (closed) {
            CompletableFuture<Connection> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("ConnectionManager is closed"));
            return future;
        }

        Connection connection = connections.get(instance);
        if (connection != null && connection.isActive()) {
            return CompletableFuture.completedFuture(connection);
        }

        return createConnection(instance);
    }

    private CompletableFuture<Connection> createConnection(ServiceInstance instance) {
        // 使用 computeIfAbsent 实现原子性的“检查并创建”。
        // 这个方法会保证对于同一个 instance，内部的 lambda 表达式只会被执行一次。
        return pendingConnections.computeIfAbsent(instance, key -> {

            // --- 这部分代码块是线程安全的，只有第一个线程会进入 ---

            logger.info("Creating new connection for {}", key);
            CompletableFuture<Connection> future = new CompletableFuture<>();

            try {
                createConnectionExecutor.execute(() -> {
                    bootstrap.connect(key.getHost(), key.getPort())
                            .addListener((ChannelFutureListener) f -> {
                                if (f.isSuccess()) {
                                    Channel channel = f.channel();
                                    Connection connection = new DefaultConnection(bootstrap, channel, key);
                                    // 添加 HTTP 协议转换处理器
                                    channel.pipeline().addLast(new HttpConnectionHandler(connection, bizExecutor));
                                    connections.put(key, connection);
                                    future.complete(connection);
                                } else {
                                    future.completeExceptionally(f.cause());
                                }

                                // 无论成功还是失败，都要从 map 中移除。 这样，下一次调用 createConnection 时就可以发起新的连接尝试。
                                pendingConnections.remove(key, future);
                            });
                });
            } catch (RejectedExecutionException e) {
                // 处理线程池拒绝任务的极端情况(线程池已关闭)
                logger.warn("Create connection task for {} was rejected by the executor.", key, e);
                pendingConnections.remove(key, future); // 从 map 中移除，以便可以重试
                future.completeExceptionally(e);
            }

            return future;
        });
    }

    @Override
    public void releaseConnection(Connection connection) {
        if (connection != null && !connection.isActive()) {
            removeConnection(connection);
        }
    }

    @Override
    public void removeConnection(Connection connection) {
        if (connection != null) {
            connections.remove(connection.getServiceInstance());
            connection.close();
        }
    }

    @Override
    public void close() {
        if (!closed) {
            createConnectionExecutor.shutdown();
            closed = true;
            connections.values().forEach(Connection::close);
            connections.clear();
            workerGroup.shutdownGracefully();
            createConnectionExecutor.shutdown();
        }
    }
}
