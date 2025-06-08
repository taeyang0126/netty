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
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 默认的连接管理器
 * </p>
 *
 * @author 伍磊
 */
public class DefaultConnectionManager implements ConnectionManager {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultConnectionManager.class);

    private final Map<ServiceInstance, Connection> connections;
    private final EventLoopGroup workerGroup;
    private final Bootstrap bootstrap;
    private volatile boolean closed;

    public DefaultConnectionManager() {
        this.connections = new ConcurrentHashMap<>();
        this.workerGroup = new MultiThreadIoEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2,
                NioIoHandler.newFactory());
        this.bootstrap = new Bootstrap();
        this.closed = false;

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
                        pipeline.addLast(new IdleStateHandler(0, 20, 0, TimeUnit.SECONDS));
                        // TODO: 添加其他处理器
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
        CompletableFuture<Connection> future = new CompletableFuture<>();

        bootstrap.connect(instance.getHost(), instance.getPort())
                .addListener((ChannelFutureListener) f -> {

                    Connection oldConnection = connections.get(instance);
                    if (oldConnection != null && oldConnection.isActive()) {
                        future.complete(oldConnection);
                        return;
                    }

                    if (f.isCancelled()) {
                        future.completeExceptionally(new RuntimeException("Connection cancelled"));
                        return;
                    }

                    if (f.isSuccess()) {
                        Channel channel = f.channel();
                        Connection connection = new DefaultConnection(bootstrap, channel, instance);
                        connections.put(instance, connection);
                        future.complete(connection);
                    } else {
                        future.completeExceptionally(f.cause());
                    }
                });

        return future;
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
            closed = true;
            connections.values().forEach(Connection::close);
            connections.clear();
            workerGroup.shutdownGracefully();
        }
    }
}
