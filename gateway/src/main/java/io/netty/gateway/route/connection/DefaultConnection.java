package io.netty.gateway.route.connection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.gateway.route.ServiceInstance;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * 默认的连接实现
 * </p>
 *
 * @author 伍磊
 */
public class DefaultConnection implements Connection {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultConnection.class);
    private static final int MAX_RETRY_TIMES = 3;
    private static final long RETRY_INTERVAL_SECONDS = 1;

    private final ServiceInstance serviceInstance;
    private volatile Channel channel;
    private final Map<Long, CompletableFuture<GatewayMessage>> pendingMessages;
    private final AtomicInteger retryCount;
    private final Bootstrap bootstrap;
    // 关键：用于区分是主动关闭还是被动断开的标志位
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    public DefaultConnection(Bootstrap bootstrap, Channel channel, ServiceInstance serviceInstance) {
        this.bootstrap = bootstrap;
        this.channel = channel;
        this.serviceInstance = serviceInstance;
        this.pendingMessages = new ConcurrentHashMap<>();
        this.retryCount = new AtomicInteger(0);

        setupChannel();
    }

    @Override
    public CompletableFuture<GatewayMessage> send(GatewayMessage message) {
        CompletableFuture<GatewayMessage> completableFuture = new CompletableFuture<>();
        if (!isActive()) {
            completableFuture.completeExceptionally(
                new IllegalStateException("Connection is not active"));
            return completableFuture;
        }

        pendingMessages.put(message.getRequestId(), completableFuture);
        channel.writeAndFlush(message).addListener(future -> {
            if (!future.isSuccess()) {
                CompletableFuture<GatewayMessage> pending = 
                    pendingMessages.remove(message.getRequestId());
                if (pending != null) {
                    pending.completeExceptionally(future.cause());
                }
            }
        });

        return completableFuture;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public boolean isActive() {
        return channel != null && channel.isActive();
    }

    @Override
    public void close() {
        if (channel != null) {
            isShuttingDown.set(true);
            channel.close();
            clearResource();
        }
    }

    @Override
    public ServiceInstance getServiceInstance() {
        return serviceInstance;
    }

    public void handleResponse(GatewayMessage response) {
        CompletableFuture<GatewayMessage> future = pendingMessages.remove(response.getRequestId());
        if (future != null) {
            future.complete(response);
        }
    }

    private void setupChannel() {
        ChannelPipeline pipeline = channel.pipeline();

        // IdleStateHandler
        pipeline.addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                if (evt instanceof IdleStateEvent) {
                    IdleStateEvent event = (IdleStateEvent) evt;
                    if (event.state() == IdleState.WRITER_IDLE) {
                        // TODO 心跳
                    }
                }
                super.userEventTriggered(ctx, evt);
            }
        });

        // 处理连接关闭
        channel.closeFuture().addListener(future -> {
            // 非手动关闭再重连
            if (!isShuttingDown.get()) {
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        int currentRetry = retryCount.incrementAndGet();
        if (currentRetry > MAX_RETRY_TIMES) {
            // 重连失败，清理资源
            logger.warn("Max retry times ({}) reached for {}", MAX_RETRY_TIMES, serviceInstance);
            clearResource();
            return;
        }

        logger.info("Scheduling reconnection for {}, attempt {}/{}",
                serviceInstance, currentRetry, MAX_RETRY_TIMES);

        channel.eventLoop().schedule(() -> {
            if (isActive()) {
                return;
            }

            bootstrap.connect(serviceInstance.getHost(), serviceInstance.getPort())
                    .addListener((ChannelFutureListener) future -> {
                        if (future.isSuccess()) {
                            channel = future.channel();
                            setupChannel();
                            retryCount.set(0);
                            logger.info("Successfully reconnected to {}", serviceInstance);
                        } else {
                            logger.warn("Failed to reconnect to {}: {}",
                                    serviceInstance, future.cause().getMessage());
                            scheduleReconnect();
                        }
                    });
        }, RETRY_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }


    private void clearResource() {
        pendingMessages.forEach((reqId, completeFuture) ->
                completeFuture.completeExceptionally(new RuntimeException("Connection closed")));
        pendingMessages.clear();
    }
}
