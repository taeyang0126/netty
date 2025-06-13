package io.netty.gateway.route.connection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.gateway.route.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(DefaultConnection.class);

    public static final int MAX_RETRY_TIMES = 3;
    public static final int MAX_TOTAL_RETRY_TIMES = 5;
    private static final long RETRY_INTERVAL_SECONDS = 1;

    private final ServiceInstance serviceInstance;
    private volatile Channel channel;
    private final Map<Long, CompletableFuture<GatewayMessage>> pendingMessages;
    // 针对一个连接的重试次数
    private final AtomicInteger retryCount;
    // 针对这个 Connection 的整体的重试次数
    private final AtomicInteger totalRetryCount;
    private final Bootstrap bootstrap;

    // 关键：用于区分是主动关闭还是被动断开的标志位
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    // 这个标志位用于确保任何时候只有一个重连流程正在进行。
    private final AtomicBoolean isReconnecting = new AtomicBoolean(false);

    public DefaultConnection(Bootstrap bootstrap, Channel channel, ServiceInstance serviceInstance) {
        this.bootstrap = bootstrap;
        this.channel = channel;
        this.serviceInstance = serviceInstance;
        this.pendingMessages = new ConcurrentHashMap<>();
        this.retryCount = new AtomicInteger(0);
        this.totalRetryCount = new AtomicInteger(0);

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

        channel.writeAndFlush(message).addListener(future -> {
            if (future.isSuccess()) {
                pendingMessages.put(message.getRequestId(), completableFuture);
            } else {
                completableFuture.completeExceptionally(future.cause());
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
        // 处理连接关闭
        channel.closeFuture().addListener(future -> {
            // 非手动关闭再重连
            if (!isShuttingDown.get()) {
                // 只有当 isReconnecting 标志位从 false 成功变为 true 时，才执行重连。
                // 这确保了只有一个线程能够启动重连流程。
                if (isReconnecting.compareAndSet(false, true)) {
                    logger.info("[{}]Connection lost. Acquiring reconnect lock and starting reconnection process for {}", this.hashCode(), serviceInstance);
                    scheduleReconnect();
                } else {
                    logger.info("Connection lost, but another reconnection process is already running for {}", serviceInstance);
                }
            }
        });
    }

    private void scheduleReconnect() {
        int currentRetry = retryCount.get();
        int totalRetry = totalRetryCount.get();
        if (currentRetry >= MAX_RETRY_TIMES || totalRetry >= MAX_TOTAL_RETRY_TIMES) {
            // 重连失败，清理资源
            logger.warn("Max retry times (currentRetry={}, totalRetry={}) reached for {}", currentRetry, totalRetry, serviceInstance);
            clearResource();
            // 放弃重连时，释放锁
            isReconnecting.set(false);
            return;
        }

        currentRetry = retryCount.incrementAndGet();
        totalRetry = totalRetryCount.incrementAndGet();

        logger.info("Scheduling reconnection for {}, attempt {}/{}, totalAttempt {}/{}",
                serviceInstance, currentRetry, MAX_RETRY_TIMES, totalRetry, MAX_TOTAL_RETRY_TIMES);

        channel.eventLoop().schedule(() -> {
            bootstrap.connect(serviceInstance.getHost(), serviceInstance.getPort())
                    .addListener((ChannelFutureListener) future -> {
                        if (future.isSuccess()) {
                            logger.info("Successfully reconnected to {}", serviceInstance);
                            channel = future.channel();
                            setupChannel();
                            retryCount.set(0);
                            isReconnecting.set(false);
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

    public AtomicInteger getRetryCount() {
        return retryCount;
    }

    public AtomicInteger getTotalRetryCount() {
        return totalRetryCount;
    }
}
