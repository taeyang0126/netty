package io.netty.gateway.route;

import io.netty.gateway.protocol.GatewayMessage;
import io.netty.gateway.route.connection.Connection;
import io.netty.gateway.route.connection.ConnectionManager;
import io.netty.gateway.route.loadbalancer.LoadBalancer;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * <p>
 * 默认的路由服务
 * </p>
 *
 * @author 伍磊
 */
public class DefaultRouteService implements RouteService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRouteService.class);

    private final ServiceRegistry registry;
    private final LoadBalancer loadBalancer;
    private final ConnectionManager connectionManager;

    public static final String ERROR_BIZ_TYPE_REQUIRED = "bizType is required";
    public static final String ERROR_SERVICE_NOT_FOUND = "service %s not found";

    public DefaultRouteService(ServiceRegistry registry, LoadBalancer loadBalancer, ConnectionManager connectionManager) {
        this.registry = registry;
        this.loadBalancer = loadBalancer;
        this.connectionManager = connectionManager;
    }

    @Override
    public CompletableFuture<GatewayMessage> route(GatewayMessage message) {
        CompletableFuture<GatewayMessage> future = new CompletableFuture<>();
        String bizType = message.getBizType();

        if (StringUtil.isNullOrEmpty(bizType)) {
            logger.error("Business type is missing for message: {}", message.getRequestId());
            future.completeExceptionally(new IllegalArgumentException(ERROR_BIZ_TYPE_REQUIRED));
            return future;
        }

        // 1. 找到对应的服务
        List<ServiceInstance> services = registry.getServices(bizType);
        if (null == services || services.isEmpty()) {
            logger.error("No service found for bizType: {}", bizType);
            future.completeExceptionally(new IllegalArgumentException(String.format(ERROR_SERVICE_NOT_FOUND, bizType)));
            return future;
        }

        ServiceInstance instance = loadBalancer.select(services);
        if (instance == null) {
            logger.error("Load balancer returned null instance for bizType: {}", bizType);
            future.completeExceptionally(new IllegalArgumentException(String.format(ERROR_SERVICE_NOT_FOUND, bizType)));
            return future;
        }

        logger.debug("Selected service instance: {} for bizType: {}", instance, bizType);

        // 2. 发送请求
        CompletableFuture<Connection> connection = connectionManager.getConnection(instance);
        connection
                .whenComplete((conn, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to get connection for instance: " + instance, throwable);
                        future.completeExceptionally(throwable);
                    } else {
                        logger.debug("Sending message to instance: {}, requestId: {}",
                                instance, message.getRequestId());
                        conn.send(message)
                                .whenComplete((resp, err) -> {
                                    if (err != null) {
                                        logger.error("Failed to send message: " + message.getRequestId(), err);
                                        future.completeExceptionally(err);
                                    } else {
                                        logger.debug("Received response for requestId: {}", message.getRequestId());
                                        future.complete(resp);
                                    }
                                });
                    }
                });

        return future;
    }

    @Override
    public ServiceRegistry getServiceRegistry() {
        return this.registry;
    }

    @Override
    public ConnectionManager getConnectionManager() {
        return this.connectionManager;
    }
}
