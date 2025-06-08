package io.netty.gateway.route;

import io.netty.gateway.protocol.GatewayMessage;
import io.netty.gateway.route.connection.Connection;
import io.netty.gateway.route.connection.ConnectionManager;
import io.netty.gateway.route.loadbalancer.LoadBalancer;
import io.netty.util.internal.StringUtil;

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

    private final ServiceRegistry registry;
    private final LoadBalancer loadBalancer;
    private final ConnectionManager connectionManager;

    private static final String ERROR_BIZ_TYPE_REQUIRED = "bizType is required";
    private static final String ERROR_SERVICE_NOT_FOUND = "service %s not found";

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
            future.completeExceptionally(new IllegalArgumentException(ERROR_BIZ_TYPE_REQUIRED));
            return future;
        }

        // 1. 找到对应的服务
        List<ServiceInstance> services = registry.getServices(bizType);
        if (null == services || services.isEmpty()) {
            future.completeExceptionally(new IllegalArgumentException(String.format(ERROR_SERVICE_NOT_FOUND, bizType)));
            return future;
        }

        ServiceInstance instance = loadBalancer.select(services);
        if (instance == null) {
            future.completeExceptionally(new IllegalArgumentException(String.format(ERROR_SERVICE_NOT_FOUND, bizType)));
            return future;
        }

        // 2. 发送请求
        CompletableFuture<Connection> connection = connectionManager.getConnection(instance);
        connection
                .whenComplete((conn, throwable) -> {
                    if (throwable != null) {
                        future.completeExceptionally(throwable);
                    } else {
                        conn.send(message)
                                .whenComplete((resp, err) -> {
                                    if (err != null) {
                                        future.completeExceptionally(err);
                                    } else {
                                        future.complete(resp);
                                    }
                                });
                    }
                });

        return future;
    }


}
