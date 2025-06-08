package io.netty.gateway.route.connection;

import io.netty.gateway.route.ServiceInstance;

import java.util.concurrent.CompletableFuture;

/**
 * <p>
 * 连接管理
 * </p>
 *
 * @author 伍磊
 */
public interface ConnectionManager {

    /**
     * 获取或创建到指定服务实例的连接
     *
     * @param instance 服务实例
     * @return 连接的Future
     */
    CompletableFuture<Connection> getConnection(ServiceInstance instance);

    /**
     * 释放连接
     *
     * @param connection 要释放的连接
     */
    void releaseConnection(Connection connection);

    /**
     * 移除并关闭连接
     *
     * @param connection 要关闭的连接
     */
    void removeConnection(Connection connection);

    /**
     * 关闭所有连接并释放资源
     */
    void close();

    /**
     * 获取连接超时时间
     * @return 超时时间（毫秒）
     */
    default int getConnectTimeoutMillis() {
        return ConnectionConfig.getInstance().getConnectTimeoutMillis();
    }

}
