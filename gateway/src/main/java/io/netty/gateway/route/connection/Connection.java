package io.netty.gateway.route.connection;

import io.netty.channel.Channel;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.gateway.route.ServiceInstance;

import java.util.concurrent.CompletableFuture;

/**
 * <p>
 * 与上游服务的连接
 * </p>
 *
 * @author 伍磊
 */
public interface Connection {

    /**
     * 获取底层Channel
     */
    Channel getChannel();

    /**
     * 获取连接的服务实例
     */
    ServiceInstance getServiceInstance();

    /**
     * 发送消息
     *
     * @param message 网关消息
     * @return 响应的Future
     */
    CompletableFuture<GatewayMessage> send(GatewayMessage message);

    /**
     * 连接是否活跃
     */
    boolean isActive();

    /**
     * 关闭连接
     */
    void close();

    /**
     * 处理响应消息
     *
     * @param message message
     */
    void handleResponse(GatewayMessage message);

}
