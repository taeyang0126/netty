package io.netty.gateway.route;

import io.netty.gateway.protocol.GatewayMessage;

import java.util.concurrent.CompletableFuture;

/**
 * <p>
 * 路由服务
 * </p>
 *
 * @author 伍磊
 */
public interface RouteService {

    /**
     * 路由并发送消息
     *
     * @param message 网关消息
     * @return 响应消息的Future
     */
    CompletableFuture<GatewayMessage> route(GatewayMessage message);

}
