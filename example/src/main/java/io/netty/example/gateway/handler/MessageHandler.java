package io.netty.example.gateway.handler;

import io.netty.example.gateway.protocol.GatewayMessage;

@FunctionalInterface
public interface MessageHandler {
    /**
     * 处理网关消息
     * @param message 待处理的消息
     */
    void handle(GatewayMessage message);
} 