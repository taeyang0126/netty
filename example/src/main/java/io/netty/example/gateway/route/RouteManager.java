package io.netty.example.gateway.route;

import io.netty.example.gateway.handler.MessageHandler;
import io.netty.example.gateway.protocol.GatewayMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RouteManager {
    private static final RouteManager INSTANCE = new RouteManager();
    
    // 业务处理器注册表
    private final Map<String, MessageHandler> handlers = new ConcurrentHashMap<>();
    
    private RouteManager() {}
    
    public static RouteManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * 注册消息处理器
     * @param bizType 业务类型
     * @param handler 消息处理器
     */
    public void registerHandler(String bizType, MessageHandler handler) {
        handlers.put(bizType, handler);
    }
    
    /**
     * 获取消息处理器
     * @param bizType 业务类型
     * @return 消息处理器，如果不存在返回null
     */
    public MessageHandler getHandler(String bizType) {
        return handlers.get(bizType);
    }
    
    /**
     * 处理消息
     * @param bizType 业务类型
     * @param message 待处理的消息
     * @return 是否成功处理
     */
    public boolean handleMessage(String bizType, GatewayMessage message) {
        MessageHandler handler = handlers.get(bizType);
        if (handler != null) {
            handler.handle(message);
            return true;
        }
        return false;
    }
} 