package io.netty.example.gateway.session;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    private static final SessionManager INSTANCE = new SessionManager();
    
    public static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("clientId");
    
    // clientId -> Channel 映射
    private final Map<String, Channel> clientChannels = new ConcurrentHashMap<>();
    
    private SessionManager() {}
    
    public static SessionManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * 注册客户端会话
     * @param clientId 客户端ID
     * @param channel 客户端Channel
     */
    public void register(String clientId, Channel channel) {
        // 保存 clientId -> channel 映射
        Channel oldChannel = clientChannels.put(clientId, channel);
        if (oldChannel != null && oldChannel != channel) {
            // 如果存在旧连接，关闭它
            oldChannel.close();
        }
        
        // 设置 Channel 属性
        channel.attr(CLIENT_ID).set(clientId);
    }
    
    /**
     * 注销客户端会话
     * @param channel 客户端Channel
     */
    public void unregister(Channel channel) {
        String clientId = channel.attr(CLIENT_ID).get();
        if (clientId != null) {
            clientChannels.remove(clientId, channel);
        }
    }
    
    /**
     * 获取客户端Channel
     * @param clientId 客户端ID
     * @return 对应的Channel，如果不存在返回null
     */
    public Channel getChannel(String clientId) {
        return clientChannels.get(clientId);
    }
    
    /**
     * 判断客户端是否在线
     * @param clientId 客户端ID
     * @return 是否在线
     */
    public boolean isOnline(String clientId) {
        Channel channel = clientChannels.get(clientId);
        return channel != null && channel.isActive();
    }
    
    /**
     * 获取当前在线客户端数量
     * @return 在线数量
     */
    public int getOnlineCount() {
        return clientChannels.size();
    }
} 