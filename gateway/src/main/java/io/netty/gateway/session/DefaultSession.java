package io.netty.gateway.session;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Session默认实现
 */
public class DefaultSession implements Session {
    private static final AttributeKey<Session> SESSION_KEY = AttributeKey.valueOf("session");
    
    private final String id;
    private final String clientId;
    private final Channel channel;
    private final long createTime;
    private volatile long lastActiveTime;
    private volatile boolean authenticated;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public DefaultSession(String id, String clientId, Channel channel) {
        this.id = id;
        this.clientId = clientId;
        this.channel = channel;
        this.createTime = System.currentTimeMillis();
        this.lastActiveTime = this.createTime;
        channel.attr(SESSION_KEY).set(this);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public long getCreateTime() {
        return createTime;
    }

    @Override
    public long getLastActiveTime() {
        return lastActiveTime;
    }

    @Override
    public void updateLastActiveTime() {
        this.lastActiveTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    @Override
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
        return (T) attributes.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.attr(SESSION_KEY).set(null);
            channel.close();
        }
    }

    @Override
    public boolean isValid() {
        return channel != null && channel.isActive();
    }

    /**
     * 从Channel中获取关联的Session
     */
    public static Session getSession(Channel channel) {
        if (channel == null) {
            return null;
        }
        return channel.attr(SESSION_KEY).get();
    }
} 