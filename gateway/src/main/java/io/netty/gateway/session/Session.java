package io.netty.gateway.session;

import io.netty.channel.Channel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 网关会话接口
 * 定义会话的基本行为和属性
 */
public interface Session {
    /**
     * 获取会话ID
     */
    String getId();

    /**
     * 获取客户端ID
     */
    String getClientId();

    /**
     * 获取关联的Channel
     */
    Channel getChannel();

    /**
     * 获取会话创建时间
     */
    long getCreateTime();

    /**
     * 获取最后活跃时间
     */
    long getLastActiveTime();

    /**
     * 更新最后活跃时间
     */
    void updateLastActiveTime();

    /**
     * 是否认证
     */
    boolean isAuthenticated();

    /**
     * 设置认证状态
     */
    void setAuthenticated(boolean authenticated);

    /**
     * 获取会话属性
     */
    <T> T getAttribute(String key);

    /**
     * 设置会话属性
     */
    void setAttribute(String key, Object value);

    /**
     * 移除会话属性
     */
    void removeAttribute(String key);

    /**
     * 获取所有会话属性
     */
    Map<String, Object> getAttributes();

    /**
     * 关闭会话
     */
    void close();

    /**
     * 判断会话是否有效
     */
    boolean isValid();
} 