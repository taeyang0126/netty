package io.netty.gateway.session;

import io.netty.channel.Channel;

import java.util.Collection;

/**
 * 会话管理器接口
 */
public interface SessionManager {
    /**
     * 创建会话
     */
    Session createSession(String clientId, Channel channel);

    /**
     * 根据会话ID获取会话
     */
    Session getSession(String sessionId);

    /**
     * 根据客户端ID获取会话
     */
    Session getSessionByClientId(String clientId);

    /**
     * 移除会话
     */
    void removeSession(String sessionId);

    /**
     * 获取所有会话
     */
    Collection<Session> getAllSessions();

    /**
     * 获取会话数量
     */
    int getSessionCount();

    /**
     * 判断会话是否存在
     */
    boolean existSession(String sessionId);
} 