package io.netty.gateway.auth;

import io.netty.gateway.protocol.GatewayMessage;

/**
 * <p>
 * AuthService
 * </p>
 *
 * @author 伍磊
 */
public interface AuthService {

    /**
     * 认证
     * @param msg 消息
     * @return
     */
    boolean authenticate(GatewayMessage msg);

}
