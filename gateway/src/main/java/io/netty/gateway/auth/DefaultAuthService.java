package io.netty.gateway.auth;

import io.netty.gateway.protocol.GatewayMessage;

/**
 * <p>
 * AuthService
 * </p>
 *
 * @author 伍磊
 */
public class DefaultAuthService implements AuthService {

    public static final String TOKEN_NAME = "x-token";
    public static final String TOKEN_VALUE = "013dc334-9e05-43ee-b05a-c3e1c7d59407";

    @Override
    public boolean authenticate(GatewayMessage msg) {
        byte msgType = msg.getMsgType();
        if (GatewayMessage.MESSAGE_TYPE_AUTH != msgType) {
            return false;
        }
        // TODO 固定从 extensions 中拿到 token
        String token = msg.getExtensions().get(TOKEN_NAME);
        if (token == null) {
            return false;
        }
        if (!TOKEN_VALUE.equals(token)) {
            return false;
        }
        return true;
    }
}
