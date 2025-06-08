package io.netty.gateway.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.gateway.auth.AuthService;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.gateway.session.Session;
import io.netty.gateway.session.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * 授权 handler
 * </p>
 *
 * @author 伍磊
 */
@ChannelHandler.Sharable
public class AuthHandler extends SimpleChannelInboundHandler<GatewayMessage> {

    private static final Logger logger = LoggerFactory.getLogger(GatewayServerHandler.class);

    private final AuthService authService;
    private final SessionManager sessionManager;

    public AuthHandler(AuthService authService, SessionManager sessionManager) {
        this.authService = authService;
        this.sessionManager = sessionManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, GatewayMessage msg) throws Exception {
        boolean authenticate = authService.authenticate(msg);
        if (!authenticate) {
            logger.info("channel authenticate failed, clientId={}", msg.getClientId());
            GatewayMessage response = new GatewayMessage();
            response.setMsgType(GatewayMessage.MESSAGE_TYPE_AUTH_FAIL_RESP);
            response.setRequestId(msg.getRequestId());
            response.setClientId(msg.getClientId());
            ctx.writeAndFlush(response);
        } else {
            // 验证成功
            ctx.pipeline().remove(this);

            // 构建 session
            Session session = sessionManager.createSession(msg.getClientId(), ctx.channel());
            session.setAuthenticated(true);
            logger.info("New session created: sessionId={}, clientId={}", session.getId(), msg.getClientId());

            // response
            GatewayMessage response = new GatewayMessage();
            response.setMsgType(GatewayMessage.MESSAGE_TYPE_AUTH_SUCCESS_RESP);
            response.setRequestId(msg.getRequestId());
            response.setClientId(msg.getClientId());
            ctx.writeAndFlush(response);
        }
    }
}
