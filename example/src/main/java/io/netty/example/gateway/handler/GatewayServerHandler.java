package io.netty.example.gateway.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.example.gateway.protocol.GatewayMessage;
import io.netty.example.gateway.session.SessionManager;
import io.netty.example.gateway.route.RouteManager;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;

public class GatewayServerHandler extends ChannelInboundHandlerAdapter {
    private final SessionManager sessionManager = SessionManager.getInstance();
    private final RouteManager routeManager = RouteManager.getInstance();
    private boolean authenticated = false;
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof GatewayMessage) {
                GatewayMessage message = (GatewayMessage) msg;
                
                if (!authenticated && message.getMsgType() != GatewayMessage.MESSAGE_TYPE_AUTH) {
                    // 未认证且不是认证消息，断开连接
                    sendError(ctx, "Not authenticated");
                    ctx.close();
                    return;
                }
                
                switch (message.getMsgType()) {
                    case GatewayMessage.MESSAGE_TYPE_AUTH:
                        handleAuth(ctx, message);
                        break;
                    case GatewayMessage.MESSAGE_TYPE_HEARTBEAT:
                        handleHeartbeat(ctx, message);
                        break;
                    case GatewayMessage.MESSAGE_TYPE_BIZ:
                        handleBizMessage(ctx, message);
                        break;
                    default:
                        sendError(ctx, "Unknown message type");
                }
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }
    
    private void handleAuth(ChannelHandlerContext ctx, GatewayMessage message) {
        String clientId = message.getClientId();
        if (clientId == null) {
            sendError(ctx, "Missing clientId");
            ctx.close();
            return;
        }
        
        // 注册会话
        sessionManager.register(clientId, ctx.channel());
        authenticated = true;
        
        // 发送认证成功响应
        GatewayMessage response = new GatewayMessage();
        response.setMsgType(GatewayMessage.MESSAGE_TYPE_AUTH_RESP);
        response.setRequestId(message.getRequestId());
        response.getExtensions().put("status", "success");
        ctx.writeAndFlush(response);
    }
    
    private void handleHeartbeat(ChannelHandlerContext ctx, GatewayMessage message) {
        // 回复心跳
        GatewayMessage response = new GatewayMessage();
        response.setMsgType(GatewayMessage.MESSAGE_TYPE_HEARTBEAT);
        response.setRequestId(message.getRequestId());
        ctx.writeAndFlush(response);
    }
    
    private void handleBizMessage(ChannelHandlerContext ctx, GatewayMessage message) {
        String bizType = message.getBizType();
        if (bizType == null) {
            sendError(ctx, "Missing bizType");
            return;
        }
        
        // 尝试路由到对应的业务处理器
        if (!routeManager.handleMessage(bizType, message)) {
            sendError(ctx, "No handler found for bizType: " + bizType);
        }
    }
    
    private void sendError(ChannelHandlerContext ctx, String errorMessage) {
        GatewayMessage response = new GatewayMessage();
        response.setMsgType(GatewayMessage.MESSAGE_TYPE_ERROR);
        response.setBody(errorMessage.getBytes());
        ctx.writeAndFlush(response);
    }
    
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state() == IdleState.READER_IDLE) {
                // 读超时，关闭连接
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 连接断开时，清理会话
        sessionManager.unregister(ctx.channel());
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
} 