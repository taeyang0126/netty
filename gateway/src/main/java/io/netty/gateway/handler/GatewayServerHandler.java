package io.netty.gateway.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.gateway.route.RouteService;
import io.netty.gateway.session.DefaultSession;
import io.netty.gateway.session.Session;
import io.netty.gateway.session.SessionManager;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 网关服务器消息处理器
 */
public class GatewayServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(GatewayServerHandler.class);

    private final SessionManager sessionManager;
    private final RouteService routeService;
    private final ExecutorService businessExecutor;

    public GatewayServerHandler(SessionManager sessionManager, RouteService routeService) {
        this.sessionManager = sessionManager;
        this.routeService = routeService;
        // 创建业务线程池，线程数可配置
        this.businessExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors() * 2,
                Runtime.getRuntime().availableProcessors() * 4,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                new DefaultThreadFactory("gateway-business"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof GatewayMessage)) {
            ReferenceCountUtil.release(msg);
            logger.error("Received message is not GatewayMessage: {}", msg);
            return;
        }

        GatewayMessage message = (GatewayMessage) msg;
        Session session = getSession(ctx, message);

        try {
            // 处理不同类型的消息
            switch (message.getMsgType()) {
                case GatewayMessage.MESSAGE_TYPE_HEARTBEAT:
                    // 心跳消息直接在 EventLoop 中处理，因为处理逻辑简单
                    handleHeartbeat(ctx, message, session);
                    break;
                case GatewayMessage.MESSAGE_TYPE_BIZ:
                    // 业务消息提交到业务线程池处理
                    final GatewayMessage requestMessage = message;
                    final Session currentSession = session;
                    businessExecutor.execute(() -> {
                        try {
                            handleBizMessage(ctx, requestMessage, currentSession);
                        } catch (Exception e) {
                            logger.error("Handle business message error", e);
                            handleError(ctx, requestMessage, e);
                        }
                    });
                    break;
                default:
                    logger.warn("Unknown message type: {}", message.getMsgType());
                    handleError(ctx, message, new IllegalArgumentException("Unknown message type"));
            }
        } catch (Exception e) {
            logger.error("Handle message error", e);
            handleError(ctx, message, e);
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 连接断开时清理会话
        Session session = DefaultSession.getSession(ctx.channel());
        if (session != null) {
            sessionManager.removeSession(session.getId());
            logger.info("Client disconnected, session removed: {}", session.getId());
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state() == IdleState.READER_IDLE) {
                // 读空闲，关闭连接
                Session session = DefaultSession.getSession(ctx.channel());
                if (session != null) {
                    logger.warn("Channel idle, closing session: {}", session.getId());
                    sessionManager.removeSession(session.getId());
                }
                ctx.close();
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Channel exception caught", cause);
        Session session = DefaultSession.getSession(ctx.channel());
        if (session != null) {
            sessionManager.removeSession(session.getId());
        }
        ctx.close();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        // 优雅关闭线程池
        businessExecutor.shutdown();
        try {
            if (!businessExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                businessExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            businessExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private Session getSession(ChannelHandlerContext ctx, GatewayMessage message) {
        Session session = DefaultSession.getSession(ctx.channel());
        if (session == null) {
            ctx.close();
        }
        return session;
    }

    private void handleHeartbeat(ChannelHandlerContext ctx, GatewayMessage message, Session session) {
        if (session == null) {
            logger.warn("Unknown heartbeat message received");
            ctx.close();
            return;
        }
        if (!session.isAuthenticated()) {
            logger.warn("Unauthorized heartbeat message received");
            sessionManager.removeSession(session.getId());
            return;
        }

        // 更新最后活跃时间
        session.updateLastActiveTime();

        // 响应心跳
        GatewayMessage response = new GatewayMessage();
        response.setMsgType(GatewayMessage.MESSAGE_TYPE_HEARTBEAT);
        response.setRequestId(message.getRequestId());
        response.setClientId(message.getClientId());
        ctx.writeAndFlush(response);
    }

    private void handleBizMessage(ChannelHandlerContext ctx, GatewayMessage message, Session session) {
        if (session == null) {
            logger.warn("UnKnown business message received");
            ctx.close();
            return;
        }
        if (!session.isAuthenticated()) {
            logger.warn("Unauthorized business message received");
            sessionManager.removeSession(session.getId());
            return;
        }

        // 更新最后活跃时间
        session.updateLastActiveTime();

        // 实现业务消息路由转发逻辑
        routeService.route(message)
                .whenComplete((response, ex) -> {
                    if (ex != null) {
                        logger.error("Failed to route message={}, e: ", message, ex);
                        handleError(ctx, message, ex);
                    } else {
                        ctx.writeAndFlush(response);
                    }
                });
    }

    private void handleError(ChannelHandlerContext ctx, GatewayMessage message, Throwable cause) {
        GatewayMessage response = new GatewayMessage();
        response.setMsgType(GatewayMessage.MESSAGE_TYPE_ERROR);
        response.setRequestId(message.getRequestId());
        response.setClientId(message.getClientId());
        String errorMsg = cause.getMessage();
        // TODO 异常优化下
        if (errorMsg != null) {
            response.setBody(errorMsg.getBytes(StandardCharsets.UTF_8));
        } else {
            response.setBody("system is busy".getBytes(StandardCharsets.UTF_8));
        }
        ctx.writeAndFlush(response);
    }
} 