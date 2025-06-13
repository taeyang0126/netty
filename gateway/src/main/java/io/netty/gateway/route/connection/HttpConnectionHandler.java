package io.netty.gateway.route.connection;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;

/**
 * HTTP 连接处理器
 * 负责 GatewayMessage 和 HTTP 消息的转换
 */
public class HttpConnectionHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(HttpConnectionHandler.class);

    private final Connection connection;
    private final ArrayDeque<GatewayMessage> requests;
    private final ExecutorService bizExecutor;

    public HttpConnectionHandler(Connection connection, ExecutorService bizExecutor) {
        this.connection = connection;
        this.requests = new ArrayDeque<>();
        this.bizExecutor = bizExecutor;
    }


    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        if (!(msg instanceof GatewayMessage)) {
            logger.debug("[{}] Forwarding non-GatewayMessage directly.", ctx.channel().id());
            ctx.write(msg, promise);
            return;
        }

        bizExecutor.execute(() -> {
            GatewayMessage gatewayMessage = (GatewayMessage) msg;
            FullHttpRequest httpRequest = HttpProtocolConverter.toHttpRequest(gatewayMessage);
            // 转换为 HTTP 请求并发送
            ctx.writeAndFlush(httpRequest, promise)
                    .addListener(future -> {
                        if (future.isSuccess()) {
                            requests.add(gatewayMessage);
                        }
                    });
        });

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        bizExecutor.execute(() -> {
            try {
                if (msg instanceof FullHttpResponse) {
                    FullHttpResponse response = (FullHttpResponse) msg;
                    // 获取原始请求
                    GatewayMessage request = requests.poll();
                    if (request != null) {
                        // 转换为 GatewayMessage 并处理
                        GatewayMessage gatewayMessage = HttpProtocolConverter.toGatewayMessage(response, request);
                        connection.handleResponse(gatewayMessage);
                    }
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        });
    }

} 