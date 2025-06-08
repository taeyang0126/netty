package io.netty.gateway.route.connection;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;

/**
 * HTTP 连接处理器
 * 负责 GatewayMessage 和 HTTP 消息的转换
 */
public class HttpConnectionHandler extends ChannelDuplexHandler {

    private final Connection connection;
    private final ArrayDeque<GatewayMessage> requests;

    public HttpConnectionHandler(Connection connection) {
        this.connection = connection;
        this.requests = new ArrayDeque<>();
    }


    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof GatewayMessage) {
            GatewayMessage gatewayMessage = (GatewayMessage) msg;
            // 转换为 HTTP 请求并发送
            ctx.write(HttpProtocolConverter.toHttpRequest(gatewayMessage), promise)
                    .addListener(future -> {
                        if (future.isSuccess()) {
                            requests.add(gatewayMessage);
                        }
                    });
        } else {
            ctx.write(msg, promise);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
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
    }

} 