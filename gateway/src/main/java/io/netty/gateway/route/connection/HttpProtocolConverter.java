package io.netty.gateway.route.connection;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.util.Map;

/**
 * HTTP 协议转换器
 */
public class HttpProtocolConverter {

    private static final String HEADER_REQUEST_ID = "Request-Id";
    private static final String HEADER_CLIENT_ID = "X-Client-Id";

    /**
     * 将 GatewayMessage 转换为 HTTP 请求
     */
    public static FullHttpRequest toHttpRequest(GatewayMessage message) {
        // 构建请求体
        ByteBuf content = message.getBody() != null ?
                Unpooled.wrappedBuffer(message.getBody()) :
                Unpooled.EMPTY_BUFFER;

        // 创建 HTTP 请求，直接使用 bizType 作为路径
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                convertToPath(message.getBizType()),
                content
        );

        // 设置 headers
        HttpHeaders headers = request.headers();
        headers.set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        headers.set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        // 设置自定义 headers
        headers.set(HEADER_REQUEST_ID, message.getRequestId());
        headers.set(HEADER_CLIENT_ID, message.getClientId());

        // 添加扩展字段到 headers
        if (message.getExtensions() != null) {
            for (Map.Entry<String, String> entry : message.getExtensions().entrySet()) {
                headers.set(entry.getKey(), entry.getValue());
            }
        }

        return request;
    }

    /**
     * 将 HTTP 响应转换为 GatewayMessage
     */
    public static GatewayMessage toGatewayMessage(FullHttpResponse response, GatewayMessage request) {
        GatewayMessage message = new GatewayMessage();

        // 复制请求中的关键字段
        message.setRequestId(request.getRequestId());
        message.setClientId(request.getClientId());
        message.setBizType(request.getBizType());

        // 设置消息类型为业务响应
        message.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);

        // 设置响应体
        ByteBuf content = response.content();
        if (content.isReadable()) {
            byte[] bytes = new byte[content.readableBytes()];
            content.readBytes(bytes);
            message.setBody(bytes);
        }

        // 将 HTTP headers 转换为扩展字段
        HttpHeaders headers = response.headers();
        for (Map.Entry<String, String> header : headers) {
            message.getExtensions().put(header.getKey(), header.getValue());
        }

        // 添加响应状态码
        message.getExtensions().put("http_status", String.valueOf(response.status().code()));

        return message;
    }

    public static String convertToPath(String bizType) {
        if (bizType == null) {
            return null;
        }
        // 1. 将所有的 '.' 替换为 '/'
        String slashSeparated = bizType.replace('.', '/');
        // 2. 在字符串开头添加 '/'
        return "/" + slashSeparated;
    }
} 