package io.netty.gateway.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.gateway.protocol.GatewayMessage;

import java.util.List;

public class GatewayMessageCodec extends ByteToMessageCodec<GatewayMessage> {
    
    @Override
    protected void encode(ChannelHandlerContext ctx, GatewayMessage msg, ByteBuf out) throws Exception {
        msg.encode(out);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 确保有足够的字节可读
        if (in.readableBytes() < GatewayMessage.HEADER_LENGTH) { // 整体长度(4) + 校验和(4)
            return;
        }
        
        // 标记当前读取位置
        in.markReaderIndex();
        
        // 读取消息总长度
        int totalLength = in.readInt();
        if (in.readableBytes() < totalLength) {
            in.resetReaderIndex();
            return;
        }
        
        // 解码消息
        in.resetReaderIndex();
        GatewayMessage message = GatewayMessage.decode(in);
        out.add(message);
    }
} 