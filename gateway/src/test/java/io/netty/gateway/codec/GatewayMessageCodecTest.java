package io.netty.gateway.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.gateway.protocol.GatewayMessage;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GatewayMessageCodecTest {

    @Test
    void testEncodeAndDecode() {
        // 1. 创建一个带有自定义编解码器的 EmbeddedChannel
        EmbeddedChannel channel = new EmbeddedChannel(new GatewayMessageCodec());

        // 2. 准备要发送的消息
        GatewayMessage message = new GatewayMessage();
        message.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        message.setRequestId(System.currentTimeMillis());
        message.setClientId(UUID.randomUUID().toString());
        message.setBizType("testBizType");
        message.getExtensions().put("key", "value");
        message.setBody("Hello".getBytes());

        // 3. (编码) 将消息写入出站缓冲区
        assertTrue(channel.writeOutbound(message));

        // 3.5. 刷出缓冲区，使数据可读
        // 注意：如果您的编码器内部调用的是 ctx.writeAndFlush()，则无需手动调用此行。
        // 但在测试中显式调用 flush 是更稳妥、更清晰的做法。
        channel.flushOutbound();

        // 4. 从出站缓冲区读取编码后的数据
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);

        // 5. (解码) 将编码后的数据写入入站缓冲区
        assertTrue(channel.writeInbound(encoded));

        // 6. (验证) 从入站缓冲区读取解码后的消息
        GatewayMessage decodedMessage = channel.readInbound();
        assertNotNull(decodedMessage);

        // 验证解码后的消息内容是否与原始消息一致
        assertEquals(message.getMsgType(), decodedMessage.getMsgType());
        assertEquals(message.getRequestId(), decodedMessage.getRequestId());
        assertEquals(message.getClientId(), decodedMessage.getClientId());
        assertArrayEquals(message.getBody(), decodedMessage.getBody());
        assertEquals("value", decodedMessage.getExtensions().get("key"));

        // 7. 所有操作完成后，最后调用 finish() 来关闭通道并清理资源
        assertFalse(channel.finish());

        // 8. 确认通道中没有剩余的入站/出站数据
        assertNull(channel.readInbound());
        assertNull(channel.readOutbound());
    }

} 