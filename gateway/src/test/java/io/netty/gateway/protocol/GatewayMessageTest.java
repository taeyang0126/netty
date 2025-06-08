package io.netty.gateway.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * <p>
 * GatewayMessageTest
 * </p>
 *
 * @author 伍磊
 */
public class GatewayMessageTest {

    @Test
    public void testEncodeAndDecodeWithBasicFields() {
        // 创建测试消息
        GatewayMessage message = new GatewayMessage();
        message.setMsgType(GatewayMessage.MESSAGE_TYPE_AUTH);
        message.setRequestId(12345L);
        
        // 编码
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);
        
        // 解码
        GatewayMessage decoded = GatewayMessage.decode(buf);
        
        // 验证基本字段
        assertEquals(GatewayMessage.MESSAGE_TYPE_AUTH, decoded.getMsgType());
        assertEquals(12345L, decoded.getRequestId());
        assertNull(decoded.getClientId());
        assertNull(decoded.getBizType());
        assertTrue(decoded.getExtensions().isEmpty());
        assertNull(decoded.getBody());
        
        buf.release();
    }
    
    @Test
    public void testEncodeAndDecodeWithAllFields() {
        // 创建测试消息
        GatewayMessage message = new GatewayMessage();
        message.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        message.setRequestId(12345L);
        message.setClientId("testClient");
        message.setBizType("testBizType");
        message.getExtensions().put("key1", "value1");
        message.getExtensions().put("key2", "value2");
        message.setBody("Hello, World!".getBytes());
        
        // 编码
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);
        
        // 解码
        GatewayMessage decoded = GatewayMessage.decode(buf);
        
        // 验证所有字段
        assertEquals(GatewayMessage.MESSAGE_TYPE_BIZ, decoded.getMsgType());
        assertEquals(12345L, decoded.getRequestId());
        assertEquals("testClient", decoded.getClientId());
        assertEquals("testBizType", decoded.getBizType());
        assertEquals(2, decoded.getExtensions().size());
        assertEquals("value1", decoded.getExtensions().get("key1"));
        assertEquals("value2", decoded.getExtensions().get("key2"));
        assertArrayEquals("Hello, World!".getBytes(), decoded.getBody());
        
        buf.release();
    }
    
    @Test
    public void testInvalidMagicNumber() {
        // 创建测试消息
        GatewayMessage message = new GatewayMessage();
        message.setMsgType(GatewayMessage.MESSAGE_TYPE_AUTH);
        
        // 编码
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);
        
        // 修改魔数
        buf.setShort(8, (short) 0xDEAD);
        // 修改校验和
        int totalLength = buf.getInt(0);
        byte[] data = new byte[totalLength - 4];
        buf.getBytes(8, data);
        buf.setInt(4,  GatewayMessage.calculateChecksum(data));

        // 验证解码时抛出异常
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            GatewayMessage.decode(buf);
        });
        assertEquals("Invalid magic number", e.getMessage());
        
        buf.release();
    }
    
    @Test
    public void testInsufficientBytes() {
        // 测试消息头不足的情况
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(100); // 只写入长度
        
        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> {
            GatewayMessage.decode(buf);
        });
        assertEquals("Insufficient bytes for message header", e1.getMessage());
        
        // 测试消息体不足的情况
        buf.clear();
        buf.writeInt(1000); // 写入一个大长度
        buf.writeInt(0); // 写入校验和
        buf.writeShort(GatewayMessage.MESSAGE_MAGIC); // 写入魔数
        buf.writeByte(GatewayMessage.MESSAGE_VERSION); // 写入版本
        
        IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class, () -> {
            GatewayMessage.decode(buf);
        });
        assertEquals("Insufficient bytes for message body", e2.getMessage());
        
        buf.release();
    }
    
    @Test
    public void testChecksum() {
        // 创建测试消息
        GatewayMessage message = new GatewayMessage();
        message.setMsgType(GatewayMessage.MESSAGE_TYPE_AUTH);
        message.setClientId("testClient");
        
        // 编码
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);
        
        // 修改内容但不更新校验和
        int contentIndex = 8;
        buf.setByte(contentIndex + 5, 0xFF); // 修改某个内容字节
        
        // 验证解码时抛出异常
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            GatewayMessage.decode(buf);
        });
        assertEquals("Invalid checksum", e.getMessage());
        
        buf.release();
    }

}
