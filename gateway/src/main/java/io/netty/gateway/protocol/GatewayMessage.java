package io.netty.gateway.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 网关消息协议定义
 * 
 * 消息格式（字节序：大端序）：
 * <pre>
 * +----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+
 * |   totalLength  |    checksum    |     magic      |    version    |    msgType    |   requestId   |    clientId    |    bizType     |   extensions   |      body      |
 * |     (4字节)    |     (4字节)    |     (2字节)    |    (1字节)    |    (1字节)    |    (8字节)    |    (变长)      |     (变长)     |     (变长)     |     (变长)     |
 * |     int32     |     int32      |     int16      |     int8      |     int8      |    int64      |    string      |    string      |      map       |     bytes      |
 * | 消息总长度     |   校验和CRC32   |   0xCAFE      |    0x01       | 0x01-认证请求  |   请求ID      | length(2字节)  | length(2字节)   | length(2字节)   | length(4字节)   |
 * | (不含自身4字节) |                |                |               | 0x02-认证响应  |              | content(UTF8)  | content(UTF8)   | count(2字节)    | content(bytes)  |
 * |                |                |                |               | 0x03-心跳      |              |                |                | entries:        |                |
 * |                |                |                |               | 0x04-业务消息  |              |                |                | - key_len(2)    |                |
 * |                |                |                |               | 0xFF-错误消息  |              |                |                | - key(UTF8)     |                |
 * |                |                |                |               |               |              |                |                | - value_len(2)  |                |
 * |                |                |                |               |               |              |                |                | - value(UTF8)   |                |
 * +----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+
 * </pre>
 * 
 * 注意事项：
 * 1. 所有字符串采用UTF-8编码
 * 2. 校验和通过CRC32计算，计算范围为校验和之后的所有字节
 * 3. 变长字符串和字节数组的长度字段为0时，表示内容为空
 * 4. 扩展字段为可选字段，length为0时表示没有扩展字段
 */
public class GatewayMessage {
    // 协议常量
    public static final int HEADER_LENGTH = 8;             // 总长度(4) + 校验和(4)
    public static final short MESSAGE_MAGIC = (short) 0xCAFE;
    public static final byte MESSAGE_VERSION = 0x01;

    // 消息类型定义
    public static final byte MESSAGE_TYPE_AUTH = (byte) 0x01;      // 认证消息
    public static final byte MESSAGE_TYPE_AUTH_SUCCESS_RESP = (byte) 0x02; // 认证成功
    public static final byte MESSAGE_TYPE_AUTH_FAIL_RESP = (byte) 0x03; // 认证失败
    public static final byte MESSAGE_TYPE_HEARTBEAT = (byte) 0x04; // 心跳消息
    public static final byte MESSAGE_TYPE_BIZ = (byte) 0x05;       // 业务消息
    public static final byte MESSAGE_TYPE_ERROR = (byte) 0xFF;     // 错误消息

    // 消息头
    private short magic = MESSAGE_MAGIC;
    private byte version = MESSAGE_VERSION;
    private byte msgType;

    private long requestId;      // 请求ID
    private String clientId;     // 客户端标识
    private String bizType;      // 业务类型标识

    // 消息体
    private Map<String, String> extensions = new HashMap<>();
    private byte[] body;

    public static GatewayMessage decode(ByteBuf in) {
        // 1. 确保有足够的字节可读
        if (in.readableBytes() < HEADER_LENGTH) {
            throw new IllegalArgumentException("Insufficient bytes for message header");
        }

        // 2. 读取整体长度
        int totalLength = in.readInt();
        if (in.readableBytes() < totalLength - 4) {
            throw new IllegalArgumentException("Insufficient bytes for message body");
        }

        // 3. 验证校验和
        int checksum = in.readInt();
        int readerIndex = in.readerIndex();
        // 减去校验和的长度
        byte[] bytes = new byte[totalLength - 4];
        in.getBytes(readerIndex, bytes);
        if (checksum != calculateChecksum(bytes)) {
            throw new IllegalArgumentException("Invalid checksum");
        }

        // 4. 读取消息内容
        GatewayMessage message = new GatewayMessage();
        message.magic = in.readShort();
        if (message.magic != MESSAGE_MAGIC) {
            throw new IllegalArgumentException("Invalid magic number");
        }

        message.version = in.readByte();
        message.msgType = in.readByte();
        message.requestId = in.readLong();

        // 读取clientId
        short clientIdLength = in.readShort();
        if (clientIdLength > 0) {
            byte[] clientIdBytes = new byte[clientIdLength];
            in.readBytes(clientIdBytes);
            message.clientId = new String(clientIdBytes, CharsetUtil.UTF_8);
        }

        // 读取业务类型
        short bizTypeLength = in.readShort();
        if (bizTypeLength > 0) {
            byte[] bizTypeBytes = new byte[bizTypeLength];
            in.readBytes(bizTypeBytes);
            message.bizType = new String(bizTypeBytes, CharsetUtil.UTF_8);
        }

        // 读取扩展字段
        short extensionsLength = in.readShort();
        if (extensionsLength > 0) {
            short count = in.readShort();
            for (int i = 0; i < count; i++) {
                short keyLen = in.readShort();
                byte[] keyBytes = new byte[keyLen];
                in.readBytes(keyBytes);
                String key = new String(keyBytes, CharsetUtil.UTF_8);

                short valueLen = in.readShort();
                byte[] valueBytes = new byte[valueLen];
                in.readBytes(valueBytes);
                String value = new String(valueBytes, CharsetUtil.UTF_8);

                message.extensions.put(key, value);
            }
        }

        // 读取消息体
        int bodyLength = in.readInt();
        if (bodyLength > 0) {
            message.body = new byte[bodyLength];
            in.readBytes(message.body);
        }

        return message;
    }

    public void encode(ByteBuf out) {
        // 1. 写入长度占位符
        int lengthIndex = out.writerIndex();
        out.writeInt(0);

        // 2. 写入校验和占位符
        int checksumIndex = out.writerIndex();
        out.writeInt(0);

        // 3. 写入消息内容
        int contentStartIndex = out.writerIndex();
        out.writeShort(magic);
        out.writeByte(version);
        out.writeByte(msgType);
        out.writeLong(requestId);

        // 写入clientId
        byte[] clientIdBytes = clientId != null ? clientId.getBytes(CharsetUtil.UTF_8) : new byte[0];
        out.writeShort(clientIdBytes.length);
        if (clientIdBytes.length > 0) {
            out.writeBytes(clientIdBytes);
        }

        // 写入业务类型
        byte[] bizTypeBytes = bizType != null ? bizType.getBytes(CharsetUtil.UTF_8) : new byte[0];
        out.writeShort(bizTypeBytes.length);
        if (bizTypeBytes.length > 0) {
            out.writeBytes(bizTypeBytes);
        }

        // 写入扩展字段
        int extensionsBytes = calculateExtensionsLength();
        out.writeShort(extensionsBytes);
        if (extensionsBytes > 0) {
            out.writeShort(extensions.size());
            for (Map.Entry<String, String> entry : extensions.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(CharsetUtil.UTF_8);
                byte[] valueBytes = entry.getValue().getBytes(CharsetUtil.UTF_8);
                out.writeShort(keyBytes.length);
                out.writeBytes(keyBytes);
                out.writeShort(valueBytes.length);
                out.writeBytes(valueBytes);
            }
        }

        // 写入消息体
        if (body != null) {
            out.writeInt(body.length);
            out.writeBytes(body);
        } else {
            out.writeInt(0);
        }

        // 4. 计算总长度和校验和
        int totalLength = out.writerIndex() - contentStartIndex;
        byte[] messageBytes = new byte[totalLength];
        out.getBytes(contentStartIndex, messageBytes);

        out.setInt(lengthIndex, totalLength + 4);
        out.setInt(checksumIndex, calculateChecksum(messageBytes));
    }

    public static int calculateChecksum(byte[] bytes) {
        CRC32 crc32 = new CRC32();
        crc32.update(bytes, 0, bytes.length);
        return (int) crc32.getValue();
    }

    private int calculateExtensionsLength() {
        if (extensions.isEmpty()) {
            return 0;
        }
        int length = 2; // size of map (short)
        for (Map.Entry<String, String> entry : extensions.entrySet()) {
            length += 2 + entry.getKey().getBytes(CharsetUtil.UTF_8).length;
            length += 2 + entry.getValue().getBytes(CharsetUtil.UTF_8).length;
        }
        return length;
    }

    // Getters and setters
    public byte getMsgType() {
        return msgType;
    }

    public void setMsgType(byte msgType) {
        this.msgType = msgType;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getBizType() {
        return bizType;
    }

    public void setBizType(String bizType) {
        this.bizType = bizType;
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public Map<String, String> getExtensions() {
        return extensions;
    }

    public void setExtensions(Map<String, String> extensions) {
        this.extensions = extensions;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "GatewayMessage{" +
                "magic=" + magic +
                ", version=" + version +
                ", msgType=" + msgType +
                ", requestId=" + requestId +
                ", clientId='" + clientId + '\'' +
                ", bizType='" + bizType + '\'' +
                ", extensions=" + extensions +
                ", body=" + (body == null ? "" : new String(body)) +
                '}';
    }
}
