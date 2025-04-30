package io.netty.example.file;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.CharsetUtil;


/**
 * <p>
 * FileTransferDTO
 * </p>
 *
 * @author 伍磊
 */
public class FileTransferDTO {

    private String fileName;

    private int magicNumber = 0X2A8C9B10;

    private long fileSize;

    public ByteBuf write() {
        // ByteBuf
        // magicNumber(4) + 文件名称长度(4) + fileName + 内容长度(8)
        byte[] bytes = fileName.getBytes(CharsetUtil.UTF_8);
        int len = 4 + 4 + bytes.length + 8;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(len);

        buffer.writeInt(magicNumber);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        buffer.writeLong(fileSize);

        return buffer;
    }

    public static FileTransferDTO read(ByteBuf buffer) {
        buffer.markReaderIndex();

        int readableBytes = buffer.readableBytes();
        if (readableBytes < 16) {
            buffer.readableBytes();
            return null;
        }

        int magicNumber = buffer.readInt();
        if (magicNumber != 0x2A8C9B10) {
            buffer.readableBytes();
            return null;
        }

        int fileNameLength = buffer.readInt();
        if (buffer.readableBytes() < fileNameLength + 8) {
            buffer.readableBytes();
            return null;
        }

        byte[] fileName = new byte[fileNameLength];
        buffer.readBytes(fileName);
        long fileSize = buffer.readLong();

        FileTransferDTO fileTransferDTO = new FileTransferDTO();
        fileTransferDTO.setFileName(new String(fileName, CharsetUtil.UTF_8));
        fileTransferDTO.setFileSize(fileSize);
        return fileTransferDTO;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Integer getMagicNumber() {
        return magicNumber;
    }

    public void setMagicNumber(Integer magicNumber) {
        this.magicNumber = magicNumber;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }


}
