package io.netty.example.file;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.AttributeKey;

import java.util.List;

/**
 * <p>
 * FileTransferDecoder
 * </p>
 *
 * @author 伍磊
 */
public class FileTransferDecoder extends MessageToMessageDecoder<ByteBuf> {

    public static final AttributeKey<FileTransferDTO> FILE_TRANSFER_DTO = AttributeKey.valueOf("fileTransferDTO");
    public static final AttributeKey<DefaultChannelPromise> FINISH_FUTURE = AttributeKey.valueOf("finishFuture");
    private FileTransferDTO fileTransferDTO;
    private long receiveSize = 0L;
    private int chunkSize = 1024 * 8;
    private DefaultChannelPromise finishFuture;
    private long startTime;
    private int receiveTimesPerFile = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {

        if (fileTransferDTO == null) {
            FileTransferDTO dto = FileTransferDTO.read(byteBuf);
            if (dto == null) {
                return;
            }
            fileTransferDTO = dto;
            finishFuture = new DefaultChannelPromise(ctx.channel());
            startTime = System.currentTimeMillis();
            receiveSize = 0;
            finishFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        System.out.printf("传输完成....耗时%d毫秒，接收次数:%d\n", (System.currentTimeMillis() - startTime), receiveTimesPerFile);
                        fileTransferDTO = null;
                    }
                }
            });
        }
        ctx.channel().attr(FILE_TRANSFER_DTO).set(fileTransferDTO);
        ctx.channel().attr(FINISH_FUTURE).set(finishFuture);

        int readableBytes = byteBuf.readableBytes();
        if (readableBytes <= 0) {
            return;
        }

        readableBytes = (int) Math.min(readableBytes, fileTransferDTO.getFileSize() - receiveSize);
        byte[] bytes = new byte[readableBytes];

        byteBuf.readBytes(bytes);
        FileChunk fileChunk = new FileChunk();
        fileChunk.setBytes(bytes);
        fileChunk.setPosition(receiveSize);
        receiveSize += bytes.length;
        fileChunk.setLastChunk(receiveSize == fileTransferDTO.getFileSize());

        // System.out.println("写入 " + receiveSize);
        receiveTimesPerFile++;

        out.add(fileChunk);
    }
}
