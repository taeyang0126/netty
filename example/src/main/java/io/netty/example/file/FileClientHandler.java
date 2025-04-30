package io.netty.example.file;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultChannelPromise;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

import static io.netty.example.file.FileTransferDecoder.FILE_TRANSFER_DTO;
import static io.netty.example.file.FileTransferDecoder.FINISH_FUTURE;

/**
 * <p>
 * TODO
 * </p>
 *
 * @author 伍磊
 */
public class FileClientHandler extends ChannelInboundHandlerAdapter {

    private RandomAccessFile randomAccessFile;
    private String fileName;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof FileChunk)) {
            return;
        }

        FileChunk fileChunk = (FileChunk) msg;
        FileTransferDTO fileTransferDTO = ctx.channel().attr(FILE_TRANSFER_DTO).get();
        DefaultChannelPromise channelFuture = ctx.channel().attr(FINISH_FUTURE).get();

        if (randomAccessFile == null) {
            fileName = UUID.randomUUID() + fileTransferDTO.getFileName().substring(fileTransferDTO.getFileName().lastIndexOf("."));
            randomAccessFile = new RandomAccessFile(fileName, "rw");
        }

        FileChannel channel = randomAccessFile.getChannel();
        channel.position(fileChunk.getPosition());
        channel.write(ByteBuffer.wrap(fileChunk.getBytes()));

        if (fileChunk.isLastChunk()) {
            channel.force(true);
            channelFuture.setSuccess();
            randomAccessFile.close();
            randomAccessFile = null;
            System.out.println("写入完成..." + fileName);
            fileName = null;
        }

    }

/*    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 发送文件地址
        String filepath = "/Users/wulei/Downloads/UNIX网络编程卷1：套接字联网API（第3版）.pdf";
        ctx.writeAndFlush(filepath);
        System.out.println("发送下载文件请求...");
    }*/
}
