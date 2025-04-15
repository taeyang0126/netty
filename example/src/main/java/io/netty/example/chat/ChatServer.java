package io.netty.example.chat;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.util.UUID;

/**
 * <p>
 * ChatServer
 * </p>
 *
 * @author 伍磊
 */
public class ChatServer {

    public static void main(String[] args) {
        NioEventLoopGroup boss = new NioEventLoopGroup(1);
        NioEventLoopGroup worker = new NioEventLoopGroup();

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<NioSocketChannel>() {
                        @Override
                        protected void initChannel(NioSocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new StringDecoder());
                            pipeline.addLast(new StringEncoder());

                            pipeline.addLast(new ChatRequestIdGenerator());
                            pipeline.addLast(new ChatContentHandler());
                        }
                    });

            ChannelFuture channelFuture = serverBootstrap.bind(9998).sync();
            System.out.println("Server started at " + channelFuture.channel().localAddress());
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }


    }

    @ChannelHandler.Sharable
    public static class ChatRequestIdGenerator extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (!(msg instanceof String)) {
                ctx.fireChannelRead(msg);
                return;
            }
            String content = (String) msg;
            String uuid = UUID.randomUUID().toString();
            ChatContent chatContent = new ChatContent(uuid, content);
            ctx.fireChannelRead(chatContent);
        }
    }

    @ChannelHandler.Sharable
    public static class ChatContentHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (!(msg instanceof ChatContent)) {
                ctx.fireChannelRead(msg);
                return;
            }
            ChatContent chatContent = (ChatContent) msg;
            System.out.printf("[%s]接收到数据: %s\n", chatContent.getUuid(), chatContent.getContent());
            ctx.channel().writeAndFlush("回复: " + chatContent.getUuid() + ":" + chatContent.getContent());
        }
    }

    public static class ChatContent {

        private String uuid;
        private String content;


        public ChatContent(String uuid, String content) {
            this.uuid = uuid;
            this.content = content;
        }

        public String getContent() {
            return content;
        }

        public String getUuid() {
            return uuid;
        }
    }
}
