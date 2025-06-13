package io.netty.gateway.route;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.gateway.route.connection.DefaultConnection;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <p>
 * 连接测试
 * </p>
 *
 * @author 伍磊
 */
public class DefaultConnectionTest {

    private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionTest.class);
    private static final EventLoopGroup BOSS_GROUP = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private static final EventLoopGroup WORKER_GROUP = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private static final int SERVER_PORT = 18000;

    @AfterAll
    public static void afterAll() {
        BOSS_GROUP.shutdownGracefully();
        WORKER_GROUP.shutdownGracefully();
    }

    @Test
    public void testClose() throws InterruptedException {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        CompletableFuture<Channel> serverChannelFuture = new CompletableFuture<>();
        // 用于保存服务端接受的客户端 Channel
        CompletableFuture<Channel> serverSideClientChannelFuture = new CompletableFuture<>();

        new Thread(() -> {
            serverBootstrap.group(BOSS_GROUP, WORKER_GROUP)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<NioSocketChannel>() {
                        @Override
                        protected void initChannel(NioSocketChannel ch) throws Exception {
                            // 保存服务端接受的客户端 Channel
                            serverSideClientChannelFuture.complete(ch);

                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new StringDecoder());
                            pipeline.addLast(new StringEncoder());
                            pipeline.addLast(new SimpleChannelInboundHandler<String>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
                                    logger.info("server received {}", msg);
                                    ctx.writeAndFlush(msg);
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                    logger.info("Server side client channel inactive");
                                    super.channelInactive(ctx);
                                }
                            });
                        }
                    });
            ChannelFuture channelFuture = serverBootstrap.bind(SERVER_PORT).syncUninterruptibly();
            logger.info("server bind finished: {}", SERVER_PORT);
            Channel serverChannel = channelFuture.channel();
            serverChannelFuture.complete(serverChannel);
            serverChannel.closeFuture().syncUninterruptibly();
        }).start();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(WORKER_GROUP)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new StringDecoder());
                        pipeline.addLast(new StringEncoder());
                        pipeline.addLast(new SimpleChannelInboundHandler<String>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
                                logger.info("client received {}", msg);
                                ctx.writeAndFlush(msg);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                logger.info("Client channel inactive");
                                super.channelInactive(ctx);
                            }
                        });
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect("127.0.0.1", SERVER_PORT).syncUninterruptibly();
        Channel clientChannel = channelFuture.channel();
        logger.info("client connected: {}", clientChannel);

        // 构建 DefaultConnection
        DefaultConnection defaultConnection = new DefaultConnection(bootstrap, clientChannel, new ServiceInstance("127.0.0.1", SERVER_PORT));

        // wait 1s
        try {
            TimeUnit.SECONDS.sleep(1);

            // 先关闭服务端接受的客户端 Channel
            Channel serverSideClientChannel = serverSideClientChannelFuture.get();
            logger.info("Closing server side client channel");
            serverSideClientChannel.close().syncUninterruptibly();
            // channel 都已关闭
            TimeUnit.MILLISECONDS.sleep(50);
            assertFalse(clientChannel.isActive());
            assertFalse(defaultConnection.isActive());

            // 等待 2s 重连
            TimeUnit.SECONDS.sleep(2);
            assertTrue(defaultConnection.isActive());
            assertFalse(clientChannel.isActive());

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testReconnectFailure() throws InterruptedException, ExecutionException {
        // 启动服务端
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        CompletableFuture<Channel> serverChannelFuture = new CompletableFuture<>();
        CompletableFuture<Channel> serverSideClientChannelFuture = new CompletableFuture<>();

        new Thread(() -> {
            serverBootstrap.group(BOSS_GROUP, WORKER_GROUP)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<NioSocketChannel>() {
                        @Override
                        protected void initChannel(NioSocketChannel ch) throws Exception {
                            serverSideClientChannelFuture.complete(ch);
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new StringDecoder());
                            pipeline.addLast(new StringEncoder());
                            pipeline.addLast(new SimpleChannelInboundHandler<String>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
                                    ctx.writeAndFlush(msg);
                                }
                            });
                        }
                    });
            ChannelFuture channelFuture = serverBootstrap.bind(SERVER_PORT).syncUninterruptibly();
            Channel serverChannel = channelFuture.channel();
            serverChannelFuture.complete(serverChannel);
            serverChannel.closeFuture().syncUninterruptibly();
        }).start();

        // 等待服务端启动
        TimeUnit.SECONDS.sleep(1);

        // 启动客户端
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(WORKER_GROUP)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new StringDecoder());
                        pipeline.addLast(new StringEncoder());
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect("127.0.0.1", SERVER_PORT).syncUninterruptibly();
        Channel clientChannel = channelFuture.channel();

        // 构建 DefaultConnection
        DefaultConnection defaultConnection = new DefaultConnection(bootstrap, clientChannel, new ServiceInstance("127.0.0.1", SERVER_PORT));

        // 关闭
        serverSideClientChannelFuture.get().close().syncUninterruptibly();

        // 关闭服务端
        Channel serverChannel = serverChannelFuture.get();
        serverChannel.close().syncUninterruptibly();

        // 等待重连超时
        TimeUnit.SECONDS.sleep(5);
        assertFalse(defaultConnection.isActive());
        // 单次连接数量
        assertEquals(DefaultConnection.MAX_RETRY_TIMES, defaultConnection.getRetryCount().get());
    }

}
