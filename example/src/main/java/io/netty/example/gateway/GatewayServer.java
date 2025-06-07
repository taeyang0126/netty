package io.netty.example.gateway;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.example.gateway.codec.GatewayMessageCodec;
import io.netty.example.gateway.handler.GatewayServerHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.concurrent.TimeUnit;

public class GatewayServer {
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    
    public GatewayServer(int port) {
        this.port = port;
        this.bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        this.workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
    }
    
    public void start() throws Exception {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 128)
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true)
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline p = ch.pipeline();
                     // 添加空闲检测，只检测读空闲
                     p.addLast(new IdleStateHandler(60, 0, 0, TimeUnit.SECONDS));
                     // 添加消息编解码器
                     p.addLast(new GatewayMessageCodec());
                     // 添加日志处理器
                     p.addLast(new LoggingHandler(LogLevel.INFO));
                     // 添加网关处理器
                     p.addLast(new GatewayServerHandler());
                 }
             });
            
            // 绑定端口并启动服务器
            ChannelFuture f = b.bind(port).sync();
            System.out.println("Gateway server started on port " + port);
            
            // 等待服务器关闭
            f.channel().closeFuture().sync();
        } finally {
            // 优雅关闭
            shutdown();
        }
    }
    
    public void shutdown() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
    
    public static void main(String[] args) throws Exception {
        int port = 8888;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        
        new GatewayServer(port).start();
    }
} 