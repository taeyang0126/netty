package io.netty.gateway;

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
import io.netty.gateway.auth.DefaultAuthService;
import io.netty.gateway.codec.GatewayMessageCodec;
import io.netty.gateway.handler.AuthHandler;
import io.netty.gateway.handler.GatewayServerHandler;
import io.netty.gateway.route.DefaultRouteService;
import io.netty.gateway.route.DefaultServiceRegistry;
import io.netty.gateway.route.RouteService;
import io.netty.gateway.route.ServiceInstance;
import io.netty.gateway.route.ServiceRegistry;
import io.netty.gateway.route.connection.ConnectionManager;
import io.netty.gateway.route.connection.DefaultConnectionManager;
import io.netty.gateway.route.loadbalancer.LoadBalancer;
import io.netty.gateway.route.loadbalancer.RoundRobinLoadBalancer;
import io.netty.gateway.session.DefaultSessionManager;
import io.netty.gateway.session.SessionManager;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class GatewayServer {
    private static final Logger logger = LoggerFactory.getLogger(GatewayServer.class);

    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final SessionManager sessionManager;
    private final RouteService routeService;
    private final AuthHandler authHandler;

    public GatewayServer(int port) {
        this.port = port;
        this.bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        this.workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        this.sessionManager = new DefaultSessionManager();
        ConnectionManager connectionManager = new DefaultConnectionManager();
        ServiceRegistry registry = new DefaultServiceRegistry(connectionManager);
        LoadBalancer loadBalancer = new RoundRobinLoadBalancer();
        this.routeService = new DefaultRouteService(registry, loadBalancer, connectionManager);
        this.authHandler = new AuthHandler(new DefaultAuthService(), sessionManager);
    }

    public GatewayServer(int port, ServiceRegistry registry, ConnectionManager connectionManager) {
        this(port, new DefaultRouteService(registry, new RoundRobinLoadBalancer(), connectionManager));
    }

    public GatewayServer(int port, RouteService routeService) {
        this.port = port;
        this.bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        this.workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        this.sessionManager = new DefaultSessionManager();
        this.routeService = routeService;
        this.authHandler = new AuthHandler(new DefaultAuthService(), sessionManager);
    }

    public void start() throws Exception {
        start(new CompletableFuture<>());
    }

    public void start(CompletableFuture<Void> completableFuture) throws Exception {
        logger.info("Starting Gateway Server on port: {}", port);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            logger.debug("New connection from: {}", ch.remoteAddress());
                            ChannelPipeline p = ch.pipeline();
                            // 添加空闲检测，60秒没有读取到数据则判定为空闲
                            p.addLast(new IdleStateHandler(60, 0, 0, TimeUnit.SECONDS));
                            // 添加消息编解码器
                            p.addLast(new GatewayMessageCodec());
                            // auth handler
                            p.addLast(authHandler);
                            // 添加网关处理器
                            p.addLast(new GatewayServerHandler(sessionManager, routeService));
                        }
                    });

            // 绑定端口并启动服务器
            ChannelFuture f = b.bind(port).sync();
            logger.info("Gateway Server started successfully on port: {}", port);
            completableFuture.complete(null);

            // 等待服务器关闭
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            logger.error("Failed to start Gateway Server", e);
            completableFuture.completeExceptionally(e);
            throw e;
        } finally {
            // 优雅关闭
            shutdown();
        }
    }

    public void shutdown() {
        logger.info("Shutting down Gateway Server...");
        // 关闭会话管理器
        if (sessionManager instanceof DefaultSessionManager) {
            ((DefaultSessionManager) sessionManager).shutdown();
        }
        // 关闭线程组
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        logger.info("Gateway Server shutdown completed");
    }

    public void registerService(String bizType, String host, int port) {
        ServiceRegistry serviceRegistry = this.routeService.getServiceRegistry();
        serviceRegistry.registerService(bizType, new ServiceInstance(host, port));
    }

    public static void main(String[] args) throws Exception {
        int port = 8888;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }

        new GatewayServer(port).start();
    }
} 