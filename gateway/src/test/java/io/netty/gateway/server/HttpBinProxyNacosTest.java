package io.netty.gateway.server;

import com.alibaba.nacos.api.exception.NacosException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.gateway.GatewayServer;
import io.netty.gateway.auth.DefaultAuthService;
import io.netty.gateway.codec.GatewayMessageCodec;
import io.netty.gateway.protocol.GatewayMessage;
import io.netty.gateway.route.ServiceRegistry;
import io.netty.gateway.route.connection.ConnectionManager;
import io.netty.gateway.route.connection.DefaultConnectionManager;
import io.netty.gateway.route.nacos.NacosConfig;
import io.netty.gateway.route.nacos.NacosConfigLoader;
import io.netty.gateway.route.nacos.NacosServiceRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.netty.gateway.test.CommonMicroServiceTest.DIRECT_HOST;
import static io.netty.gateway.test.CommonMicroServiceTest.DIRECT_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * <p>
 * 代理 httpbin
 * </p>
 *
 * @author 伍磊
 */
public class HttpBinProxyNacosTest {
    private static final Logger logger = LoggerFactory.getLogger(HttpBinProxyNacosTest.class);

    private static final String SERVER_HOST = "127.0.0.1";
    private static final int PORT = 8888;
    private static GatewayServer gatewayServer;
    private static final EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(4, NioIoHandler.newFactory());
    private static final Map<Long, CompletableFuture<GatewayMessage>> pendingRequests = new ConcurrentHashMap<>();
    private static Channel clientChannel;

    @BeforeAll
    public static void beforeAll() throws InterruptedException, NacosException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ConnectionManager connectionManager = new DefaultConnectionManager();
        NacosConfig config = NacosConfigLoader.load("nacos-test.properties");
        ServiceRegistry registry = new NacosServiceRegistry(config);
        gatewayServer = new GatewayServer(PORT, registry, connectionManager);

        Thread thread = new Thread(() -> {
            try {
                gatewayServer.start(future);
            } catch (Exception e) {
                future.completeExceptionally(e);
                throw new RuntimeException(e);
            }
        });
        thread.setName("gateway-server");
        thread.setDaemon(true);
        thread.start();
        future.join();

        // 客户端
        clientChannel = initClient();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        gatewayServer.shutdown();
        eventLoopGroup.shutdownGracefully();
        pendingRequests.clear();
    }

    @Test
    @Timeout(8)
    public void test() throws Exception {
        doAuth();

        // 1. anything
        gatewayServer.registerService("anything", DIRECT_HOST, DIRECT_PORT);
        TimeUnit.SECONDS.sleep(1);

        GatewayMessage anythingRequest = new GatewayMessage();
        anythingRequest.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        anythingRequest.setRequestId(System.currentTimeMillis());
        anythingRequest.setClientId(UUID.randomUUID().toString());
        anythingRequest.setBizType("anything");
        anythingRequest.setBody("anything".getBytes());
        CompletableFuture<GatewayMessage> anythingResponseFuture = writeMsg(anythingRequest);
        GatewayMessage gatewayMessage = anythingResponseFuture.get();
        assertNotNull(gatewayMessage);
        assertEquals(GatewayMessage.MESSAGE_TYPE_BIZ, gatewayMessage.getMsgType());
        assertEquals(anythingRequest.getRequestId(), gatewayMessage.getRequestId());
        assertEquals(anythingRequest.getClientId(), gatewayMessage.getClientId());
        assertNotNull(gatewayMessage.getBody());
        logger.info("gateway server response anything: {}", gatewayMessage);

        // 2. delay 1s
        gatewayServer.registerService("delay.1", DIRECT_HOST, DIRECT_PORT);
        TimeUnit.SECONDS.sleep(1);

        anythingRequest = new GatewayMessage();
        anythingRequest.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        anythingRequest.setRequestId(System.currentTimeMillis());
        anythingRequest.setClientId(UUID.randomUUID().toString());
        anythingRequest.setBizType("delay.1");
        CompletableFuture<GatewayMessage> delayResponseFuture = writeMsg(anythingRequest);
        gatewayMessage = delayResponseFuture.get();
        assertEquals(GatewayMessage.MESSAGE_TYPE_BIZ, gatewayMessage.getMsgType());
        assertEquals(anythingRequest.getRequestId(), gatewayMessage.getRequestId());
        assertEquals(anythingRequest.getClientId(), gatewayMessage.getClientId());
        assertNotNull(gatewayMessage.getBody());
        logger.info("gateway server response delay: {}", gatewayMessage);

        // 3. 并发测试
        // 并发 1000 个请求，全部请求 anything，看下总体的耗时情况
        int count = 1000;
        CountDownLatch latch = new CountDownLatch(count);
        List<CompletableFuture<GatewayMessage>> futures = new ArrayList<>();
        long start = System.currentTimeMillis();
        logger.info("start....");
        for (int i = 0; i < count; i++) {
            GatewayMessage request = new GatewayMessage();
            request.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
            request.setRequestId(start + i);
            request.setClientId(UUID.randomUUID().toString());
            request.setBizType("anything");
            request.setBody("anything".getBytes());
            CompletableFuture<GatewayMessage> future = writeMsg(request);
            futures.add(future);
            latch.countDown();
        }

        latch.await();
        long end = System.currentTimeMillis();
        logger.info("send end...{}", end - start);

        // 等待结果
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
        logger.info("response end...{}", System.currentTimeMillis() - start);
    }

    private static void doAuth() throws InterruptedException, ExecutionException {
        GatewayMessage gatewayMessage = new GatewayMessage();
        gatewayMessage.setMsgType(GatewayMessage.MESSAGE_TYPE_AUTH);
        gatewayMessage.setRequestId(System.currentTimeMillis());
        gatewayMessage.setClientId(UUID.randomUUID().toString());
        gatewayMessage.getExtensions().put(DefaultAuthService.TOKEN_NAME, DefaultAuthService.TOKEN_VALUE);
        CompletableFuture<GatewayMessage> responseFuture = writeMsg(gatewayMessage);

        // 等待消息回来
        GatewayMessage response = responseFuture.get();

        assertNotNull(response);
        assertEquals(gatewayMessage.getClientId(), response.getClientId());
        assertEquals(gatewayMessage.getRequestId(), response.getRequestId());
        assertEquals(GatewayMessage.MESSAGE_TYPE_AUTH_SUCCESS_RESP, response.getMsgType());
    }

    private static Channel initClient() throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new GatewayMessageCodec());
                        pipeline.addLast(new SimpleChannelInboundHandler<GatewayMessage>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, GatewayMessage msg) throws Exception {
                                CompletableFuture<GatewayMessage> request = pendingRequests.remove(msg.getRequestId());
                                request.complete(msg);
                            }
                        });
                    }
                });
        Channel channel = bootstrap.connect(SERVER_HOST, PORT).sync().channel();
        channel.closeFuture().addListener(future -> {
            // 连接关闭，清除资源
            pendingRequests.forEach((requestId, f) -> {
                f.completeExceptionally(new ClosedChannelException());
            });
            pendingRequests.clear();
        });
        return channel;
    }

    private static CompletableFuture<GatewayMessage> writeMsg(GatewayMessage request) {
        CompletableFuture<GatewayMessage> completableFuture = new CompletableFuture<>();
        clientChannel.writeAndFlush(request)
                .addListener(future -> {
                    if (future.isSuccess()) {
                        pendingRequests.put(request.getRequestId(), completableFuture);
                    }
                });
        return completableFuture;
    }


}
