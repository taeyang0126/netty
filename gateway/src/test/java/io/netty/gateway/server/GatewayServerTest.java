package io.netty.gateway.server;

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
import io.netty.gateway.route.RouteService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * <p>
 * GatewayServerTest
 * </p>
 *
 * @author 伍磊
 */
@ExtendWith(MockitoExtension.class)
public class GatewayServerTest {

    private static final Logger logger = LoggerFactory.getLogger(HttpBinProxyTest.class);

    @Mock
    private RouteService routeService;

    public static final String SERVER_HOST = "127.0.0.1";
    private static final int SERVER_PORT = 18080;
    private static final EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private GatewayServer gatewayServer;
    private Map<Long, CompletableFuture<GatewayMessage>> pendingRequests;
    private Channel clientChannel;
    private final AtomicInteger counter = new AtomicInteger();

    @BeforeEach
    public void setup() throws Exception {
        TimeUnit.SECONDS.sleep(1);
        this.pendingRequests = new ConcurrentHashMap<>();

        // 1. GatewayServer
        CompletableFuture<Void> future = new CompletableFuture<>();
        gatewayServer = new GatewayServer(SERVER_PORT, routeService);
        Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("gateway-server-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }).execute(() -> {
            try {
                gatewayServer.start(future);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        future.get();

        // 2. client Channel
        clientChannel = initClient();
    }

    @AfterEach
    public void teardown() {
        gatewayServer.shutdown();
        pendingRequests.clear();
    }

    @AfterAll
    public static void teardownAll() {
        eventLoopGroup.shutdownGracefully();
    }

    @Test
    public void testNoAuth() throws Exception {

        // 发送心跳消息
        GatewayMessage gatewayMessage = new GatewayMessage();
        gatewayMessage.setMsgType(GatewayMessage.MESSAGE_TYPE_HEARTBEAT);
        gatewayMessage.setRequestId(System.currentTimeMillis());
        gatewayMessage.setClientId(UUID.randomUUID().toString());
        CompletableFuture<GatewayMessage> responseFuture = writeMsg(gatewayMessage);

        // 等待消息回来
        GatewayMessage response = responseFuture.get();

        assertNotNull(response);
        assertEquals(gatewayMessage.getClientId(), response.getClientId());
        assertEquals(gatewayMessage.getRequestId(), response.getRequestId());
        assertEquals(GatewayMessage.MESSAGE_TYPE_AUTH_FAIL_RESP, response.getMsgType());
    }

    @Test
    public void testAuth() throws Exception {
        // 发送认证消息
        doAuth();
    }


    @Test
    public void testHeartbeat() throws Exception {

        // 1. 先发送认证消息
        doAuth();

        // 2. 发送心跳消息
        GatewayMessage gatewayMessage = new GatewayMessage();
        gatewayMessage.setMsgType(GatewayMessage.MESSAGE_TYPE_HEARTBEAT);
        gatewayMessage.setRequestId(System.currentTimeMillis());
        gatewayMessage.setClientId(UUID.randomUUID().toString());
        CompletableFuture<GatewayMessage> heartBeatFuture = writeMsg(gatewayMessage);
        GatewayMessage response = heartBeatFuture.get();
        assertNotNull(response);
        assertEquals(gatewayMessage.getClientId(), response.getClientId());
        assertEquals(gatewayMessage.getRequestId(), response.getRequestId());
        assertEquals(GatewayMessage.MESSAGE_TYPE_HEARTBEAT, response.getMsgType());

    }

    @Test
    public void testBizMsg_errorBizType() throws Exception {
        CompletableFuture<GatewayMessage> mockFuture = new CompletableFuture<>();
        mockFuture.completeExceptionally(new IllegalArgumentException());
        when(routeService.route(any()))
                .thenReturn(mockFuture);

        // 1. 先发送认证消息
        doAuth();

        // 2. 发送业务消息
        GatewayMessage gatewayMessage = new GatewayMessage();
        gatewayMessage.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        gatewayMessage.setRequestId(System.currentTimeMillis());
        gatewayMessage.setClientId(UUID.randomUUID().toString());

        CompletableFuture<GatewayMessage> bizMsgFuture = writeMsg(gatewayMessage);
        GatewayMessage responseMsg = bizMsgFuture.get();

        assertNotNull(responseMsg);
        assertEquals(gatewayMessage.getClientId(), responseMsg.getClientId());
        assertEquals(gatewayMessage.getRequestId(), responseMsg.getRequestId());
        assertEquals(GatewayMessage.MESSAGE_TYPE_ERROR, responseMsg.getMsgType());
    }

    @Test
    public void testBizMsg() throws Exception {

        String clientId = UUID.randomUUID().toString();
        long requestId = System.currentTimeMillis();
        String responseContent = UUID.randomUUID().toString();

        CompletableFuture<GatewayMessage> mockFuture = new CompletableFuture<>();
        GatewayMessage mockResponseMsg = new GatewayMessage();
        mockResponseMsg.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        mockResponseMsg.setRequestId(requestId);
        mockResponseMsg.setClientId(clientId);
        mockResponseMsg.setBody(responseContent.getBytes(StandardCharsets.UTF_8));
        mockFuture.complete(mockResponseMsg);
        when(routeService.route(any()))
                .thenReturn(mockFuture);

        // 1. 先发送认证消息
        testAuth();

        // 2. 发送业务消息
        GatewayMessage gatewayMessage = new GatewayMessage();
        gatewayMessage.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        gatewayMessage.setRequestId(requestId);
        gatewayMessage.setClientId(clientId);

        CompletableFuture<GatewayMessage> bizMsgFuture = writeMsg(gatewayMessage);
        GatewayMessage responseMsg = bizMsgFuture.get();

        assertNotNull(responseMsg);
        assertEquals(gatewayMessage.getClientId(), responseMsg.getClientId());
        assertEquals(gatewayMessage.getRequestId(), responseMsg.getRequestId());
        assertEquals(GatewayMessage.MESSAGE_TYPE_BIZ, responseMsg.getMsgType());
        assertArrayEquals(responseContent.getBytes(StandardCharsets.UTF_8), responseMsg.getBody());
    }

    @Test
    public void testMessageOrder() throws Exception {
        doAuth();  // 先认证

        // 发送多条顺序消息
        int messageCount = 5;
        List<GatewayMessage> messages = new ArrayList<>();
        List<CompletableFuture<GatewayMessage>> futures = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            GatewayMessage msg = new GatewayMessage();
            msg.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
            msg.setRequestId(System.currentTimeMillis() + i);  // 确保唯一
            msg.setClientId(UUID.randomUUID().toString());
            msg.setBody(String.valueOf(i).getBytes(StandardCharsets.UTF_8));

            messages.add(msg);
            futures.add(writeMsg(msg));
        }

        // 验证响应顺序
        for (int i = 0; i < messageCount; i++) {
            GatewayMessage response = futures.get(i).get(5, TimeUnit.SECONDS);
            assertEquals(messages.get(i).getRequestId(), response.getRequestId());
        }
    }

    @Test
    public void testConcurrencyMessage() throws Exception {
        when(routeService.route(any(GatewayMessage.class))).thenAnswer((Answer<CompletableFuture<GatewayMessage>>) invocation -> {
            GatewayMessage request = invocation.getArgument(0);

            GatewayMessage mockResponseMsg = new GatewayMessage();
            mockResponseMsg.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
            mockResponseMsg.setRequestId(request.getRequestId());
            mockResponseMsg.setClientId(request.getClientId());
            mockResponseMsg.setBody(request.getBody());

            // c. 返回修改后的对象
            CompletableFuture<GatewayMessage> concurrentFuture = new CompletableFuture<>();
            concurrentFuture.complete(mockResponseMsg);
            return concurrentFuture;
        });

        doAuth();  // 先认证

        // 发送多条顺序消息
        int messageCount = 1000;
        CountDownLatch latch = new CountDownLatch(messageCount);
        List<CompletableFuture<GatewayMessage>> futures = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            GatewayMessage msg = new GatewayMessage();
            msg.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
            msg.setRequestId(System.currentTimeMillis() + i);  // 确保唯一
            msg.setClientId(UUID.randomUUID().toString());
            msg.setBody(String.valueOf(i).getBytes(StandardCharsets.UTF_8));

            futures.add(writeMsg(msg));
            latch.countDown();
        }

        latch.await();
        logger.info("send finished");

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
        logger.info("finished");

    }

    @Test
    public void testReconnect() throws Exception {
        doAuth();  // 先认证

        // 断开连接
        clientChannel.close().sync();

        // 重新连接
        clientChannel = initClient();

        // 验证需要重新认证
        GatewayMessage heartbeat = new GatewayMessage();
        heartbeat.setMsgType(GatewayMessage.MESSAGE_TYPE_HEARTBEAT);
        heartbeat.setRequestId(System.currentTimeMillis());

        CompletableFuture<GatewayMessage> future = writeMsg(heartbeat);
        GatewayMessage response = future.get(5, TimeUnit.SECONDS);

        assertEquals(GatewayMessage.MESSAGE_TYPE_AUTH_FAIL_RESP, response.getMsgType());
    }

    @Test
    public void testLargeMessage() throws Exception {
        // 构造大消息
        byte[] largeContent = new byte[1024 * 1024]; // 1MB
        new Random().nextBytes(largeContent);

        String clientId = UUID.randomUUID().toString();
        long requestId = System.currentTimeMillis();

        CompletableFuture<GatewayMessage> mockFuture = new CompletableFuture<>();
        GatewayMessage mockResponseMsg = new GatewayMessage();
        mockResponseMsg.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        mockResponseMsg.setRequestId(requestId);
        mockResponseMsg.setClientId(clientId);
        mockResponseMsg.setBody(largeContent);
        mockFuture.complete(mockResponseMsg);
        when(routeService.route(any()))
                .thenReturn(mockFuture);

        // 1. 先发送认证消息
        testAuth();

        // 2. 发送业务消息
        GatewayMessage gatewayMessage = new GatewayMessage();
        gatewayMessage.setMsgType(GatewayMessage.MESSAGE_TYPE_BIZ);
        gatewayMessage.setRequestId(requestId);
        gatewayMessage.setClientId(clientId);

        CompletableFuture<GatewayMessage> bizMsgFuture = writeMsg(gatewayMessage);
        GatewayMessage responseMsg = bizMsgFuture.get();

        assertNotNull(responseMsg);
        assertEquals(gatewayMessage.getClientId(), responseMsg.getClientId());
        assertEquals(gatewayMessage.getRequestId(), responseMsg.getRequestId());
        assertEquals(GatewayMessage.MESSAGE_TYPE_BIZ, responseMsg.getMsgType());
        assertArrayEquals(largeContent, responseMsg.getBody());
    }

    private Channel initClient() throws InterruptedException {
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
        Channel channel = bootstrap.connect(SERVER_HOST, SERVER_PORT).sync().channel();
        channel.closeFuture().addListener(future -> {
            // 连接关闭，清除资源
            pendingRequests.forEach((requestId, f) -> {
                f.completeExceptionally(new ClosedChannelException());
            });
            pendingRequests.clear();
        });
        return channel;
    }

    private CompletableFuture<GatewayMessage> writeMsg(GatewayMessage request) {
        CompletableFuture<GatewayMessage> completableFuture = new CompletableFuture<>();
        clientChannel.writeAndFlush(request)
                .addListener(future -> {
                    if (future.isSuccess()) {
                        pendingRequests.put(request.getRequestId(), completableFuture);
                    }
                });
        return completableFuture;
    }

    private void doAuth() throws InterruptedException, ExecutionException {
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


}
