package io.netty.gateway.test;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

// 这个注解告诉 JUnit 5 该测试类需要使用 Docker 容器，并由 Testcontainers 管理容器的生命周期。
@Testcontainers
public class CommonMicroServiceTest {

    // 创建一个私有的 Docker 网络，使得不同的容器可以在这个网络内相互通信。
    private static final Network network = Network.newNetwork();
    private static final String HTTPBIN = "httpbin";
    public static final int HTTPBIN_PORT = 80;
    /*
        设置一个 httpbin 容器（它提供各种 HTTP 测试端点）
        暴露 80 端口
        将容器连接到之前创建的网络
        给容器一个网络别名 "httpbin"，使其在网络内可通过该名称访问
     */
    public static final GenericContainer<?> HTTPBIN_CONTAINER
            = new GenericContainer<>("kennethreitz/httpbin:latest")
            .withExposedPorts(HTTPBIN_PORT)
            .withNetwork(network)
            .withNetworkAliases(HTTPBIN);
    /**
     * <a href="https://java.testcontainers.org/modules/toxiproxy/">toxiproxy</a>
     * 使用 toxiproxy 封装 httpbin
     * 可以使用 toxiproxy 模拟网络故障等情况
     * 可以用的 port 范围是 8666～8697
     * 也连接到同一个网络
     * 一个 TOXIPROXY_CONTAINER 对应多个不同 proxy，通过 TOXIPROXY_CONTAINER.getMappedPort(内部端口) 获取映射到主机的端口
     */
    private static final ToxiproxyContainer TOXIPROXY_CONTAINER = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);

    // 可用的 httpbin 端口
    private static final int GOOD_HTTPBIN_PROXY_PORT = 8666;
    // READ_TIMEOUT httpbin 端口
    private static final int READ_TIMEOUT_HTTPBIN_PROXY_PORT = 8667;
    //
    private static final int RESET_PEER_HTTPBIN_PROXY_PORT = 8668;

    public static final String GOOD_HOST;
    public static final int GOOD_PORT;
    /**
     * 以下代表请求已经发出到服务端，但是响应超时，或者不能响应（比如服务器重启）
     */
    public static final String READ_TIMEOUT_HOST;
    public static final int READ_TIMEOUT_PORT;
    public static final String RESET_PEER_HOST;
    public static final int RESET_PEER_PORT;

    public static final String DIRECT_HOST;
    public static final int DIRECT_PORT;

    /**
     * 以下代表请求都没有发出去，TCP 链接都没有建立
     */
    public static final String CONNECT_TIMEOUT_HOST = "localhost";
    /**
     * 端口 1 一定连不上的
     */
    public static final int CONNECT_TIMEOUT_PORT = 1;

    static {
        // 不使用 @Container 注解管理容器声明周期，因为我们需要在静态块生成代理，必须在这之前启动容器
        // 不用担心容器不会被关闭，因为 testcontainers 会启动一个 ryuk 容器，用于监控并关闭所有容器
        HTTPBIN_CONTAINER.start();
        TOXIPROXY_CONTAINER.start();
        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(TOXIPROXY_CONTAINER.getHost(), TOXIPROXY_CONTAINER.getControlPort());
        try {

            // 1. 创建正常代理
            @SuppressWarnings("all")
            Proxy proxy = toxiproxyClient.createProxy("good", "0.0.0.0:" + GOOD_HTTPBIN_PROXY_PORT, HTTPBIN + ":" + HTTPBIN_PORT);

            // 2. 创建读取超时代理
            // 关闭流量，会 READ TIME OUT
            proxy = toxiproxyClient.createProxy("read_timeout", "0.0.0.0:" + READ_TIMEOUT_HTTPBIN_PROXY_PORT, HTTPBIN + ":" + HTTPBIN_PORT);
            // 将上下行带宽设为0，模拟读取超时
            // bandwidth 限制网络连接的带宽
            // UPSTREAM=0 客户端无法向服务端发送请求，导致写超时
            // DOWNSTREAM=0 服务端无法向客户端响应，导致读超时
            proxy.toxics().bandwidth("UP_DISABLE", ToxicDirection.UPSTREAM, 0);
            proxy.toxics().bandwidth("DOWN_DISABLE", ToxicDirection.DOWNSTREAM, 0);

            // 3. 创建连接重置代理
            // todo reset peer 不生效，抓包发现没有发送 rst 包，具体原因需要再看
            proxy = toxiproxyClient.createProxy("reset_peer", "0.0.0.0:" + RESET_PEER_HTTPBIN_PROXY_PORT, HTTPBIN + ":" + HTTPBIN_PORT);
            // 在连接建立后立即重置连接
            // 上游重置 (ToxicDirection.UPSTREAM): 当客户端尝试向服务器发送数据时，连接会被重置，客户端会收到 "Connection reset by peer" 错误
            // 下游重置 (ToxicDirection.DOWNSTREAM): 当服务器尝试向客户端发送数据时，连接会被重置，服务器会收到 "Connection reset by peer" 错误
            // 延迟为1ms
            proxy.toxics().resetPeer("UP_SLOW_CLOSE", ToxicDirection.UPSTREAM, 1);
            proxy.toxics().resetPeer("DOWN_SLOW_CLOSE", ToxicDirection.DOWNSTREAM, 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DIRECT_HOST = HTTPBIN_CONTAINER.getHost();
        DIRECT_PORT = HTTPBIN_CONTAINER.getMappedPort(HTTPBIN_PORT);
        GOOD_HOST = TOXIPROXY_CONTAINER.getHost();
        GOOD_PORT = TOXIPROXY_CONTAINER.getMappedPort(GOOD_HTTPBIN_PROXY_PORT);
        READ_TIMEOUT_HOST = TOXIPROXY_CONTAINER.getHost();
        READ_TIMEOUT_PORT = TOXIPROXY_CONTAINER.getMappedPort(READ_TIMEOUT_HTTPBIN_PROXY_PORT);
        RESET_PEER_HOST = TOXIPROXY_CONTAINER.getHost();
        RESET_PEER_PORT = TOXIPROXY_CONTAINER.getMappedPort(RESET_PEER_HTTPBIN_PROXY_PORT);
    }
}


