package io.netty.gateway.server;

import io.netty.gateway.GatewayServer;

/**
 * <p>
 * 代理 httpbin
 * </p>
 *
 * @author 伍磊
 */
public class HttpBinProxyTest extends AbstractHttpBinProxyTest {
    private static final int PORT = 8888;

    @Override
    protected int getPort() {
        return PORT;
    }

    @Override
    protected GatewayServer getGatewayServer() {
        return new GatewayServer(PORT);
    }
}
