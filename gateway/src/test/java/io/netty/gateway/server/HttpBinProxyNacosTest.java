package io.netty.gateway.server;

import com.alibaba.nacos.api.exception.NacosException;
import io.netty.gateway.GatewayServer;
import io.netty.gateway.route.ServiceRegistry;
import io.netty.gateway.route.connection.ConnectionManager;
import io.netty.gateway.route.connection.DefaultConnectionManager;
import io.netty.gateway.route.nacos.NacosConfig;
import io.netty.gateway.route.nacos.NacosConfigLoader;
import io.netty.gateway.route.nacos.NacosServiceRegistry;

/**
 * <p>
 * 代理 httpbin
 * </p>
 *
 * @author 伍磊
 */
public class HttpBinProxyNacosTest extends AbstractHttpBinProxyTest {
    private static final int PORT = 8888;

    @Override
    protected int getPort() {
        return PORT;
    }

    @Override
    protected GatewayServer getGatewayServer() {
        ConnectionManager connectionManager = new DefaultConnectionManager();
        NacosConfig config = NacosConfigLoader.load("nacos-test.properties");
        ServiceRegistry registry;
        try {
            registry = new NacosServiceRegistry(config);
        } catch (NacosException e) {
            throw new RuntimeException(e);
        }
        return new GatewayServer(PORT, registry, connectionManager);
    }


}
