package io.netty.gateway.route.connection;

/**
 * 连接配置
 */
public class ConnectionConfig {

    private static final String ROUTE_SERVICE_CONNECT_TIMEOUT = "ROUTE_SERVICE_CONNECT_TIMEOUT";
    private static final int DEFAULT_CONNECT_TIMEOUT = 3000;

    private static final ConnectionConfig INSTANCE = new ConnectionConfig();

    private final int connectTimeoutMillis;

    private ConnectionConfig() {
        this.connectTimeoutMillis = Integer.parseInt(System.getProperty(ROUTE_SERVICE_CONNECT_TIMEOUT, String.valueOf(DEFAULT_CONNECT_TIMEOUT)));
    }

    public static ConnectionConfig getInstance() {
        return INSTANCE;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }
} 