package io.netty.gateway.route;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 服务实例
 * 表示一个后端服务节点
 */
public class ServiceInstance {
    private final String host;
    private final int port;
    private final double weight;
    private volatile boolean healthy;
    private final Map<String, String> metadata;
    /**
     * If instance is enabled to accept request.
     */
    private final boolean enabled;

    public ServiceInstance(String host, int port) {
        this(host, port, 1);
    }

    public ServiceInstance(String host, int port, int weight) {
        this(host, port, weight, new HashMap<>(), true, true);
    }

    public ServiceInstance(String host, int port, Map<String, String> metadata) {
        this(host, port, 1, metadata, true, true);
    }

    public ServiceInstance(String host, int port, double weight, Map<String, String> metadata, boolean healthy, boolean enabled) {
        this.host = host;
        this.port = port;
        this.weight = weight;
        this.metadata = new HashMap<>(metadata);
        this.healthy = healthy;
        this.enabled = enabled;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public double getWeight() {
        return weight;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }


    public Map<String, String> getMetadata() {
        return new HashMap<>(metadata);
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstance that = (ServiceInstance) o;
        return port == that.port &&
                Objects.equals(host, that.host) &&
                Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, metadata);
    }

    @Override
    public String toString() {
        return "ServiceInstance{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", weight=" + weight +
                ", healthy=" + healthy +
                ", enable=" + enabled +
                ", metadata=" + metadata +
                '}';
    }
} 