package io.netty.gateway.route;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务实例
 * 表示一个后端服务节点
 */
public class ServiceInstance {
    private final String host;
    private final int port;
    private final int weight;
    private final AtomicInteger active;
    private volatile boolean healthy;

    public ServiceInstance(String host, int port) {
        this(host, port, 100);
    }

    public ServiceInstance(String host, int port, int weight) {
        this.host = host;
        this.port = port;
        this.weight = weight;
        this.active = new AtomicInteger(0);
        this.healthy = true;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getWeight() {
        return weight;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    public int incrementAndGetActive() {
        return active.incrementAndGet();
    }

    public int decrementAndGetActive() {
        return active.decrementAndGet();
    }

    public int getActive() {
        return active.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstance that = (ServiceInstance) o;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return String.format("%s:%d(weight=%d,active=%d,healthy=%s)", 
            host, port, weight, active.get(), healthy);
    }
} 