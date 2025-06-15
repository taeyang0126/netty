package io.netty.gateway.route.loadbalancer;

import io.netty.gateway.route.ServiceInstance;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * 随机负载均衡器实现
 */
public class RandomLoadBalancer implements LoadBalancer {
    private final Random random = new Random();

    @Override
    public ServiceInstance select(List<ServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }

        // 过滤出健康的实例
        List<ServiceInstance> healthyInstances = instances.stream()
                .filter(ServiceInstance::isHealthy)
                .filter(ServiceInstance::isEnabled)
                .collect(Collectors.toList());

        if (healthyInstances.isEmpty()) {
            return null;
        }

        // 随机选择一个实例
        return healthyInstances.get(random.nextInt(healthyInstances.size()));
    }
}