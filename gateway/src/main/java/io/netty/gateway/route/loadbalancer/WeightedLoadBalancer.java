package io.netty.gateway.route.loadbalancer;

import io.netty.gateway.route.ServiceInstance;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * 加权负载均衡器实现
 */
public class WeightedLoadBalancer implements LoadBalancer {
    private final Random random = new Random();

    @Override
    public ServiceInstance select(List<ServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }

        // 过滤出健康的实例
        List<ServiceInstance> healthyInstances = instances.stream()
                .filter(ServiceInstance::isHealthy)
                .collect(Collectors.toList());

        if (healthyInstances.isEmpty()) {
            return null;
        }

        // 计算总权重
        int totalWeight = healthyInstances.stream()
                .mapToInt(ServiceInstance::getWeight)
                .sum();

        // 在0到总权重之间随机选择一个值
        int randomWeight = random.nextInt(totalWeight);
        
        // 根据权重选择实例
        int currentWeight = 0;
        for (ServiceInstance instance : healthyInstances) {
            currentWeight += instance.getWeight();
            if (randomWeight < currentWeight) {
                return instance;
            }
        }

        // 保底返回最后一个实例
        return healthyInstances.get(healthyInstances.size() - 1);
    }
} 