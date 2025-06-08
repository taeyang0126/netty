package io.netty.gateway.route.loadbalancer;

import io.netty.gateway.route.ServiceInstance;

import java.util.List;

/**
 * 负载均衡器接口
 */
public interface LoadBalancer {
    /**
     * 从服务实例列表中选择一个实例
     *
     * @param instances 可用的服务实例列表
     * @return 选中的服务实例，如果列表为空则返回null
     */
    ServiceInstance select(List<ServiceInstance> instances);
} 