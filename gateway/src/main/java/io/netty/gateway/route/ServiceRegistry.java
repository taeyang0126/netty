package io.netty.gateway.route;

import java.util.List;
import java.util.Map;

/**
 * 服务注册中心接口
 */
public interface ServiceRegistry {
    /**
     * 注册服务实例
     *
     * @param bizType 业务类型
     * @param instance 服务实例
     */
    void registerService(String bizType, ServiceInstance instance);

    /**
     * 注销服务实例
     *
     * @param bizType 业务类型
     * @param instance 服务实例
     */
    void removeService(String bizType, ServiceInstance instance);

    /**
     * 获取指定业务类型的所有服务实例
     *
     * @param bizType 业务类型
     * @return 服务实例列表，如果没有返回空列表
     */
    List<ServiceInstance> getServices(String bizType);

    /**
     * 获取所有服务实例
     *
     * @return 业务类型到服务实例列表的映射
     */
    Map<String, List<ServiceInstance>> getAllServices();

    /**
     * 关闭注册中心，释放资源
     */
    void close();
} 