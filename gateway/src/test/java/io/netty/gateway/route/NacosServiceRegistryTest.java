package io.netty.gateway.route;

import com.alibaba.nacos.api.exception.NacosException;
import io.netty.gateway.route.nacos.NacosConfig;
import io.netty.gateway.route.nacos.NacosConfigLoader;
import io.netty.gateway.route.nacos.NacosServiceRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nacos服务注册测试
 * 运行前请确保Nacos服务已启动
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NacosServiceRegistryTest {
    private static final Logger logger = LoggerFactory.getLogger(NacosServiceRegistryTest.class);

    private static final String TEST_SERVICE_NAME = "test-service";
    private static NacosConfig nacosConfig;
    private static ServiceRegistry serviceRegistry;
    private static ServiceInstance testInstance;

    @BeforeAll
    public static void init() throws NacosException {

        nacosConfig = NacosConfigLoader.load("nacos-test.properties");
        serviceRegistry = new NacosServiceRegistry(nacosConfig);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("version", "1.0.0");
        metadata.put("environment", "test");
        testInstance = new ServiceInstance("127.0.0.1", 8080, metadata);
    }


    @AfterAll
    public static void clear() {
        try {
            logger.info("清理已存在的测试服务实例...");
            serviceRegistry.removeService(TEST_SERVICE_NAME, testInstance);
            TimeUnit.SECONDS.sleep(1); // 等待清理生效

            List<ServiceInstance> existingServices = serviceRegistry.getServices(TEST_SERVICE_NAME);
            if (!existingServices.isEmpty()) {
                logger.warn("仍有{}个服务实例存在，将强制清理", existingServices.size());
                for (ServiceInstance instance : existingServices) {
                    serviceRegistry.removeService(TEST_SERVICE_NAME, instance);
                }
                TimeUnit.SECONDS.sleep(1); // 再次等待清理生效
            }
        } catch (Exception e) {
            logger.warn("清理测试服务时发生异常: {}", e.getMessage());
        }
    }

    @Test
    @Order(1)
    public void testRegisterService() throws Exception {
        logger.info("测试服务注册...");

        // 注册服务
        serviceRegistry.registerService(TEST_SERVICE_NAME, testInstance);
        logger.info("服务注册请求已发送");

        // 等待服务注册生效
        TimeUnit.SECONDS.sleep(1);

        // 验证服务是否注册成功
        List<ServiceInstance> services = serviceRegistry.getServices(TEST_SERVICE_NAME);
        assertNotNull(services, "获取服务列表失败");
        assertFalse(services.isEmpty(), "服务列表为空");

        ServiceInstance registeredInstance = services.get(0);
        assertEquals(testInstance.getHost(), registeredInstance.getHost(), "服务实例Host不匹配");
        assertEquals(testInstance.getPort(), registeredInstance.getPort(), "服务实例Port不匹配");

        logger.info("服务注册成功，当前实例: {}", registeredInstance);
    }

    @Test
    @Order(2)
    public void testGetServices() throws Exception {
        logger.info("测试服务发现...");

        // 先注册服务
        serviceRegistry.registerService(TEST_SERVICE_NAME, testInstance);
        TimeUnit.SECONDS.sleep(1);

        // 然后获取服务列表
        List<ServiceInstance> services = serviceRegistry.getServices(TEST_SERVICE_NAME);
        assertNotNull(services, "获取服务列表失败");
        assertFalse(services.isEmpty(), "服务列表为空");

        ServiceInstance instance = services.get(0);
        assertEquals(testInstance.getHost(), instance.getHost(), "服务实例Host不匹配");
        assertEquals(testInstance.getPort(), instance.getPort(), "服务实例Port不匹配");

        Map<String, String> metadata = instance.getMetadata();
        assertNotNull(metadata.get("version"), "版本信息不存在");
        assertEquals("1.0.0", metadata.get("version"), "版本信息不匹配");

        logger.info("发现的服务实例: {}", instance);
    }

    @Test
    @Order(3)
    public void testRemoveService() throws Exception {
        logger.info("测试服务注销...");

        // 先注册服务
        serviceRegistry.registerService(TEST_SERVICE_NAME, testInstance);
        TimeUnit.SECONDS.sleep(1);

        // 确认服务已注册
        List<ServiceInstance> servicesBeforeRemoval = serviceRegistry.getServices(TEST_SERVICE_NAME);
        assertFalse(servicesBeforeRemoval.isEmpty(), "服务未成功注册");

        // 注销服务
        serviceRegistry.removeService(TEST_SERVICE_NAME, testInstance);
        TimeUnit.SECONDS.sleep(1);

        // 验证服务是否已注销
        List<ServiceInstance> servicesAfterRemoval = serviceRegistry.getServices(TEST_SERVICE_NAME);
        assertTrue(servicesAfterRemoval.isEmpty(), "服务未能成功注销");

        logger.info("服务注销成功");
    }
}
