package io.netty.gateway.route.nacos;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class NacosConfigLoaderTest {

    @Test
    void testLoadDefaultConfig() {
        NacosConfig config = NacosConfigLoader.load();

        assertNotNull(config, "配置不应为空");
        assertEquals("ubuntu.orb.local:8848", config.getServerAddr(), "服务器地址不匹配");
        assertEquals("public", config.getNamespace(), "命名空间不匹配");
        assertEquals("DEFAULT_GROUP", config.getGroup(), "分组不匹配");
    }

    @Test
    void testLoadCustomConfig() {
        NacosConfig config = NacosConfigLoader.load("nacos-test.properties");

        assertNotNull(config, "配置不应为空");
        // 根据测试配置文件的内容验证
        assertEquals("ubuntu.orb.local:8848", config.getServerAddr());
        assertEquals("public", config.getNamespace());
        assertEquals("TEST_GROUP", config.getGroup());
    }
} 