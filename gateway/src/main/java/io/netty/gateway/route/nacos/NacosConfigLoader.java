package io.netty.gateway.route.nacos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Nacos配置加载工具类
 */
public class NacosConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(NacosConfigLoader.class);

    // 配置文件相关
    private static final String DEFAULT_CONFIG_FILE = "nacos.properties";
    private static final String PREFIX = "nacos.";

    // 配置项Key
    private static final String KEY_SERVER_ADDR = "server-addr";
    private static final String KEY_NAMESPACE = "namespace";
    private static final String KEY_GROUP = "group";
    private static final String KEY_USERNAME = "username";
    private static final String KEY_PASSWORD = "password";

    // 默认值
    private static final String DEFAULT_NAMESPACE = "public";
    private static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    /**
     * 从默认配置文件加载配置
     */
    public static NacosConfig load() {
        return load(DEFAULT_CONFIG_FILE);
    }

    /**
     * 从指定配置文件加载配置
     *
     * @param configFile 配置文件名（相对于 classpath）
     */
    public static NacosConfig load(String configFile) {
        Properties props = new Properties();
        try (InputStream in = NacosConfigLoader.class.getClassLoader().getResourceAsStream(configFile)) {
            if (in == null) {
                throw new IllegalArgumentException("配置文件不存在: " + configFile);
            }
            props.load(in);
            return buildConfig(props);
        } catch (IOException e) {
            logger.error("加载Nacos配置文件失败: {}", configFile, e);
            throw new RuntimeException("Failed to load nacos config", e);
        }
    }

    /**
     * 从Properties对象构建NacosConfig
     */
    private static NacosConfig buildConfig(Properties props) {
        String serverAddr = getRequired(props, KEY_SERVER_ADDR);
        String namespace = get(props, KEY_NAMESPACE, DEFAULT_NAMESPACE);

        NacosConfig config = new NacosConfig(serverAddr, namespace);

        // 设置可选配置
        config.setGroup(get(props, KEY_GROUP, DEFAULT_GROUP));
        config.setUsername(get(props, KEY_USERNAME, null));
        config.setPassword(get(props, KEY_PASSWORD, null));

        logger.info("已加载Nacos配置: serverAddr={}, namespace={}, group={}, ephemeral={}",
                serverAddr, namespace, config.getGroup(), config.isEphemeral());

        return config;
    }

    private static String getRequired(Properties props, String key) {
        String value = get(props, key, null);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("必需的配置项未设置: " + PREFIX + key);
        }
        return value;
    }

    private static String get(Properties props, String key, String defaultValue) {
        return props.getProperty(PREFIX + key, defaultValue);
    }
} 