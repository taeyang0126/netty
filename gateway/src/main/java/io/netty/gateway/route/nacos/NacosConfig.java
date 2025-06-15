package io.netty.gateway.route.nacos;

import com.alibaba.nacos.api.PropertyKeyConst;

import java.util.Properties;

public class NacosConfig {
    private String serverAddr;
    private String namespace;
    private String username;
    private String password;
    private String group = "DEFAULT_GROUP";
    private boolean ephemeral = true; // 默认为临时实例

    public NacosConfig(String serverAddr) {
        this.serverAddr = serverAddr;
    }

    public NacosConfig(String serverAddr, String namespace) {
        this.serverAddr = serverAddr;
        this.namespace = namespace;
    }

    public Properties buildProperties() {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverAddr);
        if (namespace != null) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, namespace);
        }
        if (username != null) {
            properties.setProperty(PropertyKeyConst.USERNAME, username);
        }
        if (password != null) {
            properties.setProperty(PropertyKeyConst.PASSWORD, password);
        }
        
        // Nacos 2.x 配置
        String grpcPort = "9848";  // 默认 gRPC 端口
        if (serverAddr.contains(":")) {
            String httpPort = serverAddr.substring(serverAddr.lastIndexOf(":") + 1);
            // 如果是默认 HTTP 端口 8848，则使用默认 gRPC 端口 9848
            if ("8848".equals(httpPort)) {
                System.setProperty("nacos.server.grpc.port", grpcPort);
            }
        }
        
        // 设置必要的配置
        properties.setProperty("namingLoadCacheAtStart", "true");
        
        return properties;
    }

    // Getters and Setters
    public String getServerAddr() {
        return serverAddr;
    }

    public void setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isEphemeral() {
        return ephemeral;
    }

    public void setEphemeral(boolean ephemeral) {
        this.ephemeral = ephemeral;
    }
} 