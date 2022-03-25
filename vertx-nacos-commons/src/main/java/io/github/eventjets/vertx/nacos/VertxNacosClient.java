package io.github.eventjets.vertx.nacos;

import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import io.vertx.core.json.JsonObject;

public class VertxNacosClient {

    public static NamingService createNamingService(JsonObject config) throws NacosException {
        String serverAddr = config.getJsonObject("nacos").getString("addr");
        return NamingFactory.createNamingService(serverAddr);
    }

    public static ConfigService createConfigService(JsonObject config) throws NacosException {
        String serverAddr = config.getJsonObject("nacos").getString("addr");
        return ConfigFactory.createConfigService(serverAddr);
    }
}
