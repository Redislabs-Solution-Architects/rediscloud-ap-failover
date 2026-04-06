package com.redis.failoverdemo;

import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {

    private RedisConnectionManager connMgr;
    private KeyWriter writer;

    @Bean
    public AppConfig appConfig() {
        return new AppConfig();
    }

    @Bean
    public RedisConnectionManager redisConnectionManager() {
        connMgr = new RedisConnectionManager();
        return connMgr;
    }

    @Bean
    public RedisCloudApiClient redisCloudApiClient() {
        return new RedisCloudApiClient();
    }

    @Bean
    public KeyWriter keyWriter(RedisConnectionManager connMgr) {
        writer = new KeyWriter(connMgr);
        return writer;
    }

    @PreDestroy
    public void cleanup() {
        if (writer != null) writer.stop();
        if (connMgr != null) connMgr.close();
    }
}
