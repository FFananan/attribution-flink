package com.tencent.attribution.flink.config;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;

import static jodd.util.ResourcesUtil.getResourceAsStream;

public class RedisConfigsTest {
    @Test
    public void test() {
        try {
            RedisConfigs redisConfigs = new Yaml().loadAs(getResourceAsStream("redis.yaml"), RedisConfigs.class);
            System.out.println(redisConfigs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
