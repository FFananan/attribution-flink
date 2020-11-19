package com.tencent.attribution.flink.config;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.Arrays;

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

    @Test
    public void testSplit() {
        String str = "###1111###2222######3333";
        System.out.println(Arrays.toString(str.split("###")));
    }
}
