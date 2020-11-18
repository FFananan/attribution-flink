package com.tencent.attribution.flink.storage;

import com.tencent.attribution.flink.config.RedisConfigs;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class RedisStorage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Jedis client;

    /**
     * 传入配置类来初始化redis的客户端
     *
     * @param redisConfigs
     */
    public RedisStorage(RedisConfigs redisConfigs) {
//        Set<HostAndPort> set = new HashSet<>();
//        for (String node : redisConfigs.nodes) {
//            set.add(HostAndPort.parseString(node));
//        }
//        this.client = new JedisCluster(set, redisConfigs.timeout);

        this.client = new Jedis("127.0.0.1", 6379);
    }

    public void storage(String campaignId, String count) {
        this.client.set(campaignId, count);
    }

    public String fetch(String campaignId) {
        return this.client.get(campaignId);
    }

}
