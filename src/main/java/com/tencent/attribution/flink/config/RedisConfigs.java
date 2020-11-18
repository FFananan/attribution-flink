package com.tencent.attribution.flink.config;

import java.io.Serializable;
import java.util.List;

public class RedisConfigs implements Serializable {
    private static final long serialVersionUID = 2134235L;
    public List<String> nodes;
    public int timeout;

    @Override
    public String toString() {
        return "RedisConfigs{" +
                "nodes=" + nodes +
                ", timeout=" + timeout +
                '}';
    }
}
